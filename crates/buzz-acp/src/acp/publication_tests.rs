//! The subprocess receives only harness-issued scope; no gateway is contacted.
use super::*;
use crate::acp::{EnvVar, McpServer, SystemPromptTransport};
use std::time::Duration;

async fn client(enabled: bool) -> AcpClient {
    client_with_launch(enabled, &[]).await
}

async fn client_with_launch(enabled: bool, launch: &[(String, String)]) -> AcpClient {
    let script = r#"import json,sys
for line in sys.stdin:
 m=json.loads(line)
 if 'id' not in m: continue
 p=m.get('params',{})
 result={'sessionId':p.get('sessionId','session-a'),'received':p}
 if m['method']=='session/prompt': result={'stopReason':'end_turn'}
 print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':result}),flush=True)
"#;
    let mut acp = AcpClient::spawn_with_env(
        "python3",
        &["-u".into(), "-c".into(), script.into()],
        &[(
            "BUZZ_ROUTE_LABELS".into(),
            if enabled { "1" } else { "0" }.into(),
        )],
        false,
        launch,
    )
    .await
    .unwrap();
    // The synthetic protocol peer stands in for Claude's existing env extension.
    acp.standard_adapter = Some(StandardAdapterKind::Claude);
    acp
}

async fn scope(acp: &mut AcpClient, channel: uuid::Uuid, turn: &str) {
    acp.begin_publication(
        "wss://relay.invalid",
        &"a".repeat(64),
        Some(channel),
        turn,
        Duration::from_secs(60),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn publication_scope_rotates_native_and_mcp_environment_and_retires() {
    let mut acp = client(true).await;
    let channel = uuid::Uuid::new_v4();
    scope(&mut acp, channel, "turn-a").await;
    let response = acp
        .session_new_full(
            "/workspace",
            vec![McpServer {
                name: "tools".into(),
                command: "tools".into(),
                args: vec![],
                env: vec![EnvVar {
                    name: "UNCHANGED".into(),
                    value: "yes".into(),
                }],
            }],
            Some(SystemPromptTransport::ClaudeMeta("instructions")),
            Some("pilot"),
        )
        .await
        .unwrap();
    let params = &response.raw["received"];
    let env = &params["_meta"]["claudeCode"]["options"]["env"];
    let first_path = std::path::PathBuf::from(env["BUZZ_PUBLICATION_CONTEXT"].as_str().unwrap());
    let token = env["BUZZ_PUBLICATION_TOKEN"].as_str().unwrap().to_owned();
    let context: serde_json::Value =
        serde_json::from_slice(&std::fs::read(first_path.join("context.json")).unwrap()).unwrap();
    assert_eq!(context["version"], 2);
    assert_eq!(context["relay"], "https://relay.invalid");
    assert_eq!(context["channel_id"], channel.to_string());
    assert_eq!(context["agent_pubkey"], "a".repeat(64));
    assert_eq!(context["token"], token);
    assert!(
        !first_path.join("route.json").exists(),
        "no fabricated primary receipt"
    );
    for name in [
        "BUZZ_ROUTE_LABELS",
        "BUZZ_PUBLICATION_CONTEXT",
        "BUZZ_PUBLICATION_TOKEN",
    ] {
        assert!(params["mcpServers"][0]["env"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["name"] == name && entry["value"] == env[name]));
    }
    assert_eq!(params["_meta"]["systemPrompt"]["append"], "instructions");
    assert_eq!(params["_meta"]["sessionTitle"], "pilot");
    // A publisher admitted under this lock must finish signing before the
    // harness reports cancellation complete or removes the stable inode.
    let signer = File::open(first_path.join("lock")).unwrap();
    FileExt::lock_shared(&signer).unwrap();
    let session_id = response.session_id.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let cancellation = tokio::spawn(async move {
        started_tx.send(()).unwrap();
        let result = acp.session_cancel(&session_id).await;
        (acp, result)
    });
    started_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let completed_while_signing = cancellation.is_finished();
    let directory_survived = first_path.join("context.json").exists();
    drop(signer);
    let (mut acp, result) = tokio::time::timeout(Duration::from_secs(2), cancellation)
        .await
        .unwrap()
        .unwrap();
    result.unwrap();
    assert!(
        !completed_while_signing,
        "cancellation completed during signing"
    );
    assert!(
        directory_survived,
        "retirement removed a signer's stable lock"
    );
    acp.retire_publication().await.unwrap();
    assert!(!first_path.join("context.json").exists());
    scope(&mut acp, channel, "turn-b").await;
    let refreshed = acp
        .refresh_publication(&response.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_ne!(
        refreshed["received"]["_meta"]["claudeCode"]["options"]["env"]["BUZZ_PUBLICATION_TOKEN"],
        token
    );
    assert_eq!(refreshed["received"]["sessionId"], response.session_id);
    assert_eq!(
        refreshed["received"]["_meta"]["systemPrompt"]["append"],
        "instructions"
    );
    acp.shutdown().await;
}

#[tokio::test]
async fn publication_default_off_and_unsupported_runtime_fail_closed() {
    let mut off = client(false).await;
    scope(&mut off, uuid::Uuid::new_v4(), "off").await;
    let response = off
        .session_new_full("/workspace", vec![], None, None)
        .await
        .unwrap();
    assert!(response.raw["received"].get("_meta").is_none());
    off.shutdown().await;
    // The operator's launch override wins over a persona's opt-in default.
    let mut off = client_with_launch(true, &[("BUZZ_ROUTE_LABELS".into(), "0".into())]).await;
    scope(&mut off, uuid::Uuid::new_v4(), "operator-off").await;
    let response = off
        .session_new_full("/workspace", vec![], None, None)
        .await
        .unwrap();
    assert!(response.raw["received"].get("_meta").is_none());
    off.shutdown().await;
    let mut unsupported = client(true).await;
    unsupported.standard_adapter = Some(StandardAdapterKind::Codex);
    assert!(unsupported
        .begin_publication(
            "https://relay.invalid",
            &"a".repeat(64),
            Some(uuid::Uuid::new_v4()),
            "unsupported",
            Duration::from_secs(60)
        )
        .await
        .is_err());
    unsupported.shutdown().await;
}

#[tokio::test]
async fn publication_cancel_revokes_before_adapter_acknowledgement() {
    let mut acp = client(true).await;
    scope(&mut acp, uuid::Uuid::new_v4(), "cancel").await;
    let response = acp
        .session_new_full("/workspace", vec![], None, None)
        .await
        .unwrap();
    let directory = response.raw["received"]["_meta"]["claudeCode"]["options"]["env"]
        ["BUZZ_PUBLICATION_CONTEXT"]
        .as_str()
        .unwrap();
    acp.session_cancel(&response.session_id).await.unwrap();
    assert!(!std::path::Path::new(directory)
        .join("context.json")
        .exists());
    scope(&mut acp, uuid::Uuid::new_v4(), "stuck-signer").await;
    let response = acp
        .session_new_full("/workspace", vec![], None, None)
        .await
        .unwrap();
    let directory = std::path::PathBuf::from(
        response.raw["received"]["_meta"]["claudeCode"]["options"]["env"]
            ["BUZZ_PUBLICATION_CONTEXT"]
            .as_str()
            .unwrap(),
    );
    let stuck = File::open(directory.join("lock")).unwrap();
    FileExt::lock_shared(&stuck).unwrap();
    let cancelled = tokio::time::timeout(
        Duration::from_secs(2),
        acp.session_cancel(&response.session_id),
    )
    .await
    .expect("retirement must be bounded");
    assert!(
        cancelled.is_err(),
        "a stuck signer must not report successful retirement"
    );
    assert!(!directory.join("context.json").exists());
    drop(stuck);
    acp.shutdown().await;
}
