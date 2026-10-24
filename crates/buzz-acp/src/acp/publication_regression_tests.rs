//! Review regressions bind real subprocess, observer, dispatch, and result paths.
use super::*;
use crate::acp::StopReason;
use crate::observer::ObserverHandle;
use crate::pool::{OwnedAgent, PromptOutcome, PromptResult, PromptSource, SessionState};

fn owned(acp: AcpClient) -> OwnedAgent {
    OwnedAgent {
        index: 0,
        acp,
        state: SessionState::default(),
        model_capabilities: None,
        desired_model: None,
        model_overridden: false,
        desired_model_request_id: None,
        desired_model_pending_ack: false,
        startup_effort: None,
        agent_name: "issuer-regression".into(),
        goose_system_prompt_supported: None,
        protocol_version: 2,
    }
}

async fn return_to_pool(result: PromptResult) {
    use crate::pool::{AgentPool, TaskMeta};
    use std::collections::HashSet;
    let mut pool = AgentPool::from_slots(vec![None]);
    let id = pool.join_set.spawn(async {}).id();
    pool.task_map_mut().insert(
        id,
        TaskMeta {
            agent_index: 0,
            channel_id: None,
            scope: None,
            turn_id: result.turn_id.clone(),
            recoverable_batch: None,
            control_tx: None,
            steer_tx: None,
            successful_steer_deliveries: HashSet::new(),
        },
    );
    let mut queue = crate::queue::EventQueue::new(crate::config::DedupMode::Queue);
    let config = crate::build_mcp_servers_tests::test_config();
    let mut history = vec![crate::SlotCircuit {
        crash_times: vec![],
        open_until: None,
        respawn_in_flight: false,
    }];
    let (tx, _rx) = tokio::sync::mpsc::channel(8);
    let mut tasks = tokio::task::JoinSet::new();
    crate::handle_prompt_result(
        &mut pool,
        &mut queue,
        &config,
        result,
        &mut true,
        &HashSet::new(),
        &mut history,
        &tx,
        &mut tasks,
        None,
        None,
        &mut tokio::time::Instant::now(),
    );
    assert!(
        tasks.is_empty(),
        "publication failure must not respawn the seat"
    );
    assert!(!history[0].respawn_in_flight);
    assert_eq!(pool.live_count(), 1);
}

#[tokio::test]
async fn publication_heartbeat_runs_and_reuses_with_labels_enabled() {
    let mut acp = client(true).await;
    scope(&mut acp, Uuid::new_v4(), "prior-channel").await;
    let prior = acp
        .publication
        .active
        .as_ref()
        .unwrap()
        .directory
        .path()
        .to_owned();
    let observer = ObserverHandle::in_process();
    let mut agent = owned(acp);
    agent.acp.set_observer(Some(observer.clone()), 0);
    let mut ctx = crate::pool::tests::make_prompt_context_no_owner();
    ctx.mcp_servers = vec![McpServer {
        name: "tools".into(),
        command: "tools".into(),
        args: vec![],
        env: vec![],
    }];
    let ctx = std::sync::Arc::new(ctx);
    for turn in ["heartbeat-new", "heartbeat-reuse"] {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::time::timeout(
            Duration::from_secs(5),
            crate::pool::run_prompt_task(
                agent,
                None,
                Some("heartbeat".into()),
                ctx.clone(),
                tx,
                None,
                turn.into(),
                Default::default(),
            ),
        )
        .await
        .unwrap();
        let result = rx.recv().await.unwrap();
        assert!(
            matches!(result.outcome, PromptOutcome::Ok(StopReason::EndTurn)),
            "heartbeat must complete, never Protocol"
        );
        agent = result.agent;
    }
    assert!(agent.state.heartbeat_session.is_some());
    assert!(!prior.exists());
    assert!(agent.acp.publication.active.is_none());
    // The synthetic peer echoes received params, independent of write redaction.
    let frames = observer.snapshot();
    for (method, count) in [("session/new", 1), ("session/prompt", 2)] {
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame.kind == "acp_write" && frame.payload["method"] == method)
                .count(),
            count
        );
    }
    let setup: Vec<_> = frames
        .iter()
        .filter_map(|frame| {
            let params = &frame.payload["result"]["received"];
            params.get("mcpServers").map(|_| params)
        })
        .collect();
    assert_eq!(setup.len(), 1);
    for params in setup {
        let env = &params["_meta"]["claudeCode"]["options"]["env"];
        assert_eq!(env["BUZZ_ROUTE_LABELS"], "0");
        assert_eq!(env["BUZZ_PUBLICATION_TOKEN"], "");
        assert!(params["mcpServers"][0]["env"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["name"] == "BUZZ_ROUTE_LABELS" && entry["value"] == "0"));
    }
    return_to_pool(PromptResult {
        agent,
        source: PromptSource::Heartbeat,
        turn_id: "heartbeat-reuse".into(),
        outcome: PromptOutcome::Ok(StopReason::EndTurn),
        batch: None,
    })
    .await;
}

#[tokio::test]
async fn publication_preserves_seven_day_configuration_ceiling() {
    let mut acp = client(true).await;
    for seconds in [86_401, crate::config::MAX_TURN_DURATION_CEILING_SECS] {
        acp.begin_publication(
            "https://relay.invalid",
            &"a".repeat(64),
            Some(Uuid::new_v4()),
            "long-turn",
            Duration::from_secs(seconds),
        )
        .await
        .unwrap();
    }
    for seconds in [0, crate::config::MAX_TURN_DURATION_CEILING_SECS + 1] {
        assert!(acp
            .begin_publication(
                "https://relay.invalid",
                &"a".repeat(64),
                Some(Uuid::new_v4()),
                "invalid-turn",
                Duration::from_secs(seconds)
            )
            .await
            .is_err());
    }
    acp.shutdown().await;
}

#[tokio::test]
async fn publication_session_observer_redacts_credentials() {
    let mut acp = client(true).await;
    let observer = ObserverHandle::in_process();
    acp.set_observer(Some(observer.clone()), 0);
    scope(&mut acp, Uuid::new_v4(), "first").await;
    let created = acp
        .session_new_full(
            "/workspace",
            vec![McpServer {
                name: "tools".into(),
                command: "tools".into(),
                args: vec![],
                env: vec![EnvVar {
                    name: "BUZZ_PRIVATE_KEY".into(),
                    value: "synthetic-key-canary".into(),
                }],
            }],
            None,
            None,
        )
        .await
        .unwrap();
    scope(&mut acp, Uuid::new_v4(), "second").await;
    acp.refresh_publication(&created.session_id).await.unwrap();
    let writes: Vec<_> = observer
        .snapshot()
        .into_iter()
        .filter(|frame| frame.kind == "acp_write")
        .collect();
    assert_eq!(writes.len(), 2);
    for (frame, method) in writes.iter().zip(["session/new", "session/resume"]) {
        assert_eq!(frame.payload["method"], method);
        assert!(frame.payload.get("params").is_none());
        assert!(frame.payload.get("id").is_some());
        assert!(!frame.payload.to_string().contains("synthetic-key-canary"));
    }
    acp.shutdown().await;
}

#[tokio::test]
async fn publication_stuck_signer_reaches_cancel_and_pool_without_protocol_error() {
    let mut acp = client(true).await;
    let observer = ObserverHandle::in_process();
    acp.set_observer(Some(observer.clone()), 0);
    scope(&mut acp, Uuid::new_v4(), "cancel").await;
    let directory = acp
        .publication
        .active
        .as_ref()
        .unwrap()
        .directory
        .path()
        .to_owned();
    let signer = File::open(directory.join("lock")).unwrap();
    FileExt::lock_shared(&signer).unwrap();
    let error = acp.session_cancel("session-a").await.unwrap_err();
    assert!(
        !matches!(error, AcpError::Protocol(_)),
        "signer drain is not a broken ACP pipe"
    );
    assert!(observer
        .snapshot()
        .iter()
        .any(|frame| frame.kind == "acp_write" && frame.payload["method"] == "session/cancel"));
    assert!(!directory.exists());
    drop(signer);
    return_to_pool(PromptResult {
        agent: owned(acp),
        source: PromptSource::Heartbeat,
        turn_id: "cancel".into(),
        outcome: PromptOutcome::Error(error),
        batch: None,
    })
    .await;
}

#[tokio::test]
async fn publication_control_cancel_keeps_adapter_grace_despite_stuck_signer() {
    let mut acp = client(true).await;
    let observer = ObserverHandle::in_process();
    acp.set_observer(Some(observer.clone()), 0);
    scope(&mut acp, Uuid::new_v4(), "control-cancel").await;
    let directory = acp
        .publication
        .active
        .as_ref()
        .unwrap()
        .directory
        .path()
        .to_owned();
    let signer = File::open(directory.join("lock")).unwrap();
    FileExt::lock_shared(&signer).unwrap();
    // No matching response is produced: cancellation must still reach its ACP deadline.
    acp.last_prompt_id = Some(999);
    let grace = Duration::from_secs(2);
    let error = acp
        .cancel_with_cleanup_grace("session-a", grace)
        .await
        .unwrap_err();
    assert!(matches!(error, AcpError::CancelDrainTimeout(duration) if duration == grace));
    assert!(observer
        .snapshot()
        .iter()
        .any(|frame| frame.kind == "acp_write" && frame.payload["method"] == "session/cancel"));
    drop(signer);
    acp.shutdown().await;
}
