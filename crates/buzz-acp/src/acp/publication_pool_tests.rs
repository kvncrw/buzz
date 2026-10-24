//! Exercise the real pool and CLI; only the ACP peer and relay are synthetic.
use super::*;
use crate::acp::{EnvVar, McpServer, StopReason};
use crate::pool::{
    run_prompt_task, ChannelInfoResolver, OwnedAgent, PromptContext, PromptOutcome, SessionState,
};
use crate::queue::{BatchEvent, FlushBatch};
use crate::scope::SessionScope;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const PEER: &str = r#"
import fcntl,hashlib,json,os,pathlib,subprocess,sys
root=pathlib.Path(sys.argv[1]); sessions={}; sequence=0; pending=None
signers=[]
def record(value):
 with (root/'wire.jsonl').open('a') as f: f.write(json.dumps(value)+'\n')
for line in sys.stdin:
 m=json.loads(line); p=m.get('params',{}); method=m.get('method')
 if method=='session/cancel' and pending is not None:
  print(json.dumps({'jsonrpc':'2.0','id':pending,'result':{'stopReason':'cancelled'}}),flush=True)
  pending=None; continue
 if 'id' not in m: continue
 if method in ('session/new','session/resume'):
  sequence+=1
  sid=p.get('sessionId','session-'+str(sequence))
  e=p['_meta']['claudeCode']['options']['env']
  record({'method':method,'session':sid,'directory':e['BUZZ_PUBLICATION_CONTEXT'],'digest':hashlib.sha256(e['BUZZ_PUBLICATION_TOKEN'].encode()).hexdigest()})
  if (root/('fail-new' if method=='session/new' else 'fail-resume')).exists():
   print(json.dumps({'jsonrpc':'2.0','id':m['id'],'error':{'code':-32603,'message':'synthetic setup failure'}}),flush=True); continue
  sessions[sid]=p; result={'sessionId':sid}
 elif method=='session/prompt':
  s=sessions[p['sessionId']]; native=s['_meta']['claudeCode']['options']['env']
  context=({'channel_id':os.environ['BUZZ_TEST_PUBLICATION_CHANNEL']} if native['BUZZ_ROUTE_LABELS']=='0' else json.loads((pathlib.Path(native['BUZZ_PUBLICATION_CONTEXT'])/'context.json').read_text()))
  if (root/'hold-signer').exists() or (root/'hold-cancel').exists():
   signer=open(pathlib.Path(native['BUZZ_PUBLICATION_CONTEXT'])/'lock')
   fcntl.flock(signer,fcntl.LOCK_SH); signers.append(signer)
   if (root/'hold-cancel').exists():
    pending=m['id']; (root/'prompt-ready').write_text('ready'); continue
   print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':{'stopReason':'end_turn'}}),flush=True); continue
  for kind,env in [('native',native),('mcp',{e['name']:e['value'] for e in s['mcpServers'][0]['env']})]:
   combined=dict(os.environ,**env); combined.pop('BUZZ_AUTH_TAG',None)
   call=subprocess.run([combined['BUZZ_TEST_PUBLICATION_CLI'],'messages','send','--channel',context['channel_id'],'--content','issuer '+kind],env=combined,capture_output=True,timeout=5)
   record({'method':'publish','kind':kind,'code':call.returncode,'directory':native['BUZZ_PUBLICATION_CONTEXT'],'channel':context['channel_id']})
   assert call.returncode==0,call.stderr.decode()
  result={'stopReason':'end_turn'}
 else: result={}
 print(json.dumps({'jsonrpc':'2.0','id':m['id'],'result':result}),flush=True)
"#;

async fn relay() -> (String, Arc<Mutex<Vec<Value>>>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let events = Arc::new(Mutex::new(Vec::new()));
    let captured = events.clone();
    let handle = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let captured = captured.clone();
            tokio::spawn(async move {
                let mut bytes = Vec::new();
                let mut chunk = [0u8; 8192];
                let (offset, length) = loop {
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() < 1024 * 1024);
                    if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..index]).to_lowercase();
                        let length: usize = headers
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length:"))
                            .unwrap()
                            .trim()
                            .parse()
                            .unwrap();
                        break (index + 4, length);
                    }
                };
                while bytes.len() < offset + length {
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() < 1024 * 1024);
                }
                let body: Value = serde_json::from_slice(&bytes[offset..offset + length]).unwrap();
                let result = if bytes.starts_with(b"POST /events ") {
                    let id = body["id"].clone();
                    captured.lock().unwrap().push(body);
                    json!({"accepted":true,"event_id":id})
                } else {
                    json!([])
                };
                let body = result.to_string();
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                let _ = socket.write_all(response.as_bytes()).await;
            });
        }
    });
    (endpoint, events, handle)
}

async fn seat(
    root: &Path,
    cli: &str,
    endpoint: &str,
    channel: Uuid,
) -> (OwnedAgent, Arc<PromptContext>) {
    let mut ctx = crate::pool::tests::make_prompt_context_no_owner();
    ctx.relay_url = endpoint.to_owned();
    ctx.rest_client.base_url = endpoint.to_owned();
    ctx.initial_message = Some("initial fixture prompt".into());
    ctx.cwd = root.to_string_lossy().into_owned();
    ctx.channel_info = ChannelInfoResolver::new(
        HashMap::from([(
            channel,
            crate::relay::ChannelInfo {
                name: "issuer-fixture".into(),
                channel_type: "stream".into(),
                description: None,
            },
        )]),
        ctx.rest_client.clone(),
    );
    let environment = vec![
        ("BUZZ_ROUTE_LABELS".into(), "1".into()),
        ("BUZZ_TEST_PUBLICATION_CLI".into(), cli.into()),
        ("BUZZ_RELAY_URL".into(), endpoint.into()),
        ("BUZZ_TEST_PUBLICATION_CHANNEL".into(), channel.to_string()),
        (
            "BUZZ_PRIVATE_KEY".into(),
            ctx.agent_keys.secret_key().to_secret_hex(),
        ),
    ];
    ctx.mcp_servers = vec![McpServer {
        name: "fixture".into(),
        command: "fixture".into(),
        args: vec![],
        env: vec![EnvVar {
            name: "UNCHANGED".into(),
            value: "yes".into(),
        }],
    }];
    let mut acp = AcpClient::spawn_with_env(
        "python3",
        &[
            "-E".into(),
            "-u".into(),
            "-c".into(),
            PEER.into(),
            root.to_string_lossy().into_owned(),
        ],
        &[],
        false,
        &environment,
    )
    .await
    .unwrap();
    acp.standard_adapter = Some(StandardAdapterKind::Claude);
    (
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
            agent_name: "issuer-fixture".into(),
            goose_system_prompt_supported: None,
            protocol_version: 2,
        },
        Arc::new(ctx),
    )
}

async fn turn(
    agent: OwnedAgent,
    ctx: Arc<PromptContext>,
    channel: Uuid,
    turn: &str,
) -> crate::pool::PromptResult {
    turn_with_control(agent, ctx, channel, turn, None).await
}

async fn turn_with_control(
    agent: OwnedAgent,
    ctx: Arc<PromptContext>,
    channel: Uuid,
    turn: &str,
    control: Option<tokio::sync::oneshot::Receiver<crate::pool::ControlSignal>>,
) -> crate::pool::PromptResult {
    let event = nostr::EventBuilder::new(nostr::Kind::Custom(9), "fixture request")
        .sign_with_keys(&nostr::Keys::generate())
        .unwrap();
    let batch = FlushBatch {
        channel_id: channel,
        scope: SessionScope::Conversation {
            channel_id: channel,
        },
        events: vec![BatchEvent {
            edit: None,
            event,
            prompt_tag: "fixture".into(),
            received_at: std::time::Instant::now(),
        }],
        cancelled_events: vec![],
        cancel_reason: None,
    };
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::time::timeout(
        Duration::from_secs(15),
        run_prompt_task(
            agent,
            Some(batch),
            None,
            ctx,
            sender,
            control,
            turn.into(),
            Default::default(),
        ),
    )
    .await
    .unwrap();
    receiver.recv().await.unwrap()
}

#[tokio::test]
async fn publication_pool_completed_cancel_preserves_control_and_queue() {
    use crate::pool::{AgentPool, ControlSignal, TaskMeta};
    use crate::queue::{CancelReason, EventQueue, QueuedEvent};
    use std::collections::HashSet;

    for (signal, reason) in [
        (ControlSignal::Cancel, None),
        (ControlSignal::Steer, Some(CancelReason::Steer)),
        (ControlSignal::Interrupt, Some(CancelReason::Interrupt)),
    ] {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("hold-cancel"), "1").unwrap();
        let (endpoint, _, server) = relay().await;
        let channel = Uuid::new_v4();
        let (mut agent, mut ctx) = seat(root.path(), "unused", &endpoint, channel).await;
        Arc::get_mut(&mut ctx).unwrap().initial_message = None;
        Arc::get_mut(&mut ctx).unwrap().dedup_mode = crate::config::DedupMode::Queue;
        let observer = crate::observer::ObserverHandle::in_process();
        agent.acp.set_observer(Some(observer.clone()), 0);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(turn_with_control(
            agent,
            ctx,
            channel,
            "control-cancel",
            Some(rx),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !root.path().join("prompt-ready").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tx.send(signal).unwrap();
        let result = task.await.unwrap();
        assert!(
            matches!(result.outcome, PromptOutcome::Cancelled),
            "acknowledged control must remain Cancelled"
        );
        assert_eq!(result.batch.is_some(), reason.is_some());
        if let Some(batch) = &result.batch {
            assert_eq!(batch.cancel_reason, reason);
            assert_eq!(batch.events.len(), 1);
        }
        assert!(observer
            .snapshot()
            .iter()
            .any(|event| event.kind == "publication_retirement_failed"));
        let mut pool = AgentPool::from_slots(vec![None]);
        let id = pool.join_set.spawn(async {}).id();
        pool.task_map_mut().insert(
            id,
            TaskMeta {
                agent_index: 0,
                channel_id: Some(channel),
                scope: None,
                turn_id: result.turn_id.clone(),
                recoverable_batch: None,
                control_tx: None,
                steer_tx: None,
                successful_steer_deliveries: HashSet::new(),
            },
        );
        let mut queue = EventQueue::new(crate::config::DedupMode::Queue);
        let next = nostr::EventBuilder::new(nostr::Kind::Custom(9), "next request")
            .sign_with_keys(&nostr::Keys::generate())
            .unwrap();
        queue.push(QueuedEvent {
            edit: None,
            channel_id: channel,
            scope: SessionScope::Conversation {
                channel_id: channel,
            },
            event: next.clone(),
            received_at: std::time::Instant::now(),
            prompt_tag: "fixture".into(),
        });
        let mut history = vec![crate::SlotCircuit {
            crash_times: vec![],
            open_until: None,
            respawn_in_flight: false,
        }];
        let (respawn_tx, _) = tokio::sync::mpsc::channel(8);
        let mut respawns = tokio::task::JoinSet::new();
        crate::handle_prompt_result(
            &mut pool,
            &mut queue,
            &crate::build_mcp_servers_tests::test_config(),
            result,
            &mut false,
            &HashSet::new(),
            &mut history,
            &respawn_tx,
            &mut respawns,
            Some(observer.clone()),
            None,
            &mut tokio::time::Instant::now(),
        );
        assert_eq!(pool.live_count(), 1);
        assert!(respawns.is_empty());
        assert!(!observer
            .snapshot()
            .iter()
            .any(|event| event.kind == "turn_error"));
        let batch = queue
            .flush_next()
            .expect("next request must be immediately available without retry delay");
        assert_eq!(batch.events.len(), 1);
        assert_eq!(batch.events[0].event.id, next.id);
        assert_eq!(batch.cancel_reason, reason);
        assert_eq!(batch.cancelled_events.len(), usize::from(reason.is_some()));
        if reason.is_some() {
            assert_eq!(batch.cancelled_events[0].event.content, "fixture request");
        }
        server.abort();
    }
}

#[tokio::test]
#[ignore = "requires BUZZ_TEST_PUBLICATION_CLI and BUZZ_TEST_ISSUER_OUTPUT"]
async fn publication_pool_e2e_heartbeat_reuse() {
    let cli = std::env::var("BUZZ_TEST_PUBLICATION_CLI").unwrap();
    let output = PathBuf::from(std::env::var("BUZZ_TEST_ISSUER_OUTPUT").unwrap());
    let root = tempfile::Builder::new()
        .prefix("issuer-heartbeat-")
        .tempdir_in(output)
        .unwrap()
        .keep();
    let (endpoint, events, server) = relay().await;
    let channel = Uuid::new_v4();
    let (mut agent, ctx) = seat(&root, &cli, &endpoint, channel).await;
    for turn in ["heartbeat-new", "heartbeat-reuse"] {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::time::timeout(
            Duration::from_secs(15),
            run_prompt_task(
                agent,
                None,
                Some("heartbeat fixture".into()),
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
            "heartbeat CLI send failed"
        );
        agent = result.agent;
    }
    agent.acp.shutdown().await;
    let wire = records(&root);
    assert_eq!(
        wire.iter()
            .filter(|entry| entry["method"] == "session/new")
            .count(),
        1
    );
    assert_eq!(
        wire.iter()
            .filter(|entry| entry["method"] == "session/resume")
            .count(),
        0
    );
    let messages: Vec<_> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == 9)
        .cloned()
        .collect();
    assert_eq!(
        messages.len(),
        4,
        "native and MCP sends from both heartbeat turns"
    );
    for message in &messages {
        let event: nostr::Event = serde_json::from_value(message.clone()).unwrap();
        event.verify().unwrap();
        assert_eq!(event.pubkey, ctx.agent_keys.public_key());
        assert!(
            matches!(event.content.as_str(), "issuer native" | "issuer mcp"),
            "heartbeat must stay unlabeled"
        );
    }
    std::fs::write(root.join("result.json"), serde_json::to_vec_pretty(&json!({"passed":true,"messages":messages,"wire":wire,"checks":["new heartbeat","reused heartbeat","native CLI send","MCP CLI send","unlabeled signed replies"]})).unwrap()).unwrap();
    server.abort();
    println!("heartbeat evidence: {}", root.join("result.json").display());
}

fn records(root: &Path) -> Vec<Value> {
    std::fs::read_to_string(root.join("wire.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[tokio::test]
async fn publication_success_survives_stuck_signer_retirement() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("hold-signer"), "1").unwrap();
    let (endpoint, _, server) = relay().await;
    let channel = Uuid::new_v4();
    let (agent, mut ctx) = seat(root.path(), "unused", &endpoint, channel).await;
    Arc::get_mut(&mut ctx).unwrap().initial_message = None;
    let observer = crate::observer::ObserverHandle::in_process();
    let mut agent = agent;
    agent.acp.set_observer(Some(observer.clone()), 0);
    let mut result = turn(agent, ctx, channel, "stuck-signer-success").await;
    assert!(
        matches!(result.outcome, PromptOutcome::Ok(_)),
        "retirement must preserve the completed adapter outcome"
    );
    assert!(
        observer
            .snapshot()
            .iter()
            .any(|event| event.kind == "publication_retirement_failed"),
        "retirement failure must remain observable"
    );
    for entry in records(root.path()) {
        assert!(!Path::new(entry["directory"].as_str().unwrap()).exists());
    }
    result.agent.acp.shutdown().await;
    server.abort();
}

#[tokio::test]
#[ignore = "requires BUZZ_TEST_PUBLICATION_CLI and BUZZ_TEST_ISSUER_OUTPUT"]
async fn publication_pool_e2e() {
    let cli = std::env::var("BUZZ_TEST_PUBLICATION_CLI").unwrap();
    let output = PathBuf::from(std::env::var("BUZZ_TEST_ISSUER_OUTPUT").unwrap());
    let root = tempfile::Builder::new()
        .prefix("issuer-pool-")
        .tempdir_in(output)
        .unwrap()
        .keep();
    let a_dir = root.join("a");
    let b_dir = root.join("b");
    std::fs::create_dir(&a_dir).unwrap();
    std::fs::create_dir(&b_dir).unwrap();
    let (endpoint, events, server) = relay().await;
    let a_channel = Uuid::new_v4();
    let b_channel = Uuid::new_v4();
    let (a, a_ctx) = seat(&a_dir, &cli, &endpoint, a_channel).await;
    let (b, b_ctx) = seat(&b_dir, &cli, &endpoint, b_channel).await;
    let authors = HashMap::from([
        (
            a_channel.to_string(),
            a_ctx.agent_keys.public_key().to_hex(),
        ),
        (
            b_channel.to_string(),
            b_ctx.agent_keys.public_key().to_hex(),
        ),
    ]);
    let (a, mut b) = tokio::join!(
        turn(a, a_ctx.clone(), a_channel, "a1"),
        turn(b, b_ctx, b_channel, "b1")
    );
    assert!(matches!(a.outcome, PromptOutcome::Ok(_)));
    assert!(matches!(b.outcome, PromptOutcome::Ok(_)));
    let a = turn(a.agent, a_ctx.clone(), a_channel, "a2").await;
    assert!(matches!(a.outcome, PromptOutcome::Ok(_)));
    let before_failure = records(&a_dir)
        .iter()
        .filter(|r| r["method"] == "publish")
        .count();
    std::fs::write(a_dir.join("fail-resume"), "1").unwrap();
    let a = turn(a.agent, a_ctx.clone(), a_channel, "a3-fails").await;
    assert!(matches!(a.outcome, PromptOutcome::Error(_)));
    assert_eq!(
        records(&a_dir)
            .iter()
            .filter(|r| r["method"] == "publish")
            .count(),
        before_failure
    );
    std::fs::write(a_dir.join("fail-new"), "1").unwrap();
    let mut a = turn(a.agent, a_ctx, a_channel, "a4-setup-fails").await;
    assert!(matches!(a.outcome, PromptOutcome::Error(_)));
    let a_records = records(&a_dir);
    let b_records = records(&b_dir);
    let issuance: Vec<_> = a_records
        .iter()
        .chain(&b_records)
        .filter(|r| r["method"] != "publish")
        .collect();
    let paths: std::collections::HashSet<_> = issuance
        .iter()
        .map(|r| r["directory"].as_str().unwrap())
        .collect();
    assert_eq!(
        paths.len(),
        issuance.len(),
        "each turn and seat needs its own scope"
    );
    let tokens: std::collections::HashSet<_> = issuance
        .iter()
        .map(|record| record["digest"].as_str().unwrap())
        .collect();
    assert_eq!(
        tokens.len(),
        issuance.len(),
        "turn tokens must not be reused"
    );
    for path in &paths {
        assert!(!Path::new(path).exists(), "terminal scope remained active");
    }
    assert_eq!(
        a_records
            .iter()
            .filter(|r| r["method"] == "session/new")
            .count(),
        2
    );
    assert_eq!(
        a_records
            .iter()
            .filter(|r| r["method"] == "session/resume")
            .count(),
        2
    );
    a.agent.acp.shutdown().await;
    b.agent.acp.shutdown().await;
    let messages: Vec<_> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == 9)
        .cloned()
        .collect();
    assert_eq!(
        messages.len(),
        10,
        "two paths for initial and main prompts, then reuse"
    );
    for message in &messages {
        let event: nostr::Event = serde_json::from_value(message.clone()).unwrap();
        event.verify().unwrap();
        let channel = message["tags"]
            .as_array()
            .unwrap()
            .iter()
            .find(|tag| tag[0] == "h")
            .unwrap()[1]
            .as_str()
            .unwrap();
        assert_eq!(message["pubkey"].as_str().unwrap(), authors[channel]);
        assert!(message["content"]
            .as_str()
            .unwrap()
            .contains("[routing unknown]"));
    }
    std::fs::write(root.join("result.json"),serde_json::to_vec_pretty(&json!({"passed":true,"messages":messages,"issuedScopes":issuance.len(),"checks":["initial prompt","parallel seats","native and MCP CLI","session reuse","failed resume","failed setup","terminal retirement"]})).unwrap()).unwrap();
    server.abort();
    println!(
        "issuer pool evidence: {}",
        root.join("result.json").display()
    );
}
