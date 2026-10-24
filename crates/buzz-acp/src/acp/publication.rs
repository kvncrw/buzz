//! Per-turn publication authority for the opt-in Claude ACP pilot.
//! Missing route evidence stays unknown; this module never asserts a model route.
use super::{AcpClient, AcpError, StandardAdapterKind};
use fs2::FileExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use uuid::Uuid;

pub(super) struct State {
    enabled: bool,
    unscoped_turn: bool,
    active: Option<Lease>,
    sessions: HashMap<String, Value>,
}

struct Lease {
    directory: TempDir,
    // Rust files are close-on-exec. The adapter must not inherit this lease.
    _issuer: File,
    token: String,
}

fn failure() -> AcpError {
    AcpError::Protocol("publication context unavailable; refusing labeled turn".into())
}

impl State {
    pub(super) fn from_command(command: &tokio::process::Command) -> Result<Self, AcpError> {
        let value = command
            .as_std()
            .get_envs()
            .find(|(key, _)| *key == "BUZZ_ROUTE_LABELS")
            .map(|(_, value)| value.map(std::ffi::OsStr::to_os_string))
            .unwrap_or_else(|| std::env::var_os("BUZZ_ROUTE_LABELS"));
        let value = value
            .map(|value| value.into_string().map_err(|_| failure()))
            .transpose()?;
        let enabled = match value.as_deref() {
            None | Some("0") => false,
            Some("1") => true,
            _ => {
                return Err(AcpError::Protocol(
                    "BUZZ_ROUTE_LABELS must be 0 or 1".into(),
                ))
            }
        };
        Ok(Self {
            enabled,
            unscoped_turn: false,
            active: None,
            sessions: HashMap::new(),
        })
    }

    pub(super) fn inject(&self, params: &mut Value) -> Result<(), AcpError> {
        if !self.enabled {
            return Ok(());
        }
        let env = if self.unscoped_turn {
            // A heartbeat has no channel authority. Clear inherited values in
            // both execution environments, including on session reuse.
            json!({"BUZZ_ROUTE_LABELS": "0", "BUZZ_PUBLICATION_CONTEXT": "", "BUZZ_PUBLICATION_TOKEN": ""})
        } else {
            let lease = self.active.as_ref().ok_or_else(failure)?;
            json!({
                "BUZZ_ROUTE_LABELS": "1",
                "BUZZ_PUBLICATION_CONTEXT": lease.directory.path().to_str().ok_or_else(failure)?,
                "BUZZ_PUBLICATION_TOKEN": lease.token,
            })
        };
        params["_meta"]["claudeCode"]["options"]["env"] = env.clone();
        for server in params["mcpServers"].as_array_mut().ok_or_else(failure)? {
            let entries = server["env"].as_array_mut().ok_or_else(failure)?;
            entries.retain(|entry| {
                !env.as_object().is_some_and(|env| {
                    entry["name"]
                        .as_str()
                        .is_some_and(|name| env.contains_key(name))
                })
            });
            for (name, value) in env.as_object().ok_or_else(failure)? {
                entries.push(json!({ "name": name, "value": value }));
            }
        }
        Ok(())
    }

    pub(super) fn remember(&mut self, session: &str, params: Value) -> Result<(), AcpError> {
        if self.enabled {
            if self.sessions.len() >= 128 && !self.sessions.contains_key(session) {
                return Err(AcpError::Protocol(
                    "publication session limit reached".into(),
                ));
            }
            self.sessions.insert(session.into(), params);
        }
        Ok(())
    }
}

impl AcpClient {
    pub(crate) async fn begin_publication(
        &mut self,
        relay: &str,
        author: &str,
        channel: Option<Uuid>,
        turn: &str,
        lifetime: Duration,
    ) -> Result<(), AcpError> {
        self.retire_publication().await?;
        self.publication.unscoped_turn = channel.is_none();
        if !self.publication.enabled {
            return Ok(());
        }
        let Some(channel) = channel else {
            return Ok(());
        };
        if self.standard_adapter != Some(StandardAdapterKind::Claude) {
            return Err(AcpError::Protocol(
                "route labels currently require Claude ACP".into(),
            ));
        }
        if lifetime.is_zero()
            || lifetime > Duration::from_secs(crate::config::MAX_TURN_DURATION_CEILING_SECS)
            || turn.is_empty()
        {
            return Err(failure());
        }
        nostr::PublicKey::from_hex(author).map_err(|_| failure())?;
        let mut relay = url::Url::parse(relay).map_err(|_| failure())?;
        let scheme = match relay.scheme() {
            "ws" | "http" => "http",
            "wss" | "https" => "https",
            _ => return Err(failure()),
        };
        relay.set_scheme(scheme).map_err(|_| failure())?;
        let directory = tempfile::Builder::new()
            .prefix("buzz-publication-")
            .tempdir()?;
        let issuer = private_file(&directory.path().join("issuer.lock"))?;
        FileExt::try_lock_exclusive(&issuer)?;
        private_file(&directory.path().join("lock"))?.sync_all()?;
        let token = Uuid::new_v4().to_string();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| failure())?
            .as_secs();
        let context = json!({
            "version": 2, "token": token, "state": "active",
            "expires_at": now + lifetime.as_secs() + 120,
            "relay": relay.as_str().trim_end_matches('/'),
            "agent_pubkey": author, "channel_id": channel.to_string(),
        });
        let registration = json!({
            "version": 1, "turn_id": turn, "generation": Uuid::new_v4().to_string(),
            "token": token, "relay": context["relay"], "agent_pubkey": author,
            "channel_id": channel.to_string(),
        });
        for (name, record) in [
            ("context.json", context),
            ("registration.json", registration),
        ] {
            let mut file = private_file(&directory.path().join(name))?;
            file.write_all(record.to_string().as_bytes())?;
            file.sync_all()?;
        }
        #[cfg(unix)]
        File::open(directory.path())?.sync_all()?;
        self.publication.active = Some(Lease {
            directory,
            _issuer: issuer,
            token,
        });
        Ok(())
    }

    pub(crate) fn revoke_publication(&mut self) {
        // Drop and panic paths cannot drain signers. Releasing the issuer lease
        // denies new signers even if directory cleanup fails.
        self.publication.active = None;
    }

    pub(crate) async fn retire_publication(&mut self) -> Result<(), AcpError> {
        let directory = self.revoke_publication_for_drain();
        drain_retired(directory).await
    }

    pub(super) fn revoke_publication_for_drain(&mut self) -> Option<TempDir> {
        let lease = self.publication.active.take()?;
        let Lease {
            directory, _issuer, ..
        } = lease;
        // Stop admitting publishers before draining the ones already signing.
        // The directory remains alive through the drain, preserving its inode.
        drop(_issuer);
        Some(directory)
    }

    pub(crate) fn report_retirement_failure(&self, error: &AcpError) {
        tracing::warn!(target: "acp::publication", %error, "publication revoked; retirement failed");
        self.observe(
            "publication_retirement_failed",
            json!({"error": error.to_string()}),
        );
    }

    pub(super) fn finish_publication_cancel(
        &self,
        cancelled: Result<super::StopReason, AcpError>,
        retired: Result<(), AcpError>,
    ) -> Result<super::StopReason, AcpError> {
        if let Err(error) = &retired {
            self.report_retirement_failure(error);
        }
        // A completed stop must stay cancelled work, without retry penalties.
        // Adapter failures, including the bounded cancellation deadline, also survive.
        cancelled
    }

    pub(crate) async fn refresh_publication(
        &mut self,
        session: &str,
    ) -> Result<Option<Value>, AcpError> {
        if !self.publication.enabled {
            return Ok(None);
        }
        let mut params = self
            .publication
            .sessions
            .get(session)
            .cloned()
            .ok_or_else(failure)?;
        self.publication.inject(&mut params)?;
        params["sessionId"] = json!(session);
        let response = self.send_request("session/resume", params).await?;
        if response["sessionId"].as_str() != Some(session) {
            return Err(failure());
        }
        Ok(Some(response))
    }
}

pub(super) async fn drain_retired(directory: Option<TempDir>) -> Result<(), AcpError> {
    let Some(directory) = directory else {
        return Ok(());
    };
    let lock = File::open(directory.path().join("lock"))?;
    let grace = Duration::from_secs(1);
    let deadline = tokio::time::Instant::now() + grace;
    loop {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(AcpError::PublicationDrainTimeout(grace));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn private_file(path: &std::path::Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

#[cfg(test)]
#[path = "publication_tests.rs"]
mod tests;

#[cfg(all(test, unix))]
#[path = "publication_pool_tests.rs"]
mod pool_tests;
