//! Opt-in turn fencing for model-authored sends and edits.
//!
//! The harness owns the context directory and retires tokens while holding an
//! exclusive lock on its stable `lock` file. A publisher holds a shared lock
//! from validation through signing. Neither participant may replace that inode.
//! This orders retirement against signing; it cannot recall an event that was
//! already signed. Route snapshots use this same lock through signing.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::Deserialize;

use crate::error::CliError;

const MAX_CONTEXT_BYTES: u64 = 16 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Context {
    version: u8,
    pub(crate) token: String,
    state: String,
    pub(crate) expires_at: u64,
    pub(crate) relay: String,
    pub(crate) agent_pubkey: String,
    pub(crate) channel_id: String,
}

pub(crate) struct PublicationFence {
    _lock: File,
    pub(crate) directory: PathBuf,
    pub(crate) context: Context,
}

fn rejected(reason: &str) -> CliError {
    // Do not include the supplied token or record in diagnostics.
    CliError::Other(format!("publication fence rejected signing: {reason}"))
}

pub(crate) fn open_regular(path: &Path) -> Result<File, CliError> {
    // Check before open: opening a FIFO would otherwise block indefinitely.
    // The harness owns this directory and only atomically replaces JSON with
    // another regular file; it must never replace the stable lock file.
    if !std::fs::symlink_metadata(path)
        .map_err(|_| rejected("context file unavailable"))?
        .file_type()
        .is_file()
    {
        return Err(rejected("context files must be regular files"));
    }
    File::open(path).map_err(|_| rejected("context file unreadable"))
}

/// Acquire an optional turn fence and keep its file alive until signing ends.
///
/// With both variables absent or empty, ordinary clients and channel-less
/// heartbeats keep their existing behavior. Partial configuration fails closed. A busy lock
/// also fails closed rather than blocking cancellation behind network work.
pub(crate) fn acquire(
    relay: &str,
    agent_pubkey: &str,
    channel_id: &str,
) -> Result<Option<PublicationFence>, CliError> {
    let directory = std::env::var_os("BUZZ_PUBLICATION_CONTEXT").filter(|value| !value.is_empty());
    let token = std::env::var_os("BUZZ_PUBLICATION_TOKEN").filter(|value| !value.is_empty());
    let (directory, token) = match (directory, token) {
        (None, None) => return Ok(None),
        (Some(directory), Some(token)) => (PathBuf::from(directory), token),
        _ => return Err(rejected("incomplete configuration")),
    };
    let token = token
        .to_str()
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or_else(|| rejected("invalid token"))?;
    if !directory.is_absolute() {
        return Err(rejected("context directory must be absolute"));
    }
    let lock = open_regular(&directory.join("lock"))?;
    FileExt::try_lock_shared(&lock).map_err(|_| rejected("context busy"))?;
    let file = open_regular(&directory.join("context.json"))?;
    if !file
        .metadata()
        .map_err(|_| rejected("context unreadable"))?
        .is_file()
    {
        return Err(rejected("context must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_CONTEXT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| rejected("context unreadable"))?;
    if bytes.len() as u64 > MAX_CONTEXT_BYTES {
        return Err(rejected("context exceeds size limit"));
    }
    let context: Context =
        serde_json::from_slice(&bytes).map_err(|_| rejected("malformed context"))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| rejected("clock unavailable"))?
        .as_secs();
    if !matches!(context.version, 1 | 2) || context.state != "active" || context.token != token {
        return Err(rejected("inactive or superseded token"));
    }
    if context.version == 2 {
        let issuer = open_regular(&directory.join("issuer.lock"))?;
        // An exclusive lease belongs only to the live harness. An orphaned
        // context must fail closed even if a crash left its JSON active.
        match FileExt::try_lock_shared(&issuer) {
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => {}
            _ => return Err(rejected("issuer is no longer active")),
        }
    }
    if context.expires_at <= now {
        return Err(rejected("expired context"));
    }
    if context.relay.trim_end_matches('/') != relay.trim_end_matches('/')
        || context.agent_pubkey != agent_pubkey
        || context.channel_id != channel_id
    {
        return Err(rejected("identity or destination mismatch"));
    }
    Ok(Some(PublicationFence {
        _lock: lock,
        directory,
        context,
    }))
}

#[cfg(test)]
mod tests {
    #[test]
    fn publication_empty_environment_is_unconfigured_but_partial_is_rejected() {
        const CASE: &str = "BUZZ_TEST_PUBLICATION_ENV_CASE";
        if let Ok(case) = std::env::var(CASE) {
            let result = super::acquire("https://relay.invalid", "agent", "channel");
            if case == "unconfigured" {
                assert!(
                    matches!(result, Ok(None)),
                    "empty credentials must be absent"
                );
            } else {
                assert!(result.is_err(), "partial credentials must fail closed");
            }
            return;
        }
        // Each case gets its own process so parallel tests cannot see these values.
        for (directory, token, expected) in [
            (None, None, "unconfigured"),
            (Some(""), Some(""), "unconfigured"),
            (Some(""), None, "unconfigured"),
            (None, Some(""), "unconfigured"),
            (Some("/missing"), Some(""), "rejected"),
            (Some(""), Some("token"), "rejected"),
            (Some("/missing"), None, "rejected"),
            (None, Some("token"), "rejected"),
        ] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "publication_fence::tests::publication_empty_environment_is_unconfigured_but_partial_is_rejected",
                    "--nocapture",
                ])
                .env(CASE, expected)
                .env("BUZZ_ROUTE_LABELS", "0")
                .env_remove("BUZZ_PUBLICATION_CONTEXT")
                .env_remove("BUZZ_PUBLICATION_TOKEN");
            if let Some(value) = directory {
                command.env("BUZZ_PUBLICATION_CONTEXT", value);
            }
            if let Some(value) = token {
                command.env("BUZZ_PUBLICATION_TOKEN", value);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "context={directory:?}, token={token:?}: {} {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
