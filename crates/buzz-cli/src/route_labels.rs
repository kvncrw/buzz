//! Disabled-by-default formatting from a trusted, cumulative turn snapshot.
//!
//! The harness owns snapshot integrity and updates it under the publication
//! lock. This consumer never treats a model-authored prefix as route evidence.

use std::io::Read;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;

use crate::error::CliError;
use crate::publication_fence::{open_regular, PublicationFence};

const MAX_SNAPSHOT_BYTES: u64 = 16 * 1024;
const UNKNOWN: &str = "[routing unknown]";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u8,
    token: String,
    relay: String,
    agent_pubkey: String,
    channel_id: String,
    observed_at: u64,
    expires_at: u64,
    status: String,
    fallbacks: Vec<String>,
}

fn enabled() -> Result<bool, CliError> {
    match std::env::var_os("BUZZ_ROUTE_LABELS") {
        None => Ok(false),
        Some(value) if value == "0" => Ok(false),
        Some(value) if value == "1" => Ok(true),
        _ => Err(CliError::Usage("BUZZ_ROUTE_LABELS must be 0 or 1".into())),
    }
}

// Only standalone leading marker lines are reserved. Inline discussion
// and markers inside code blocks remain part of the original body.
fn without_labels(mut content: &str) -> &str {
    loop {
        let candidate = content.trim_start();
        let (line, rest) = candidate.split_once('\n').unwrap_or((candidate, ""));
        let line = line.trim_end();
        let degraded = line
            .strip_prefix("[degraded (")
            .and_then(|value| value.strip_suffix(")]"))
            .is_some_and(|value| !value.contains(['\r', '\n']));
        if line != UNKNOWN && !degraded {
            return content;
        }
        content = rest.trim_start_matches(['\r', '\n']);
    }
}

/// Reject empty source content before attachments or a label can conceal it.
pub(crate) fn validate_input(content: &str) -> Result<(), CliError> {
    if enabled()? && without_labels(content).trim().is_empty() {
        return Err(CliError::Usage(
            "route-labeled content must have a body".into(),
        ));
    }
    Ok(())
}

fn verified_prefix(fence: &PublicationFence) -> Option<String> {
    let file = open_regular(&fence.directory.join("route.json")).ok()?;
    let mut bytes = Vec::new();
    file.take(MAX_SNAPSHOT_BYTES + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return None;
    }
    let mut snapshot: Snapshot = serde_json::from_slice(&bytes).ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    let context = &fence.context;
    if snapshot.version != 1
        || snapshot.token != context.token
        || snapshot.relay.trim_end_matches('/') != context.relay.trim_end_matches('/')
        || snapshot.agent_pubkey != context.agent_pubkey
        || snapshot.channel_id != context.channel_id
        || snapshot.observed_at > now
        || snapshot.observed_at >= snapshot.expires_at
        || snapshot.expires_at <= now
        || snapshot.expires_at > context.expires_at
        || snapshot.fallbacks.len() > 16
        || snapshot.fallbacks.iter().any(|name| {
            name.is_empty()
                || name.len() > 128
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._:/-".contains(&byte))
        })
    {
        return None;
    }
    match snapshot.status.as_str() {
        "primary" if snapshot.fallbacks.is_empty() => Some(String::new()),
        "degraded" if !snapshot.fallbacks.is_empty() => {
            snapshot.fallbacks.sort();
            snapshot.fallbacks.dedup();
            Some(format!("[degraded ({})]", snapshot.fallbacks.join(", ")))
        }
        _ => None,
    }
}

/// Stamp while the caller holds the fence through final validation and signing.
pub(crate) fn stamp(content: &str, fence: Option<&PublicationFence>) -> Result<String, CliError> {
    if !enabled()? {
        return Ok(content.to_owned());
    }
    let fence = fence.ok_or_else(|| {
        CliError::Other("route labels require a valid publication context".into())
    })?;
    let body = without_labels(content);
    if body.trim().is_empty() {
        return Err(CliError::Usage(
            "route-labeled content must have a body".into(),
        ));
    }
    let prefix = verified_prefix(fence).unwrap_or_else(|| UNKNOWN.to_owned());
    Ok(if prefix.is_empty() {
        body.to_owned()
    } else {
        format!("{prefix}\n\n{body}")
    })
}
