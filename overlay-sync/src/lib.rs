//! Grid overlay-sync: copies operator-generated [`ConfigMap`] data to a local
//! file.
//!
//! This crate implements a synchronous polling loop that reads a specified
//! Kubernetes [`ConfigMap`] via `kubectl` and writes the extracted data to a
//! local file using atomic rename.  It is designed to run as a host service
//! alongside the Praxis AI edge gateway.
//!
//! # Security invariants
//!
//! - Output writes are confined to the configured output directory.
//! - Content hashes are logged, but full data is never emitted.
//! - Last-good output is preserved when the [`ConfigMap`] is unavailable.
//!
//! [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/

pub mod config;
pub mod fetch;
pub mod writer;

use std::time::Duration;

use sha2::{Digest as _, Sha256};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors returned by overlay-sync operations.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// Configuration file is invalid or missing.
    #[error("config: {0}")]
    Config(String),

    /// [`ConfigMap`](https://kubernetes.io/docs/concepts/configuration/configmap/)
    /// fetch via `kubectl` failed.
    #[error("fetch: {0}")]
    Fetch(String),

    /// File write or path validation failed.
    #[error("write: {0}")]
    Write(String),

    /// I/O error from the filesystem or subprocess.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// Sync loop
// ---------------------------------------------------------------------------

/// Run the sync poll loop until the process is terminated.
///
/// Polls the configured [`ConfigMap`] at the specified interval, writing
/// updated content to the output file when the content hash changes.
/// On fetch failure the last-good output is preserved.
///
/// # Errors
///
/// Returns `Err` only for fatal configuration errors (unparseable interval
/// or invalid output path).  Transient fetch/write errors are logged and
/// retried on the next poll cycle.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
#[expect(clippy::infinite_loop, reason = "daemon polls until process is terminated")]
pub fn run_sync_loop(cfg: &config::SyncConfig) -> Result<(), SyncError> {
    let interval = config::parse_interval(&cfg.watch.interval)?;
    writer::validate_output_path(&cfg.output.path)?;
    let mut last_hash: Option<String> = None;
    tracing::info!(
        config_map = cfg.source.config_map.as_str(),
        namespace = cfg.source.namespace.as_str(),
        output = cfg.output.path.as_str(),
        "starting sync loop",
    );
    loop {
        match poll_once(cfg, &last_hash) {
            Ok(hash) => last_hash = Some(hash),
            Err(e) => tracing::warn!(error = %e, "poll failed, keeping last-good output"),
        }
        sleep_interval(interval);
    }
}

/// Execute a single poll cycle: fetch, hash, conditionally write.
fn poll_once(cfg: &config::SyncConfig, last_hash: &Option<String>) -> Result<String, SyncError> {
    let raw = fetch::fetch_config_map_key(
        &cfg.source.context,
        &cfg.source.namespace,
        &cfg.source.config_map,
        &cfg.source.key,
    )?;
    let hash = content_hash(&raw);
    if last_hash.as_ref() == Some(&hash) {
        return Ok(hash);
    }
    let output = config::format_output(&raw, &cfg.output.format)?;
    writer::write_atomic(&cfg.output.path, &output)?;
    tracing::info!(hash = hash.as_str(), "wrote updated config");
    Ok(hash)
}

/// Compute a hex-encoded SHA-256 hash of the content.
fn content_hash(data: &str) -> String {
    let digest = Sha256::digest(data.as_bytes());
    hex_encode(&digest)
}

/// Encode bytes as a lowercase hex string.
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Sleep for the given duration between poll cycles.
#[expect(
    clippy::disallowed_methods,
    reason = "overlay-sync is synchronous; no async runtime available"
)]
fn sleep_interval(dur: Duration) {
    std::thread::sleep(dur);
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Same content produces the same hash.
    #[test]
    fn content_hash_deterministic() {
        let h1 = content_hash("hello world");
        let h2 = content_hash("hello world");
        assert_eq!(h1, h2, "same content should produce same hash");
    }

    /// Different content produces different hashes.
    #[test]
    fn content_hash_differs_for_different_input() {
        let h1 = content_hash("hello");
        let h2 = content_hash("world");
        assert_ne!(h1, h2, "different content should produce different hashes");
    }

    /// SHA-256 produces a 64-character hex string.
    #[test]
    fn content_hash_length() {
        let h = content_hash("test");
        assert_eq!(h.len(), 64, "SHA-256 hash should be 64 hex characters");
    }

    /// Known SHA-256 test vector.
    #[test]
    fn content_hash_known_vector() {
        let h = content_hash("abc");
        assert_eq!(
            h, "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "SHA-256 of 'abc' should match known vector",
        );
    }
}
