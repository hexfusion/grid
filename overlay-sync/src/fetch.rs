//! [`ConfigMap`] fetching via `kubectl` subprocess.
//!
//! [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/

use crate::SyncError;

/// Fetch a single data key from a Kubernetes [`ConfigMap`].
///
/// Runs `kubectl get configmap` with the given context, namespace, and
/// name, then extracts the specified key from the `.data` object.
///
/// # Errors
///
/// Returns [`SyncError::Io`] if `kubectl` cannot be executed.
/// Returns [`SyncError::Fetch`] if `kubectl` exits non-zero, the
/// output is not valid JSON, or the requested key is absent.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
pub fn fetch_config_map_key(context: &str, namespace: &str, name: &str, key: &str) -> Result<String, SyncError> {
    let json = run_kubectl(context, namespace, name)?;
    extract_key(&json, key)
}

/// Run `kubectl get configmap` and return the raw JSON output.
fn run_kubectl(context: &str, namespace: &str, name: &str) -> Result<String, SyncError> {
    let output = std::process::Command::new("kubectl")
        .args(["get", "configmap", name])
        .args(["-n", namespace])
        .args(["--context", context])
        .args(["-o", "json"])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(SyncError::Fetch(format!("kubectl failed: {stderr}")));
    }
    String::from_utf8(output.stdout).map_err(|e| SyncError::Fetch(format!("kubectl output not UTF-8: {e}")))
}

/// Extract a data key from a [`ConfigMap`] JSON representation.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
fn extract_key(json: &str, key: &str) -> Result<String, SyncError> {
    let parsed: serde_json::Value =
        serde_json::from_str(json).map_err(|e| SyncError::Fetch(format!("invalid kubectl JSON: {e}")))?;
    parsed
        .get("data")
        .and_then(|d| d.get(key))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| SyncError::Fetch(format!("ConfigMap key '{key}' not found in .data")))
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Valid JSON with the expected key returns the value.
    #[test]
    fn extract_key_from_valid_json() -> Result<(), SyncError> {
        let json = r#"{"data": {"grid-config.json": "content here"}}"#;
        let result = extract_key(json, "grid-config.json")?;
        assert_eq!(result, "content here", "extracted value should match");
        Ok(())
    }

    /// Missing key returns an error.
    #[test]
    fn extract_key_missing_key() {
        let json = r#"{"data": {"other": "value"}}"#;
        assert!(
            extract_key(json, "grid-config.json").is_err(),
            "missing key should fail",
        );
    }

    /// Missing `.data` field returns an error.
    #[test]
    fn extract_key_missing_data_field() {
        let json = r#"{"metadata": {}}"#;
        assert!(
            extract_key(json, "grid-config.json").is_err(),
            "missing .data should fail",
        );
    }

    /// Malformed JSON returns an error.
    #[test]
    fn extract_key_invalid_json() {
        assert!(extract_key("not json", "key").is_err(), "invalid JSON should fail",);
    }

    /// Non-string value returns an error (`.data` values must be strings).
    #[test]
    fn extract_key_non_string_value() {
        let json = r#"{"data": {"key": 42}}"#;
        assert!(extract_key(json, "key").is_err(), "non-string value should fail",);
    }
}
