//! Sync configuration model, validation, and format conversion.

use std::time::Duration;

use serde::Deserialize;

use crate::SyncError;

// ---------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------

/// Top-level overlay-sync configuration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SyncConfig {
    /// Kubernetes [`ConfigMap`] source location.
    ///
    /// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
    pub source: SourceConfig,
    /// Output file settings.
    pub output: OutputConfig,
    /// Polling configuration.
    pub watch: WatchConfig,
}

/// [`ConfigMap`] source location within a Kubernetes cluster.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceConfig {
    /// Kubernetes context name (e.g. `kind-grid-glb-edge-control`).
    pub context: String,
    /// Kubernetes namespace containing the [`ConfigMap`].
    ///
    /// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
    pub namespace: String,
    /// [`ConfigMap`] resource name.
    ///
    /// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
    pub config_map: String,
    /// Data key within the [`ConfigMap`] (default: `grid-config.json`).
    ///
    /// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
    #[serde(default = "default_key")]
    pub key: String,
}

/// Default [`ConfigMap`] data key matching the operator convention.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
fn default_key() -> String {
    "grid-config.json".to_owned()
}

/// Output file configuration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct OutputConfig {
    /// Absolute path to the output file.
    pub path: String,
    /// Output serialization format.
    pub format: OutputFormat,
}

/// Supported output serialization formats.
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Convert YAML [`ConfigMap`] data to JSON.
    ///
    /// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
    Json,
    /// Write [`ConfigMap`] data verbatim (no conversion).
    ///
    /// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
    Raw,
}

/// Polling interval configuration.
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WatchConfig {
    /// Poll interval as a duration string (e.g. `"5s"`, `"500ms"`).
    pub interval: String,
}

// ---------------------------------------------------------------------------
// Loading and validation
// ---------------------------------------------------------------------------

/// Load and parse a sync configuration file.
///
/// # Errors
///
/// Returns [`SyncError::Io`] if the file is not accessible.
/// Returns [`SyncError::Config`] if the YAML content is malformed.
pub fn load(path: &str) -> Result<SyncConfig, SyncError> {
    let contents = std::fs::read_to_string(path)?;
    serde_yaml::from_str(&contents).map_err(|e| SyncError::Config(format!("failed to parse {path}: {e}")))
}

/// Validate a parsed configuration for correctness.
///
/// Checks that required string fields are non-blank and the poll interval is
/// parseable. Output path validation is performed separately by
/// [`crate::writer::validate_output_path`] because it depends on the runtime
/// output mount.
///
/// # Errors
///
/// Returns [`SyncError::Config`] if any required field is blank.
pub fn validate(cfg: &SyncConfig) -> Result<(), SyncError> {
    check_not_blank(&cfg.source.context, "source.context")?;
    check_not_blank(&cfg.source.namespace, "source.namespace")?;
    check_not_blank(&cfg.source.config_map, "source.configMap")?;
    check_not_blank(&cfg.source.key, "source.key")?;
    check_not_blank(&cfg.output.path, "output.path")?;
    parse_interval(&cfg.watch.interval)?;
    Ok(())
}

/// Reject blank (empty or whitespace-only) string values.
fn check_not_blank(value: &str, field: &str) -> Result<(), SyncError> {
    if value.trim().is_empty() {
        return Err(SyncError::Config(format!("{field} must not be blank")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Interval parsing
// ---------------------------------------------------------------------------

/// Parse a human-readable duration string into a [`Duration`].
///
/// Supported formats: `"5s"` (seconds), `"500ms"` (milliseconds).
///
/// # Errors
///
/// Returns [`SyncError::Config`] if the format is unrecognized or the
/// numeric portion is not a valid integer.
pub fn parse_interval(raw: &str) -> Result<Duration, SyncError> {
    let trimmed = raw.trim();
    if let Some(ms_str) = trimmed.strip_suffix("ms") {
        let ms = parse_u64(ms_str, "ms")?;
        check_non_zero(ms, "ms")?;
        return Ok(Duration::from_millis(ms));
    }
    if let Some(s_str) = trimmed.strip_suffix('s') {
        let secs = parse_u64(s_str, "s")?;
        check_non_zero(secs, "s")?;
        return Ok(Duration::from_secs(secs));
    }
    Err(SyncError::Config(format!("unsupported interval format: {trimmed}")))
}

/// Parse a string as `u64` with a descriptive error on failure.
fn parse_u64(s: &str, unit: &str) -> Result<u64, SyncError> {
    s.parse::<u64>()
        .map_err(|e| SyncError::Config(format!("invalid {unit} value '{s}': {e}")))
}

/// Reject zero durations; they would create a tight polling loop.
fn check_non_zero(value: u64, unit: &str) -> Result<(), SyncError> {
    if value == 0 {
        return Err(SyncError::Config(format!("{unit} value must be greater than zero")));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Format conversion
// ---------------------------------------------------------------------------

/// Convert raw [`ConfigMap`] data to the configured output format.
///
/// # Errors
///
/// Returns [`SyncError::Config`] if YAML-to-JSON conversion fails on
/// malformed input.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
pub fn format_output(raw: &str, format: &OutputFormat) -> Result<String, SyncError> {
    match format {
        OutputFormat::Json => yaml_to_json(raw),
        OutputFormat::Raw => Ok(raw.to_owned()),
    }
}

/// Convert a YAML string to pretty-printed JSON.
fn yaml_to_json(yaml: &str) -> Result<String, SyncError> {
    let value: serde_json::Value =
        serde_yaml::from_str(yaml).map_err(|e| SyncError::Config(format!("invalid YAML data: {e}")))?;
    serde_json::to_string_pretty(&value).map_err(|e| SyncError::Config(format!("JSON serialization failed: {e}")))
}

// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Config without explicit `key` uses the default `grid-config.json`.
    #[test]
    fn valid_config_with_defaults() -> Result<(), Box<dyn std::error::Error>> {
        let yaml = r#"
source:
  context: kind-test
  namespace: test-ns
  configMap: test-cm
output:
  path: /output/test.json
  format: json
watch:
  interval: "5s"
"#;
        let cfg: SyncConfig = serde_yaml::from_str(yaml)?;
        assert_eq!(
            cfg.source.key, "grid-config.json",
            "default key should be grid-config.json",
        );
        Ok(())
    }

    /// Explicit `key` overrides the default.
    #[test]
    fn valid_config_with_explicit_key() -> Result<(), Box<dyn std::error::Error>> {
        let yaml = r#"
source:
  context: kind-test
  namespace: test-ns
  configMap: test-cm
  key: custom.yaml
output:
  path: /output/test.json
  format: json
watch:
  interval: "5s"
"#;
        let cfg: SyncConfig = serde_yaml::from_str(yaml)?;
        assert_eq!(cfg.source.key, "custom.yaml", "explicit key should be preserved",);
        Ok(())
    }

    /// Unknown fields are rejected by `deny_unknown_fields`.
    #[test]
    fn config_rejects_unknown_field() {
        let yaml = r#"
source:
  context: kind-test
  namespace: test-ns
  configMap: test-cm
  extra: unexpected
output:
  path: /output/test.json
  format: json
watch:
  interval: "5s"
"#;
        let result = serde_yaml::from_str::<SyncConfig>(yaml);
        assert!(result.is_err(), "unknown field should be rejected");
    }

    /// Blank `source.context` is rejected.
    #[test]
    fn validate_rejects_blank_context() {
        let cfg = test_config(" ", "ns", "cm");
        assert!(validate(&cfg).is_err(), "blank context should be rejected",);
    }

    /// Blank `source.namespace` is rejected.
    #[test]
    fn validate_rejects_blank_namespace() {
        let cfg = test_config("ctx", "", "cm");
        assert!(validate(&cfg).is_err(), "blank namespace should be rejected",);
    }

    /// Blank `source.configMap` is rejected.
    #[test]
    fn validate_rejects_blank_config_map() {
        let cfg = test_config("ctx", "ns", "");
        assert!(validate(&cfg).is_err(), "blank configMap should be rejected",);
    }

    /// `"5s"` parses as 5 seconds.
    #[test]
    fn parse_interval_seconds() -> Result<(), SyncError> {
        let dur = parse_interval("5s")?;
        assert_eq!(dur, Duration::from_secs(5), "5s should be 5 seconds");
        Ok(())
    }

    /// `"500ms"` parses as 500 milliseconds.
    #[test]
    fn parse_interval_milliseconds() -> Result<(), SyncError> {
        let dur = parse_interval("500ms")?;
        assert_eq!(dur, Duration::from_millis(500), "500ms should be 500 milliseconds",);
        Ok(())
    }

    /// An unrecognized unit suffix is rejected.
    #[test]
    fn parse_interval_invalid_unit() {
        assert!(parse_interval("5x").is_err(), "invalid unit should fail");
    }

    /// Zero-second intervals are rejected to prevent tight polling loops.
    #[test]
    fn parse_interval_rejects_zero_seconds() {
        assert!(parse_interval("0s").is_err(), "zero seconds should fail");
    }

    /// Zero-millisecond intervals are rejected to prevent tight polling loops.
    #[test]
    fn parse_interval_rejects_zero_milliseconds() {
        assert!(parse_interval("0ms").is_err(), "zero milliseconds should fail");
    }

    /// JSON format converts YAML to equivalent JSON.
    #[test]
    fn format_output_json_converts_yaml() -> Result<(), SyncError> {
        let yaml = "key: value\nnested:\n  a: 1\n";
        let json = format_output(yaml, &OutputFormat::Json)?;
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            parsed.get("key").and_then(serde_json::Value::as_str),
            Some("value"),
            "top-level key should match",
        );
        Ok(())
    }

    /// Raw format passes content through unchanged.
    #[test]
    fn format_output_raw_preserves_content() -> Result<(), SyncError> {
        let raw = "arbitrary content\n";
        let result = format_output(raw, &OutputFormat::Raw)?;
        assert_eq!(result, raw, "raw format should preserve content");
        Ok(())
    }

    // Test Utilities

    /// Build a [`SyncConfig`] with the given source fields and sensible
    /// defaults for everything else.
    fn test_config(context: &str, namespace: &str, config_map: &str) -> SyncConfig {
        SyncConfig {
            source: SourceConfig {
                context: context.to_owned(),
                namespace: namespace.to_owned(),
                config_map: config_map.to_owned(),
                key: "grid-config.json".to_owned(),
            },
            output: OutputConfig {
                path: "/output/test.json".to_owned(),
                format: OutputFormat::Json,
            },
            watch: WatchConfig {
                interval: "5s".to_owned(),
            },
        }
    }
}
