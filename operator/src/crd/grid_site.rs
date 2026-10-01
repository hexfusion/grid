//! [`GridSite`] custom resource definition.
//!
//! Represents a remote site in the grid. Created manually for
//! seed peers or automatically by SWIM discovery. The status
//! tracks the site lifecycle from discovery through mTLS
//! establishment to active connectivity.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::grid_network::TlsMode;

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for a [`GridSite`].
///
/// Describes a remote site's egress endpoint, region, and
/// grid membership.
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1beta1",
    kind = "GridSite",
    plural = "gridsites",
    shortname = "gs",
    category = "grid",
    status = "GridSiteStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#,
    printcolumn = r#"{"name":"Network","type":"string","jsonPath":".spec.gridNetworkRef"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteSpec {
    /// Name of the [`GridNetwork`] this site belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Egress endpoint for data-plane connectivity.
    pub egress: Option<EgressConfig>,

    /// Deployment region.
    pub region: Option<String>,

    /// Sovereignty zone for data residency constraints.
    pub sovereignty_zone: Option<String>,

    /// Availability zone.
    pub zone: Option<String>,

    /// Trust policy for this site.
    ///
    /// When configured, the operator verifies the received public certificate against
    /// this policy before promoting the site to `Active`. If absent, the site remains
    /// `Connecting` with reason `TrustMaterialMissing` regardless of certificate
    /// material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trust: Option<GridSiteTrustPolicy>,
}

/// Trust policy controlling when a [`GridSite`] can advance to `Active`.
///
/// # Security
///
/// Fingerprint values must be verified out-of-band before configuration.
/// The operator performs X.509 chain, validity, and identity verification
/// via the TLS handshake; the fingerprint provides additional pin-based
/// binding to a specific leaf certificate.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteTrustPolicy {
    /// Canonical DER-certificate SHA-256 fingerprint pins.
    ///
    /// Each entry is a 64-character lowercase hex string computed as
    /// `hex(sha256(der_bytes))` where `der_bytes` are the raw DER encoding
    /// of the leaf certificate.
    ///
    /// At most two entries are allowed: the current pin and an optional
    /// next pin for bounded rotation overlap.  The probe succeeds if the
    /// peer leaf certificate matches **any** entry in this list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 2), inner(regex(pattern = "^[0-9a-f]{64}$")))]
    pub canonical_fingerprints: Option<Vec<String>>,
}

/// Egress endpoint configuration for a site.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressConfig {
    /// Egress gateway `host:port`, overriding discovery; empty uses `status.discovered.egressAddress`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub address: String,

    /// TLS mode for the connection.
    #[serde(default)]
    pub tls: EgressTls,
}

/// TLS configuration for site egress.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressTls {
    /// TLS transport mode, `mutualTls` when absent.
    #[serde(default = "default_egress_mode")]
    pub mode: TlsMode,

    /// Expected DNS identity for TLS verification.
    ///
    /// Used as both the TLS SNI value and for certificate SAN
    /// verification.  Defaults to `<site>.grid.internal`, the DNS name
    /// enrollment issues, for [`TlsMode::MutualTls`]; must be absent for
    /// [`TlsMode::Plaintext`].
    ///
    /// Must be a valid DNS name (not an IP address), at most 253
    /// characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1, max = 253))]
    pub server_name: Option<String>,
}

impl Default for EgressTls {
    fn default() -> Self {
        Self {
            mode: default_egress_mode(),
            server_name: None,
        }
    }
}

/// Egress defaults to mutual TLS; a plaintext egress cannot become `Active`.
const fn default_egress_mode() -> TlsMode {
    TlsMode::MutualTls
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of a [`GridSite`].
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GridSiteStatus {
    /// Observed conditions, `metav1.Condition` shaped and keyed by `type`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<super::condition::Condition>,

    /// Capabilities offered by this site.
    #[serde(default)]
    pub capabilities: SiteCapabilities,

    /// Timestamp of the last probe whose resulting status was persisted.
    ///
    /// This is not a liveness heartbeat; it may remain unchanged when a probe
    /// produces no status changes.
    pub last_probe_time: Option<String>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current lifecycle phase.
    #[serde(default)]
    pub phase: GridSitePhase,

    /// What SWIM gossip reports about this site; observed, never authored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered: Option<DiscoveredStatus>,
}

/// Site state learned from SWIM gossip.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredStatus {
    /// Gateway address the remote operator advertised.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_address: Option<String>,

    /// When gossip lost this member (absent or `Dead`), RFC 3339; cleared when it returns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absent_since: Option<String>,
}

/// Capabilities a site advertises over the grid.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[expect(clippy::struct_excessive_bools, reason = "capability flags are boolean by nature")]
#[serde(rename_all = "camelCase")]
pub struct SiteCapabilities {
    /// Site offers A2A agent access.
    #[serde(default)]
    pub agent_to_agent: bool,

    /// Site offers MCP tool access.
    #[serde(default)]
    pub agent_tools: bool,

    /// Site offers inference access.
    #[serde(default)]
    pub inference: bool,
}

impl SiteCapabilities {
    /// Returns true if the site offers any capability.
    pub fn has_any(&self) -> bool {
        self.agent_to_agent || self.agent_tools || self.inference
    }
}

/// Lifecycle phase of a [`GridSite`].
///
/// ```text
/// Pending → Discovered → Connecting → Active
///                                       ↓
///                                  Unreachable → Left
/// ```
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum GridSitePhase {
    /// Site record created but not yet seen via SWIM.
    #[default]
    Pending,

    /// SWIM has discovered this site.
    Discovered,

    /// Gateway address known; trust and data-plane readiness being established.
    Connecting,

    /// Fully connected according to the deployment workflow.
    Active,

    /// Previously active but SWIM probes failing.
    Unreachable,

    /// Site has left the grid (graceful or timeout).
    Left,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt as _;

    use super::*;

    fn crd_json() -> serde_json::Value {
        serde_json::to_value(GridSite::crd()).unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn default_site_phase() {
        let phase = GridSitePhase::default();
        assert_eq!(phase, GridSitePhase::Pending, "should default to Pending");
    }

    #[test]
    fn capabilities_has_any() {
        let empty = SiteCapabilities::default();
        assert!(!empty.has_any(), "empty capabilities");

        let with_inference = SiteCapabilities {
            inference: true,
            ..Default::default()
        };
        assert!(with_inference.has_any(), "inference capability");
    }

    #[test]
    fn spec_serde_round_trip() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "egress": {
                "address": "egress.cluster-b:8443",
                "tls": {"mode": "mutualTls"}
            },
            "region": "us-east-1"
        });
        let spec: GridSiteSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.grid_network_ref, "production", "network ref");
        assert_eq!(spec.region.as_deref(), Some("us-east-1"), "region");
    }

    #[test]
    fn status_defaults() {
        let status = GridSiteStatus::default();
        assert_eq!(status.phase, GridSitePhase::Pending, "default phase");
        assert!(!status.capabilities.has_any(), "no default capabilities");
    }

    #[test]
    fn grid_site_crd_has_correct_group_and_plural() {
        let crd = crd_json();
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("group"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "grid.praxis.fast",
            "wrong CRD group"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("plural"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "gridsites",
            "wrong plural name"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("kind"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "GridSite",
            "wrong kind name"
        );
    }

    #[test]
    fn grid_site_crd_has_grid_network_ref() {
        let crd = crd_json();
        let spec_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec_properties.contains_key("gridNetworkRef"),
            "CRD schema must include gridNetworkRef field"
        );
    }

    #[test]
    fn egress_tls_mode_defaults_to_mutual_tls() {
        let tls: EgressTls = serde_json::from_value(serde_json::json!({})).unwrap_or_else(|_| std::process::abort());
        assert_eq!(tls.mode, TlsMode::MutualTls, "an absent egress mode is mutual TLS");
        assert_eq!(
            EgressTls::default().mode,
            TlsMode::MutualTls,
            "the struct default matches"
        );
    }

    #[test]
    fn tls_mode_is_camel_case_and_refuses_v1alpha1_values() {
        for (json, mode) in [("mutualTls", TlsMode::MutualTls), ("plaintext", TlsMode::Plaintext)] {
            let parsed: TlsMode =
                serde_json::from_value(serde_json::json!(json)).unwrap_or_else(|_| std::process::abort());
            assert_eq!(parsed, mode, "{json} parses");
        }
        for old in ["Mutual", "Plaintext", "mutual_tls"] {
            assert!(
                serde_json::from_value::<TlsMode>(serde_json::json!(old)).is_err(),
                "{old} is refused"
            );
        }
    }

    #[test]
    fn unknown_tls_mode_rejected() {
        let unknown = serde_json::json!("Passthrough");
        let result: Result<TlsMode, _> = serde_json::from_value(unknown);
        assert!(result.is_err(), "unknown TLS mode must fail closed");
    }

    #[test]
    fn egress_tls_with_server_name() {
        let json = serde_json::json!({
            "mode": "mutualTls",
            "serverName": "east-provider.grid.internal"
        });
        let tls: EgressTls = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(tls.mode, TlsMode::MutualTls, "mode");
        assert_eq!(
            tls.server_name.as_deref(),
            Some("east-provider.grid.internal"),
            "serverName"
        );
    }

    #[test]
    fn egress_tls_without_server_name() {
        let json = serde_json::json!({"mode": "plaintext"});
        let tls: EgressTls = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(tls.mode, TlsMode::Plaintext, "mode");
        assert!(tls.server_name.is_none(), "serverName must be absent for Plaintext");
    }

    #[test]
    fn trust_policy_with_canonical_fingerprints() {
        let json = serde_json::json!({
            "canonicalFingerprints": [
                "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
                "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
            ]
        });
        let policy: GridSiteTrustPolicy = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let pins = policy.canonical_fingerprints.unwrap_or_else(|| std::process::abort());
        assert_eq!(pins.len(), 2, "must have 2 canonical pins");
    }

    #[test]
    fn grid_site_crd_bounds_identity_fields() {
        let crd = crd_json();
        let pins = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/trust/properties/canonicalFingerprints",
            )
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(pins.get("minItems").and_then(serde_json::Value::as_u64), Some(1));
        assert_eq!(pins.get("maxItems").and_then(serde_json::Value::as_u64), Some(2));
        assert_eq!(
            pins.pointer("/items/pattern").and_then(serde_json::Value::as_str),
            Some("^[0-9a-f]{64}$")
        );

        let server_name = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/egress/properties/tls/properties/serverName",
            )
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            server_name.get("minLength").and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            server_name.get("maxLength").and_then(serde_json::Value::as_u64),
            Some(253)
        );
    }
}
