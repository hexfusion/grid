//! [`AgentToolProvider`] custom resource definition.
//!
//! Represents MCP tool servers available over the grid.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    auth::{AccessPolicy, AuthConfig, SelectorConfig},
    inference_provider::{EndpointTlsConfig, ProviderPhase},
};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for an [`AgentToolProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1beta1",
    kind = "AgentToolProvider",
    plural = "agenttoolproviders",
    shortname = "atp",
    category = "grid",
    status = "AgentToolProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Protocol","type":"string","jsonPath":".spec.protocol"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Which sites can consume these tools.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Authentication configuration.
    pub auth: Option<AuthConfig>,

    /// HTTP endpoint of the MCP server.
    #[schemars(regex(pattern = r"^https?://\S+$"))]
    pub endpoint: String,

    /// Protocol the provider speaks.
    #[serde(default)]
    pub protocol: ToolProtocol,

    /// Which sites host this provider. Omitted matches no site; `{}` matches every site.
    pub host_selector: Option<SelectorConfig>,

    /// TLS configuration for the operator's own MCP `tools/list` probe.
    ///
    /// Reuses [`EndpointTlsConfig`] from [`InferenceProvider`] (CA trust and
    /// optional mTLS client identity for the probe connection). When absent,
    /// the probe uses native root certificates and no client certificate.
    ///
    /// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<EndpointTlsConfig>,

    /// Tool definitions (auto-discovered if omitted).
    #[serde(default)]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["name"]))]
    pub tools: Vec<ToolInfo>,
}

/// Metadata for a single MCP tool.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
pub struct ToolInfo {
    /// Tool name.
    pub name: String,

    /// Human-readable description.
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`AgentToolProvider`].
///
/// # Stable `Available` condition reasons
///
/// While the provider is healthy the `Available` reason is `Available` or `Pending`. Otherwise it is one
/// of the following stable, machine-readable strings — following the
/// same naming convention as [`InferenceProvider`]'s `MetricsTls*`/
/// `HealthCheckTls*` reasons:
///
/// | Reason | Meaning |
/// |--------|---------|
/// | `EndpointInvalid` | `spec.endpoint` is not an http(s) URL with a host. |
/// | `GridNetworkRefInvalid` | `spec.gridNetworkRef` is blank or whitespace-only. |
/// | `GridNetworkNotFound` | The `GridNetwork` referenced by `spec.gridNetworkRef` does not exist. |
/// | `McpEndpointUnreachable` | The MCP probe could not connect (transport failure, timeout, DNS error). |
/// | `McpToolsListInvalidResponse` | The endpoint responded but the `tools/list` response was malformed. |
/// | `McpAuthRejected` | The MCP server rejected the configured `spec.auth` credentials. |
/// | `McpAuthTokenInvalid` | The resolved `spec.auth` bearer token contains characters that cannot be sent as an HTTP header value; the probe fails closed rather than proceeding unauthenticated. |
/// | `EndpointTlsSecretMissing` | `spec.tls`'s referenced Secret does not exist in the cluster. |
/// | `EndpointTlsKeyMissing` | `spec.tls`'s referenced Secret exists but is missing the expected key. |
/// | `EndpointTlsMaterialInvalid` | `spec.tls`'s certificate or key material could not be parsed. |
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentToolProviderStatus {
    /// Observed conditions, `metav1.Condition` shaped and keyed by `type`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<super::condition::Condition>,

    /// Tools discovered via MCP `tools/list`.
    #[serde(default)]
    #[schemars(extend("x-kubernetes-list-type" = "set"))]
    pub discovered_tools: Vec<String>,

    /// Sites matched by the site selector.
    #[serde(default)]
    #[schemars(extend("x-kubernetes-list-type" = "set"))]
    pub matching_sites: Vec<String>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current phase.
    #[serde(default)]
    pub phase: ProviderPhase,
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

/// Protocol an [`AgentToolProvider`] speaks.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ToolProtocol {
    /// Model Context Protocol.
    #[default]
    Mcp,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn spec_serde() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "endpoint": "http://tools:8080",
            "tools": [{"name": "db-query", "description": "Query database"}]
        });
        let spec: AgentToolProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.protocol, ToolProtocol::Mcp, "default protocol");
        assert_eq!(spec.tools.len(), 1, "tool count");
    }

    #[test]
    fn an_unknown_protocol_is_refused() {
        let json =
            serde_json::json!({"gridNetworkRef": "production", "endpoint": "http://tools:8080", "protocol": "grpc"});
        assert!(
            serde_json::from_value::<AgentToolProviderSpec>(json).is_err(),
            "only mcp is accepted"
        );
    }

    // -----------------------------------------------------------------------
    // spec.tls — absent must default to None, present must round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn spec_tls_absent_defaults_to_none() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "endpoint": "http://tools:8080"
        });
        let spec: AgentToolProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(spec.tls.is_none(), "absent spec.tls must deserialize to None");
    }

    #[test]
    fn spec_tls_with_ca_only_round_trips() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "endpoint": "https://tools:8443",
            "tls": {
                "caSecretRef": { "name": "tools-ca", "namespace": "grid-system" }
            }
        });
        let spec: AgentToolProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let tls = spec.tls.unwrap_or_else(|| std::process::abort());
        assert_eq!(tls.ca_secret_ref.name, "tools-ca", "caSecretRef.name must round-trip");
        assert_eq!(
            tls.ca_secret_ref.namespace, "grid-system",
            "caSecretRef.namespace must round-trip"
        );
        assert!(
            tls.client_certificate_secret_ref.is_none(),
            "absent clientCertificateSecretRef must be None"
        );
    }

    #[test]
    fn spec_tls_with_client_cert_round_trips() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "endpoint": "https://tools:8443",
            "tls": {
                "caSecretRef": { "name": "tools-ca", "namespace": "grid-system" },
                "clientCertificateSecretRef": { "name": "tools-client-cert", "namespace": "grid-system" }
            }
        });
        let spec: AgentToolProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let tls = spec.tls.unwrap_or_else(|| std::process::abort());
        let client_ref = tls
            .client_certificate_secret_ref
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(client_ref.name, "tools-client-cert", "client cert name must round-trip");
        assert_eq!(
            client_ref.certificate_key, "tls.crt",
            "certificateKey must default to tls.crt"
        );
        assert_eq!(
            client_ref.private_key_key, "tls.key",
            "privateKeyKey must default to tls.key"
        );
    }
}
