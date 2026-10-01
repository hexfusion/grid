//! [`InferenceProvider`] custom resource definition.
//!
//! Represents an inference backend available over the grid.
//! Three backend categories: self-hosted clusters (llm-d),
//! cloud-managed services (Bedrock, Vertex), and third-party
//! APIs (OpenAI, Anthropic).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    auth::{AccessPolicy, AuthConfig},
    grid_network::SecretRef,
};

// ---------------------------------------------------------------------------
// Spec
// ---------------------------------------------------------------------------

/// Specification for an [`InferenceProvider`].
#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "grid.praxis.fast",
    version = "v1beta1",
    kind = "InferenceProvider",
    plural = "inferenceproviders",
    shortname = "infpvd",
    category = "grid",
    status = "InferenceProviderStatus",
    namespaced = false,
    printcolumn = r#"{"name":"Provider","type":"string","jsonPath":".spec.providerKind"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderSpec {
    /// Name of the [`GridNetwork`] this provider belongs to.
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub grid_network_ref: String,

    /// Which sites can consume this provider.
    #[serde(default)]
    pub access_policy: AccessPolicy,

    /// Authentication configuration.
    pub auth: Option<AuthConfig>,

    /// Where the provider runs, which drives locality scoring.
    pub backend_kind: BackendKind,

    /// Stable provider-gateway identity used by administrative operations.
    ///
    /// Providers with the same value are drained together by the gateway-wide
    /// operation. It is an explicit control-plane relationship; the operator
    /// never infers it from endpoint URLs.
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_ref: Option<String>,

    /// Relative capacity used by an opt-in placement policy.
    #[schemars(range(min = 1, max = 1000))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity_weight: Option<u32>,

    /// Cost information.
    pub cost: Option<CostConfig>,

    /// HTTP endpoint URL, or a bare `host:port`.
    #[schemars(regex(pattern = r"^(https?://\S+|[^\s/@]+:[0-9]{1,5})$"))]
    pub endpoint: String,

    /// Health check configuration.
    pub health_check: Option<HealthCheckConfig>,

    /// Models served by this provider.
    #[serde(default)]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["name"]))]
    pub models: Vec<ModelInfo>,

    /// Where to discover the models this provider serves.
    ///
    /// When set, the operator polls the backend on a fixed cadence and holds
    /// the served-model set in memory, expiring it when polls stop succeeding.
    /// Discovery does not yet affect routing or gossip; `spec.models` remains
    /// the routing source.
    /// When absent, no discovery runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_discovery: Option<ModelDiscoveryConfig>,

    /// The wire API the provider speaks. Self-hosted vLLM and llm-d speak `openAi`.
    pub provider_kind: ProviderKind,

    /// Optional routing identity used in overlay candidate `site` and `cluster` fields.
    ///
    /// When set, routing overlay candidates produced for this provider use this value
    /// instead of `metadata.name`:
    ///
    /// - In Phase 1 (no [`GridSite`] inventory), both `candidate.site` and `candidate.cluster` are set to this value.
    /// - When [`GridSite`] resources are present, only `candidate.cluster` is overridden; `candidate.site` is derived
    ///   from the matched `GridSite`.
    ///
    /// Use this to align the provider's routing identity with an upstream cluster
    /// name already configured in the consumer gateway, such as a Praxis
    /// `load_balancer` cluster entry.  When absent, `metadata.name` is used.
    ///
    /// [`GridSite`]: crate::crd::grid_site::GridSite
    pub routing_cluster_ref: Option<String>,

    /// Which sites host this provider. Omitted matches no site; `{}` matches every site.
    pub host_selector: Option<super::auth::SelectorConfig>,

    /// Prometheus metrics scraping configuration.
    ///
    /// When set, the Grid operator scrapes the provider's metrics endpoint during
    /// each [`GridNetwork`] reconcile and incorporates the parsed signals into the
    /// routing overlay scoring pass.  When absent, the provider uses locality and
    /// cost as the only scoring signals (static ordering).
    ///
    /// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
    pub metrics_config: Option<MetricsConfig>,

    /// Optional administrative traffic policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic_policy: Option<TrafficPolicy>,
}

/// The wire API an [`InferenceProvider`] speaks.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ProviderKind {
    /// `OpenAI` chat completions, also spoken by vLLM and llm-d.
    OpenAi,
    /// Anthropic Messages API.
    Anthropic,
    /// AWS Bedrock Converse API.
    Bedrock,
    /// Google Vertex AI `generateContent` API.
    Vertex,
}

impl ProviderKind {
    /// The scoring engine's equivalent.
    #[must_use]
    pub const fn to_scoring(self) -> scoring::ProviderKind {
        match self {
            Self::OpenAi => scoring::ProviderKind::OpenAi,
            Self::Anthropic => scoring::ProviderKind::Anthropic,
            Self::Bedrock => scoring::ProviderKind::Bedrock,
            Self::Vertex => scoring::ProviderKind::Vertex,
        }
    }
}

/// Where an [`InferenceProvider`] runs.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BackendKind {
    /// A model server on this cluster.
    Local,
    /// A model server on another grid cluster.
    Remote,
    /// A cloud-managed inference service such as Bedrock or Vertex.
    CloudManaged,
    /// A third-party API provider such as `OpenAI` or Anthropic.
    ApiProvider,
}

impl BackendKind {
    /// The scoring engine's equivalent.
    #[must_use]
    pub const fn to_scoring(self) -> scoring::BackendKind {
        match self {
            Self::Local => scoring::BackendKind::Local,
            Self::Remote => scoring::BackendKind::Remote,
            Self::CloudManaged => scoring::BackendKind::CloudManaged,
            Self::ApiProvider => scoring::BackendKind::ApiProvider,
        }
    }

    /// The name carried in gossip, unchanged since v1alpha1 so mixed-version sites agree.
    #[must_use]
    pub const fn wire_name(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
            Self::CloudManaged => "cloud_managed",
            Self::ApiProvider => "api_provider",
        }
    }

    /// Parse a gossiped backend kind, `None` for an unknown name.
    #[must_use]
    pub fn from_wire_name(name: &str) -> Option<scoring::BackendKind> {
        match name {
            "local" => Some(scoring::BackendKind::Local),
            "remote" => Some(scoring::BackendKind::Remote),
            "cloud_managed" => Some(scoring::BackendKind::CloudManaged),
            "api_provider" => Some(scoring::BackendKind::ApiProvider),
            _ => None,
        }
    }
}

/// Administrative policy for provider traffic.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TrafficPolicy {
    /// Stop new sessions while preserving existing affinity sessions.
    #[serde(default)]
    pub drain: bool,
}

/// Prometheus metrics scraping configuration for an `InferenceProvider`.
///
/// The operator scrapes `{spec.endpoint}{path}` (or `{metrics_endpoint}{path}`
/// when set) and parses the Prometheus text using the `signal_names` mapping.
/// Signals without a configured name receive the neutral default (`0.5`) in
/// scoring. Plaintext scrape failures retain neutral-scoring compatibility
/// behavior. When TLS is configured, a failed scrape uses a successful cached
/// sample only within `stale_metrics_seconds`; after that the provider is
/// marked unhealthy and excluded from routing.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsConfig {
    /// Base URL for the metrics endpoint, independent of `spec.endpoint`.
    ///
    /// When set, the scrape URL is `{metrics_endpoint}{path}` instead of
    /// `{spec.endpoint}{path}`.  This allows scraping metrics from a separate
    /// service (such as an llm-d EPP) while the provider inference endpoint
    /// points at the pool's request path.
    ///
    /// When absent, the scrape URL uses `spec.endpoint` as before.
    #[schemars(length(min = 1), regex(pattern = r"^https?://\S+$"))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_endpoint: Option<String>,

    /// HTTP path for the Prometheus metrics endpoint.
    ///
    /// Appended to `metrics_endpoint` (when set) or `spec.endpoint`.
    /// Defaults to `"/metrics"` when absent.
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// Per-request scrape timeout (e.g. `"2s"`, `"500ms"`).
    ///
    /// Defaults to `"2s"`.  Only `s` and `ms` suffixes are recognised; unrecognised
    /// values fall back to the default.
    #[serde(default = "default_metrics_timeout")]
    pub timeout: String,

    /// Exporter whose well-known metric names fill any signal `signalNames` leaves unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset: Option<MetricsPreset>,

    /// Mapping from scoring signal names to Prometheus metric names; each set name overrides the preset.
    ///
    /// Signals with no name from either source score neutrally (`0.5`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_names: Option<MetricSignalNames>,

    /// Expected Prometheus `name` label value for pool-level metric selection.
    ///
    /// When set, the parser only matches metric samples whose `name` label
    /// equals this value.  This is required when scraping an endpoint that
    /// exposes metrics for multiple pools (such as an llm-d EPP), ensuring
    /// the configured pool is selected deterministically.
    ///
    /// The parser rejects the scrape when the expected pool series is absent
    /// or when a metric sample has no `name` label and this field is set.
    ///
    /// When absent, labels are stripped before matching as in v1 (backward
    /// compatible).
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_name: Option<String>,

    /// Maximum queue slot count for normalising a raw queue-size metric to
    /// 0.0–1.0.
    ///
    /// When set, the `queue_depth` signal value is divided by this capacity
    /// and clamped to `[0.0, 1.0]` before scoring.  This allows consuming raw
    /// average queue-size metrics (such as `llm_d_epp_average_queue_size`)
    /// without requiring the exporter to pre-normalise.
    ///
    /// When absent, the `queue_depth` signal must already be normalised to
    /// 0.0–1.0 by the exporter (backward compatible).
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_capacity: Option<u32>,

    /// Maximum age in seconds for which a previously-scraped metric sample may be
    /// used when the current scrape fails.
    ///
    /// When a Prometheus scrape fails (connection refused, timeout, HTTP error),
    /// setting this field enables a grace period: if the last *successful* scrape
    /// is no older than `stale_metrics_seconds`, that cached sample is reused.
    ///
    /// For plaintext metrics, after the grace period expires or before any
    /// successful scrape, metrics fall back to neutral scoring. When TLS is
    /// configured, the same condition marks the provider unhealthy and excludes
    /// it from routing.
    ///
    /// **Default (absent):** no grace period. Plaintext scrape failures immediately
    /// produce neutral scoring; TLS-configured failures without a cached sample
    /// fail closed.
    ///
    /// **Minimum value:** `1` second.  The schema rejects `0`; the operator also
    /// treats zero defensively as absent.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stale_metrics_seconds: Option<u32>,

    /// TLS configuration for the metrics endpoint.
    ///
    /// When set, the operator uses the provided CA certificate for TLS server
    /// verification and optionally presents a client certificate for mutual TLS
    /// (mTLS).  When absent, the scraper uses system root certificates
    /// (backward-compatible).
    ///
    /// **Fail-closed:** when configured but the referenced Secrets cannot be
    /// resolved or contain invalid material, the scrape is skipped entirely.
    /// The scraper never falls back to system roots or plain HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<EndpointTlsConfig>,
}

/// TLS configuration for endpoint scraping or health probes.
///
/// Controls how the operator verifies the remote server's identity and,
/// optionally, authenticates itself to the server via a client certificate
/// (mutual TLS / mTLS).  Used by both `metricsConfig.tls` and
/// `healthCheck.tls`.
///
/// Secret references include explicit `namespace` and `name` fields.
/// The operator reads referenced Secrets during reconciliation —
/// bounded requeue (60 s for TLS-configured providers) detects
/// certificate rotation without a cluster-wide Secret watch.
///
/// # Secret key conventions
///
/// | Secret ref                       | Default keys                | Override field(s)                        |
/// |----------------------------------|-----------------------------|------------------------------------------|
/// | `ca_secret_ref`                  | `ca.crt`                    | `key`                                    |
/// | `client_certificate_secret_ref`  | `tls.crt` / `tls.key`       | `certificate_key` / `private_key_key`    |
///
/// # Security invariant
///
/// Private key bytes from `client_certificate_secret_ref` are loaded into a
/// [`rustls::ClientConfig`] and **never** written to logs, events, status
/// fields, or Prometheus labels.
///
/// [`rustls::ClientConfig`]: rustls::ClientConfig
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EndpointTlsConfig {
    /// Reference to a Secret containing the CA certificate PEM for server
    /// verification.
    ///
    /// The Secret must contain the PEM-encoded CA certificate under the key
    /// `ca.crt` (or the key specified by `key`).  When this CA is
    /// set, the scraper trusts **only** this CA; system root certificates
    /// are not consulted.
    pub ca_secret_ref: SecretRef,

    /// Reference to a Secret containing the client certificate and private
    /// key for mutual TLS.
    ///
    /// When set, the scraper presents this identity during the TLS handshake.
    /// The Secret must contain `tls.crt` (or `certificate_key`) and
    /// `tls.key` (or `private_key_key`) in PEM format.
    ///
    /// When absent, the scraper performs one-way TLS only (server verification
    /// with the CA from `ca_secret_ref`, no client certificate).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate_secret_ref: Option<ClientCertificateSecretRef>,
}

/// Reference to a Kubernetes Secret containing a client certificate and
/// private key for mutual TLS.
///
/// Both `name` and `namespace` are required because [`InferenceProvider`] is
/// cluster-scoped.
///
/// The key fields default to the standard Kubernetes TLS Secret convention
/// (`tls.crt` / `tls.key`) but can be overridden for non-standard Secrets.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientCertificateSecretRef {
    /// Secret name.
    #[schemars(length(min = 1))]
    pub name: String,

    /// Secret namespace.
    #[schemars(length(min = 1))]
    pub namespace: String,

    /// Key within `Secret.data` holding the PEM-encoded client certificate.
    #[schemars(length(min = 1))]
    #[serde(default = "default_certificate_key")]
    pub certificate_key: String,

    /// Key within `Secret.data` holding the PEM-encoded private key.
    #[schemars(length(min = 1))]
    #[serde(default = "default_private_key_key")]
    pub private_key_key: String,
}

/// Default key for the client certificate PEM in a Secret.
fn default_certificate_key() -> String {
    "tls.crt".to_owned()
}

/// Default key for the private key PEM in a Secret.
fn default_private_key_key() -> String {
    "tls.key".to_owned()
}

/// Mapping from scoring signal names to Prometheus metric names.
///
/// Every field is optional.  A signal left as `None` is not extracted from the
/// Prometheus text output and receives the neutral default (`0.5`) in scoring.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSignalNames {
    /// Metric name for normalised queue depth (0.0–1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_depth: Option<String>,

    /// Metric name for KV-cache utilisation (0.0–1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv_cache_utilization: Option<String>,

    /// Metric name for P99 request latency in milliseconds (pre-computed gauge).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_p99_ms: Option<String>,

    /// Metric name for prefix-cache hit ratio (0.0–1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix_cache_hit_ratio: Option<String>,

    /// Metric name for normalised error rate (0.0–1.0).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_rate: Option<String>,

    /// Metric name for a health gauge (any positive value = healthy).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthy: Option<String>,
}

impl MetricSignalNames {
    /// Whether no signal has a metric name.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.queue_depth.is_none()
            && self.kv_cache_utilization.is_none()
            && self.latency_p99_ms.is_none()
            && self.prefix_cache_hit_ratio.is_none()
            && self.error_rate.is_none()
            && self.healthy.is_none()
    }
}

/// An exporter with well-known metric names.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MetricsPreset {
    /// vLLM; its queue metric is a raw request count, so it needs `queueCapacity`.
    Vllm,
    /// llm-d EPP pool averages; its queue metric is a raw average, so it needs `queueCapacity`.
    LlmdEpp,
}

impl MetricsPreset {
    /// The preset's `(queueDepth, kvCacheUtilization)` metric names.
    #[must_use]
    pub const fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::Vllm => ("vllm:num_requests_waiting", "vllm:kv_cache_usage_perc"),
            Self::LlmdEpp => ("llm_d_epp_average_queue_size", "llm_d_epp_average_kv_cache_utilization"),
        }
    }
}

/// Why a provider's metric signals cannot be used as declared.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignalNamesIssue {
    /// Neither a preset nor `signalNames` names any signal.
    NoSignalNames,
    /// The preset's raw queue metric has no `queueCapacity` to normalise against.
    MissingQueueCapacity,
}

impl SignalNamesIssue {
    /// The `MetricsSignals` condition reason.
    #[must_use]
    pub const fn reason(self) -> &'static str {
        match self {
            Self::NoSignalNames => "NoSignalNames",
            Self::MissingQueueCapacity => "MissingQueueCapacity",
        }
    }
}

impl MetricsConfig {
    /// Signal names after applying `preset`, with each explicit `signalNames` entry winning.
    #[must_use]
    pub fn resolved_signal_names(&self) -> MetricSignalNames {
        let mut names = self.signal_names.clone().unwrap_or_default();
        if let Some(preset) = self.preset {
            let (queue, kv) = preset.names();
            names.queue_depth.get_or_insert_with(|| queue.to_owned());
            names.kv_cache_utilization.get_or_insert_with(|| kv.to_owned());
        }
        names
    }

    /// Signal names the scrape uses: resolved, minus a preset raw queue metric with no `queueCapacity`.
    ///
    /// An unnormalised count would clamp to full saturation, so that signal scores neutrally instead.
    #[must_use]
    pub fn effective_signal_names(&self) -> MetricSignalNames {
        let mut names = self.resolved_signal_names();
        if self.signal_names_issue() == Some(SignalNamesIssue::MissingQueueCapacity) {
            names.queue_depth = None;
        }
        names
    }

    /// Whether the resolved names are unusable as declared.
    #[must_use]
    pub fn signal_names_issue(&self) -> Option<SignalNamesIssue> {
        let resolved = self.resolved_signal_names();
        if resolved.is_empty() {
            return Some(SignalNamesIssue::NoSignalNames);
        }
        let queue_from_preset = self
            .preset
            .is_some_and(|preset| resolved.queue_depth.as_deref() == Some(preset.names().0));
        (queue_from_preset && self.queue_capacity.is_none()).then_some(SignalNamesIssue::MissingQueueCapacity)
    }
}

/// Returns the default metrics scrape path.
fn default_metrics_path() -> String {
    "/metrics".to_owned()
}

/// Returns the default metrics scrape timeout.
fn default_metrics_timeout() -> String {
    "2s".to_owned()
}

/// Cost information for an inference provider.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CostConfig {
    /// Cost per million input tokens (USD).
    #[serde(default)]
    pub per_million_input_tokens: f64,

    /// Cost per million output tokens (USD).
    #[serde(default)]
    pub per_million_output_tokens: f64,
}

/// Model metadata for an inference provider.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelInfo {
    /// Model name.
    #[schemars(regex(pattern = r"\S"))]
    pub name: String,

    /// Supported capabilities.
    #[serde(default)]
    pub capabilities: Vec<String>,

    /// Maximum context window size.
    pub context_window: Option<u32>,
}

/// Health check configuration.
///
/// The probe URL is `{endpoint}{path}` when `endpoint` is set, or
/// `{spec.endpoint}{path}` when absent.  This allows pointing health
/// probes at a different service (e.g. an llm-d EPP health endpoint)
/// while `spec.endpoint` points at the inference backend.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthCheckConfig {
    /// Base URL for health probes, independent of `spec.endpoint`.
    ///
    /// When set, the probe URL is `{endpoint}{path}` instead of
    /// `{spec.endpoint}{path}`.  This allows probing a separate service
    /// (such as an llm-d EPP) while the provider inference endpoint
    /// points at the pool's request path.
    ///
    /// When absent, the probe URL uses `spec.endpoint` as before.
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Check interval (e.g. "30s").
    pub interval: Option<String>,

    /// HTTP path for health probes.
    pub path: Option<String>,

    /// Timeout per check (e.g. "5s").
    pub timeout: Option<String>,

    /// TLS configuration for the health probe endpoint.
    ///
    /// When set, the operator uses the provided CA certificate for TLS server
    /// verification and optionally presents a client certificate for mutual TLS
    /// (mTLS).  When absent, the probe uses system root certificates
    /// (backward-compatible).
    ///
    /// **Fail-closed:** when configured but the referenced Secrets cannot be
    /// resolved or contain invalid material, the probe is skipped entirely.
    /// The operator never falls back to system roots or plain HTTP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<EndpointTlsConfig>,
}

// ---------------------------------------------------------------------------
// Model Discovery
// ---------------------------------------------------------------------------

/// Served-model discovery source. Exactly one variant must be set.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ModelDiscoveryConfig {
    /// OpenAI-compatible `GET /v1/models` (vLLM, `KServe`, `OpenAI`).
    ///
    /// Reads `data[].id` from the response. The response is rejected as a
    /// whole when it is not valid JSON, has a blank, overlong, or duplicate
    /// id, or lists more models than the operator's cap. A rejected response
    /// never clears the previously discovered set.
    OpenAiModels(OpenAiModelsSource),
}

/// OpenAI-compatible model-listing source.
///
/// Uses `spec.auth` for the bearer token. With `auth.manual`, requests are
/// sent without credentials.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenAiModelsSource {
    /// Base URL; defaults to `spec.endpoint`.
    #[schemars(length(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Path appended to the base URL.
    #[schemars(length(min = 1))]
    #[serde(default = "default_models_path")]
    pub path: String,

    /// TLS configuration for the model-listing endpoint.
    ///
    /// When absent, system root certificates are used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<EndpointTlsConfig>,
}

impl OpenAiModelsSource {
    /// Full model-listing URL after applying the endpoint and path defaults.
    pub(crate) fn effective_url(&self, provider_endpoint: &str) -> String {
        let base = self.endpoint.as_deref().unwrap_or(provider_endpoint);
        format!("{}/{}", base.trim_end_matches('/'), self.path.trim_start_matches('/'))
    }
}

/// Default OpenAI-compatible model-listing path.
fn default_models_path() -> String {
    "/v1/models".to_owned()
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/// Observed status of an [`InferenceProvider`].
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceProviderStatus {
    /// Observed conditions, `metav1.Condition` shaped and keyed by `type`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(extend("x-kubernetes-list-type" = "map", "x-kubernetes-list-map-keys" = ["type"]))]
    pub conditions: Vec<super::condition::Condition>,

    /// Sites matched by the site selector.
    #[serde(default)]
    #[schemars(extend("x-kubernetes-list-type" = "set"))]
    pub matching_sites: Vec<String>,

    /// Signal metric names the scrape uses after `metricsConfig.preset`, absent without `metricsConfig`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_signals: Option<MetricSignalNames>,

    /// Bounded reason for the latest model-discovery failure, absent after a successful poll.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_discovery_error: Option<String>,

    /// Effective model-listing URL when discovery is configured.
    ///
    /// This is the requested URL, not an indication that polling succeeded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_discovery_url: Option<String>,

    /// Last observed generation.
    #[serde(default)]
    pub observed_generation: i64,

    /// Current phase.
    #[serde(default)]
    pub phase: ProviderPhase,
}

impl InferenceProviderStatus {
    /// Compare the fields written by provider reconciliation, excluding the discovery poller's error.
    pub(crate) fn matches_reconciler_status(&self, desired: &Self) -> bool {
        self.conditions == desired.conditions
            && self.matching_sites == desired.matching_sites
            && self.metrics_signals == desired.metrics_signals
            && self.model_discovery_url == desired.model_discovery_url
            && self.observed_generation == desired.observed_generation
            && self.phase == desired.phase
    }
}

/// Lifecycle phase of a provider resource.
#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ProviderPhase {
    /// Waiting for validation.
    #[default]
    Pending,

    /// Provider is healthy and available for routing.
    Available,

    /// Provider is partially degraded.
    Degraded,

    /// Provider is not reachable.
    Unavailable,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
    use kube::CustomResourceExt as _;

    use super::*;

    fn crd_json() -> serde_json::Value {
        serde_json::to_value(InferenceProvider::crd()).unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn model_discovery_defaults() {
        let json = serde_json::json!({ "openAiModels": {} });

        let config: ModelDiscoveryConfig = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());

        let ModelDiscoveryConfig::OpenAiModels(source) = &config;
        assert_eq!(source.path, "/v1/models", "default path");
        assert!(source.endpoint.is_none(), "endpoint defaults to spec.endpoint");
    }

    #[test]
    fn default_phase() {
        let phase = ProviderPhase::default();
        assert_eq!(phase, ProviderPhase::Pending, "should default to Pending");
    }

    #[test]
    fn spec_serde() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "hostSelector": {},
            "providerKind": "anthropic",
            "backendKind": "apiProvider",
            "endpoint": "https://api.anthropic.com",
            "models": [{"name": "claude-sonnet-4"}]
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(spec.provider_kind, ProviderKind::Anthropic, "provider kind");
        assert_eq!(spec.backend_kind, BackendKind::ApiProvider, "backend kind");
        assert_eq!(spec.models.len(), 1, "model count");
    }

    #[test]
    fn unknown_and_v1alpha1_kind_values_are_refused() {
        for (provider_kind, backend_kind) in [
            ("self_hosted", "local"),
            ("vllm-vcr", "local"),
            ("open_ai", "local"),
            ("openAi", "local_model"),
            ("openAi", "api_provider"),
            ("openAi", "cloud_managed"),
            ("openAi", ""),
        ] {
            let json = serde_json::json!({
                "gridNetworkRef": "production",
                "hostSelector": {},
                "providerKind": provider_kind,
                "backendKind": backend_kind,
                "endpoint": "http://backend:8080"
            });
            assert!(
                serde_json::from_value::<InferenceProviderSpec>(json).is_err(),
                "{provider_kind}/{backend_kind} must be refused"
            );
        }
    }

    #[test]
    fn backend_kind_gossip_names_round_trip_and_stay_snake_case() {
        for (kind, wire) in [
            (BackendKind::Local, "local"),
            (BackendKind::Remote, "remote"),
            (BackendKind::CloudManaged, "cloud_managed"),
            (BackendKind::ApiProvider, "api_provider"),
        ] {
            assert_eq!(kind.wire_name(), wire, "gossip name for {kind:?}");
            assert_eq!(
                BackendKind::from_wire_name(wire),
                Some(kind.to_scoring()),
                "{wire} parses back"
            );
        }
        assert_eq!(
            BackendKind::from_wire_name("apiProvider"),
            None,
            "camelCase is not a gossip name"
        );
    }

    #[test]
    fn kinds_map_to_the_scoring_engine_without_serde() {
        assert_eq!(ProviderKind::OpenAi.to_scoring(), scoring::ProviderKind::OpenAi);
        assert_eq!(ProviderKind::Anthropic.to_scoring(), scoring::ProviderKind::Anthropic);
        assert_eq!(ProviderKind::Bedrock.to_scoring(), scoring::ProviderKind::Bedrock);
        assert_eq!(ProviderKind::Vertex.to_scoring(), scoring::ProviderKind::Vertex);
        assert_eq!(
            BackendKind::CloudManaged.to_scoring(),
            scoring::BackendKind::CloudManaged
        );
    }

    #[test]
    fn administrative_policy_round_trips_and_is_omitted_by_default() {
        let base = serde_json::json!({
            "gridNetworkRef": "production", "providerKind": "openAi",
            "hostSelector": {},
            "backendKind": "local", "endpoint": "http://backend:8080"
        });
        let spec: InferenceProviderSpec = serde_json::from_value(base).unwrap_or_else(|_| std::process::abort());
        let serialized = serde_json::to_value(&spec).unwrap_or_else(|_| std::process::abort());
        assert!(serialized.get("gatewayRef").is_none());
        assert!(serialized.get("trafficPolicy").is_none());

        let drained: InferenceProviderSpec = serde_json::from_value(serde_json::json!({
            "gridNetworkRef": "production", "providerKind": "openAi",
            "hostSelector": {},
            "backendKind": "local", "endpoint": "http://backend:8080",
            "gatewayRef": "provider-gateway-a", "trafficPolicy": {"drain": true}
        }))
        .unwrap_or_else(|_| std::process::abort());
        assert_eq!(drained.gateway_ref.as_deref(), Some("provider-gateway-a"));
        assert!(drained.traffic_policy.as_ref().is_some_and(|p| p.drain));
    }

    #[test]
    fn crd_contains_administrative_provider_fields() {
        let crd = crd_json();
        let properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(properties.contains_key("gatewayRef"));
        assert!(properties.contains_key("trafficPolicy"));
    }

    #[test]
    fn inference_provider_crd_has_correct_group_and_plural() {
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
            "inferenceproviders",
            "wrong plural name"
        );
        assert_eq!(
            crd.get("spec")
                .and_then(|spec| spec.get("names"))
                .and_then(|names| names.get("kind"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or_else(|| std::process::abort()),
            "InferenceProvider",
            "wrong kind name"
        );
    }

    #[test]
    fn inference_provider_crd_has_short_name() {
        let crd = crd_json();
        assert_eq!(
            crd.pointer("/spec/names/shortNames"),
            Some(&serde_json::json!(["infpvd"])),
            "kubectl get infpvd needs this short name"
        );
    }

    #[test]
    fn deploy_crd_manifest_has_generated_short_names() {
        let manifest: CustomResourceDefinition =
            serde_yaml::from_str(include_str!("../../../deploy/crds/inferenceprovider.yaml"))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            manifest.spec.names.short_names,
            InferenceProvider::crd().spec.names.short_names,
            "chart CRD manifest and Rust definition must have the same short names"
        );
    }

    #[test]
    fn inference_provider_crd_has_status_subresource() {
        let crd = crd_json();
        assert!(
            crd.pointer("/spec/versions/0/subresources/status").is_some(),
            "CRD must declare a status subresource"
        );
    }

    #[test]
    fn inference_provider_crd_has_host_selector_field() {
        let crd = crd_json();
        let spec_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec_properties.contains_key("hostSelector"),
            "CRD schema must include hostSelector field"
        );
    }

    #[test]
    fn inference_provider_crd_has_metrics_config_field() {
        let crd = crd_json();
        let spec_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            spec_properties.contains_key("metricsConfig"),
            "CRD schema must include metricsConfig field"
        );
    }

    #[test]
    fn inference_provider_crd_health_check_has_endpoint_and_tls_fields() {
        let crd = crd_json();
        let hc_properties = crd
            .pointer("/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/healthCheck/properties")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());
        assert!(
            hc_properties.contains_key("endpoint"),
            "healthCheck must include endpoint field"
        );
        assert!(hc_properties.contains_key("tls"), "healthCheck must include tls field");
        assert!(
            hc_properties.contains_key("path"),
            "healthCheck must include path field"
        );
        assert!(
            hc_properties.contains_key("interval"),
            "healthCheck must include interval field"
        );
        assert!(
            hc_properties.contains_key("timeout"),
            "healthCheck must include timeout field"
        );
    }

    #[test]
    fn metrics_config_absent_deserializes() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "hostSelector": {},
            "providerKind": "openAi",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}]
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(
            spec.metrics_config.is_none(),
            "metricsConfig must be absent when not set"
        );
    }

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "tests multiple signal_names fields in one assertion block"
    )]
    fn metrics_config_with_path_and_signal_names_deserializes() {
        let json = serde_json::json!({
            "gridNetworkRef": "production",
            "hostSelector": {},
            "providerKind": "openAi",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}],
            "metricsConfig": {
                "path": "/custom/metrics",
                "timeout": "500ms",
                "signalNames": {
                    "queueDepth": "provider_queue_depth",
                    "kvCacheUtilization": "provider_kv_cache"
                }
            }
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let mc = spec.metrics_config.unwrap_or_else(|| std::process::abort());
        assert_eq!(mc.path, "/custom/metrics", "path must round-trip");
        assert_eq!(mc.timeout, "500ms", "timeout must round-trip");
        assert_eq!(
            mc.resolved_signal_names().queue_depth.as_deref(),
            Some("provider_queue_depth"),
            "queueDepth must round-trip"
        );
        assert_eq!(
            mc.resolved_signal_names().kv_cache_utilization.as_deref(),
            Some("provider_kv_cache"),
            "kvCacheUtilization must round-trip"
        );
        assert!(
            mc.resolved_signal_names().latency_p99_ms.is_none(),
            "unconfigured signal must be None"
        );
    }

    fn metrics(json: serde_json::Value) -> MetricsConfig {
        serde_json::from_value(json).unwrap_or_else(|_| std::process::abort())
    }

    #[test]
    fn presets_resolve_to_the_upstream_metric_names() {
        let vllm = metrics(serde_json::json!({ "preset": "vllm" })).resolved_signal_names();
        assert_eq!(vllm.queue_depth.as_deref(), Some("vllm:num_requests_waiting"));
        assert_eq!(vllm.kv_cache_utilization.as_deref(), Some("vllm:kv_cache_usage_perc"));
        let epp = metrics(serde_json::json!({ "preset": "llmdEpp" })).resolved_signal_names();
        assert_eq!(epp.queue_depth.as_deref(), Some("llm_d_epp_average_queue_size"));
        assert_eq!(
            epp.kv_cache_utilization.as_deref(),
            Some("llm_d_epp_average_kv_cache_utilization")
        );
    }

    #[test]
    fn an_explicit_signal_name_overrides_the_preset_for_that_signal_only() {
        let names = metrics(serde_json::json!({
            "preset": "vllm",
            "signalNames": { "queueDepth": "custom_queue" }
        }))
        .resolved_signal_names();
        assert_eq!(names.queue_depth.as_deref(), Some("custom_queue"));
        assert_eq!(names.kv_cache_utilization.as_deref(), Some("vllm:kv_cache_usage_perc"));
    }

    #[test]
    fn signal_names_issues_follow_the_preset_and_capacity() {
        assert_eq!(
            metrics(serde_json::json!({})).signal_names_issue(),
            Some(SignalNamesIssue::NoSignalNames)
        );
        assert_eq!(
            metrics(serde_json::json!({ "preset": "vllm" })).signal_names_issue(),
            Some(SignalNamesIssue::MissingQueueCapacity)
        );
        assert_eq!(
            metrics(serde_json::json!({ "preset": "vllm", "queueCapacity": 8 })).signal_names_issue(),
            None
        );
        assert_eq!(
            metrics(serde_json::json!({ "preset": "vllm", "signalNames": { "queueDepth": "normalised" } }))
                .signal_names_issue(),
            None,
            "an explicit queue metric is the user's to normalise"
        );
        assert_eq!(
            metrics(serde_json::json!({ "signalNames": { "kvCacheUtilization": "kv" } })).signal_names_issue(),
            None
        );
    }

    #[test]
    fn a_preset_queue_without_capacity_is_left_out_of_the_scrape() {
        let names = metrics(serde_json::json!({ "preset": "vllm" })).effective_signal_names();
        assert!(
            names.queue_depth.is_none(),
            "a raw count must not reach scoring unnormalised"
        );
        assert_eq!(names.kv_cache_utilization.as_deref(), Some("vllm:kv_cache_usage_perc"));
        let capped = metrics(serde_json::json!({ "preset": "vllm", "queueCapacity": 8 })).effective_signal_names();
        assert_eq!(capped.queue_depth.as_deref(), Some("vllm:num_requests_waiting"));
    }

    #[test]
    fn an_unknown_preset_is_refused() {
        let parsed: Result<MetricsConfig, _> = serde_json::from_value(serde_json::json!({ "preset": "triton" }));
        assert!(parsed.ok().is_none(), "triton is not a preset");
    }

    #[test]
    fn unset_signal_names_serialize_without_nulls() {
        let json = serde_json::to_value(metrics(serde_json::json!({ "signalNames": { "queueDepth": "q" } })))
            .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            json.pointer("/signalNames"),
            Some(&serde_json::json!({ "queueDepth": "q" }))
        );
    }

    #[test]
    fn metrics_config_defaults_apply_when_fields_absent() {
        let json = serde_json::json!({
            "gridNetworkRef": "net",
            "hostSelector": {},
            "providerKind": "openAi",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}],
            "metricsConfig": {}
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let mc = spec.metrics_config.unwrap_or_else(|| std::process::abort());
        assert_eq!(mc.path, "/metrics", "path must default to /metrics");
        assert_eq!(mc.timeout, "2s", "timeout must default to 2s");
        assert!(mc.signal_names.is_none(), "signalNames must default to absent");
        assert!(mc.preset.is_none(), "preset must default to absent");
    }

    #[test]
    fn stale_metrics_seconds_defaults_to_none_when_absent() {
        let json = serde_json::json!({
            "gridNetworkRef": "net",
            "hostSelector": {},
            "providerKind": "openAi",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}],
            "metricsConfig": {}
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let mc = spec.metrics_config.unwrap_or_else(|| std::process::abort());
        assert!(
            mc.stale_metrics_seconds.is_none(),
            "absent staleMetricsSeconds must default to None (no grace period)"
        );
    }

    #[test]
    fn stale_metrics_seconds_round_trips() {
        let json = serde_json::json!({
            "gridNetworkRef": "net",
            "hostSelector": {},
            "providerKind": "openAi",
            "backendKind": "local",
            "endpoint": "http://backend:8080",
            "models": [{"name": "model-a"}],
            "metricsConfig": {"staleMetricsSeconds": 30}
        });
        let spec: InferenceProviderSpec = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        let mc = spec.metrics_config.unwrap_or_else(|| std::process::abort());
        assert_eq!(
            mc.stale_metrics_seconds,
            Some(30),
            "staleMetricsSeconds must round-trip through serde"
        );
    }

    #[test]
    fn stale_metrics_seconds_absent_from_serialised_output_when_none() {
        let mc = MetricsConfig {
            path: "/metrics".to_owned(),
            timeout: "2s".to_owned(),
            preset: None,
            signal_names: None,
            stale_metrics_seconds: None,
            metrics_endpoint: None,
            pool_name: None,
            queue_capacity: None,
            tls: None,
        };
        let serialised = serde_json::to_value(&mc).unwrap_or_else(|_| std::process::abort());
        assert!(
            serialised.get("staleMetricsSeconds").is_none(),
            "absent staleMetricsSeconds must not appear in serialised output"
        );
    }

    // -----------------------------------------------------------------------
    // CRD schema: minLength validation on Secret reference fields
    // -----------------------------------------------------------------------

    #[test]
    #[expect(
        clippy::too_many_lines,
        reason = "tests multiple schema paths in one assertion block"
    )]
    fn crd_schema_enforces_min_length_on_tls_secret_fields() {
        let crd = crd_json();
        let tls_props = crd
            .pointer(
                "/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/metricsConfig/properties/tls/properties",
            )
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());

        let ca_ref_props = tls_props
            .get("caSecretRef")
            .and_then(|v| v.pointer("/properties"))
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(
            ca_ref_props
                .get("name")
                .and_then(|v| v.get("minLength"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "caSecretRef.name must have minLength: 1"
        );
        assert_eq!(
            ca_ref_props
                .get("namespace")
                .and_then(|v| v.get("minLength"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "caSecretRef.namespace must have minLength: 1"
        );

        let client_ref_props = tls_props
            .get("clientCertificateSecretRef")
            .and_then(|v| v.pointer("/properties"))
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| std::process::abort());

        assert_eq!(
            client_ref_props
                .get("name")
                .and_then(|v| v.get("minLength"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "clientCertificateSecretRef.name must have minLength: 1"
        );
        assert_eq!(
            client_ref_props
                .get("namespace")
                .and_then(|v| v.get("minLength"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "clientCertificateSecretRef.namespace must have minLength: 1"
        );
        assert_eq!(
            client_ref_props
                .get("certificateKey")
                .and_then(|v| v.get("minLength"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "clientCertificateSecretRef.certificateKey must have minLength: 1"
        );
        assert_eq!(
            client_ref_props
                .get("privateKeyKey")
                .and_then(|v| v.get("minLength"))
                .and_then(serde_json::Value::as_u64),
            Some(1),
            "clientCertificateSecretRef.privateKeyKey must have minLength: 1"
        );
    }
}
