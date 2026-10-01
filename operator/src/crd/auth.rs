//! Authentication strategy types shared across provider CRDs.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::grid_network::SecretRef;

// ---------------------------------------------------------------------------
// Auth Config
// ---------------------------------------------------------------------------

/// Authentication configuration for consuming a provider.
///
/// Declares how consumers authenticate to this provider.
/// The Grid Operator manages credential lifecycle and
/// configures Praxis to inject them transparently.
#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthConfig {
    /// Whether the user manages credentials manually.
    ///
    /// When true, the operator does not inject credentials
    /// and the user is responsible for configuring auth.
    #[serde(default)]
    pub manual: bool,

    /// Reference to a Secret containing the credential.
    pub secret_ref: Option<SecretRef>,

    /// How credentials are presented to the provider.
    pub strategy: AuthStrategy,
}

/// How credentials are presented to a provider.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum AuthStrategy {
    /// API key in a custom header (e.g. `x-api-key`).
    ApiKey,

    /// Bearer token in the Authorization header.
    BearerToken,

    /// User-configured header and value.
    Custom,

    /// Grid mTLS certificate identity (no extra header).
    MtlsOnly,

    /// `OAuth2` token with automatic refresh.
    Oauth2,

    /// Kubernetes `ServiceAccount` token.
    ServiceAccount,

    /// AWS `SigV4` per-request signing.
    Sigv4,
}

// ---------------------------------------------------------------------------
// Access Policy
// ---------------------------------------------------------------------------

/// Access policy for a provider.
///
/// Controls which sites (and optionally workloads) can
/// consume this provider. Empty selectors mean all allowed.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessPolicy {
    /// Which sites can route to this provider.
    #[serde(default)]
    pub site_selector: SelectorConfig,
}

/// Label selector for access policies.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SelectorConfig {
    /// Label key-value pairs that must match.
    #[serde(default)]
    pub match_labels: std::collections::BTreeMap<String, String>,
}

impl SelectorConfig {
    /// Whether `labels` carry every `matchLabels` pair; empty `matchLabels` matches everything.
    #[must_use]
    pub fn matches(&self, labels: Option<&std::collections::BTreeMap<String, String>>) -> bool {
        self.match_labels
            .iter()
            .all(|(key, value)| labels.and_then(|labels| labels.get(key)) == Some(value))
    }
}

/// Whether a provider's `hostSelector` places it on a site with `labels`; an omitted selector places it nowhere.
#[must_use]
pub fn hosts_on(
    selector: Option<&SelectorConfig>,
    labels: Option<&std::collections::BTreeMap<String, String>>,
) -> bool {
    selector.is_some_and(|selector| selector.matches(labels))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_strategy_serde() {
        let json = serde_json::to_string(&AuthStrategy::BearerToken).unwrap_or_else(|_| std::process::abort());
        assert_eq!(json, "\"bearerToken\"", "camelCase serialization");
    }

    #[test]
    fn access_policy_default_allows_all() {
        let policy = AccessPolicy::default();
        assert!(policy.site_selector.match_labels.is_empty(), "default should allow all");
    }

    #[test]
    fn host_selector_omitted_places_nowhere_and_empty_places_everywhere() {
        let labels: std::collections::BTreeMap<String, String> = [("region".to_owned(), "east".to_owned())].into();
        let east = SelectorConfig {
            match_labels: labels.clone(),
        };
        let west = SelectorConfig {
            match_labels: [("region".to_owned(), "west".to_owned())].into(),
        };
        assert!(!hosts_on(None, Some(&labels)), "omitted matches no site");
        assert!(!hosts_on(None, None), "omitted matches an unlabelled site neither");
        assert!(
            hosts_on(Some(&SelectorConfig::default()), None),
            "empty matches every site"
        );
        assert!(hosts_on(Some(&east), Some(&labels)));
        assert!(!hosts_on(Some(&west), Some(&labels)));
        assert!(
            !hosts_on(Some(&east), None),
            "a label selector never matches an unlabelled site"
        );
    }

    #[test]
    fn host_selector_schema_has_no_default() {
        let schema = serde_json::to_value(schemars::schema_for!(
            super::super::inference_provider::InferenceProviderSpec
        ))
        .unwrap_or_else(|_| std::process::abort());
        let host = schema
            .pointer("/properties/hostSelector")
            .unwrap_or_else(|| std::process::abort());
        assert!(
            host.get("default").is_none(),
            "a default would make an omitted selector match every site"
        );
        let required = schema.pointer("/required").and_then(serde_json::Value::as_array);
        assert!(
            !required.is_some_and(|r| r.contains(&serde_json::json!("hostSelector"))),
            "hostSelector stays optional"
        );
    }

    #[test]
    fn auth_config_manual_default_false() {
        let json = serde_json::json!({
            "strategy": "bearerToken"
        });
        let cfg: AuthConfig = serde_json::from_value(json).unwrap_or_else(|_| std::process::abort());
        assert!(!cfg.manual, "manual should default false");
    }

    #[test]
    fn auth_strategy_is_camel_case_and_refuses_snake_case() {
        let parsed: AuthStrategy =
            serde_json::from_value(serde_json::json!("bearerToken")).unwrap_or_else(|_| std::process::abort());
        assert_eq!(parsed, AuthStrategy::BearerToken, "bearerToken parses");
        assert!(
            serde_json::from_value::<AuthStrategy>(serde_json::json!("bearer_token")).is_err(),
            "the v1alpha1 value is refused"
        );
    }
}
