//! Projects an approved enrollment into the cluster resources that make a member
//! visible: a `GridSite` (reachability and trust) and the MaaS `ExternalProvider`
//! / `ExternalModel` (what it serves).
//!
//! One definition, two consumers: gridctl serializes it to YAML for `kubectl
//! apply`, and the operator deserializes it into its typed `GridSite` for
//! server-side apply. The projection is pure data with no Kubernetes dependency,
//! which is what keeps the enrollment service free of one.

use serde_json::{Value, json};

/// Anything wrong while projecting an enrollment record.
#[derive(Debug, thiserror::Error)]
pub enum ProjectionError {
    /// The record carried no egress address to reach the member on.
    #[error("the enrollment recorded no egress address")]
    NoEgress,
    /// The issued certificate could not be fingerprinted.
    #[error("fingerprinting the certificate: {0}")]
    Fingerprint(String),
    /// The objects could not be rendered as YAML.
    #[error("rendering YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
}

/// A string field of the record, or empty when absent.
fn field<'record>(record: &'record Value, name: &str) -> &'record str {
    record.get(name).and_then(Value::as_str).unwrap_or_default()
}

/// The egress address the member recorded, if any.
fn egress_address(record: &Value) -> Option<&str> {
    record.get("egress")?.get("address")?.as_str()
}

/// The `GridSite` object for an approved member.
///
/// Name and `serverName` come from the site name, `gridNetworkRef` and egress from
/// the request, and the canonical fingerprint from the issued certificate — a
/// projection of the record plus its cert, nothing hand-authored.
///
/// `GridSite` is cluster-scoped, so no namespace is stamped on it.
///
/// # Errors
///
/// [`ProjectionError::NoEgress`] when the record has no egress address, or
/// [`ProjectionError::Fingerprint`] when the certificate cannot be fingerprinted.
pub fn grid_site(record: &Value) -> Result<Value, ProjectionError> {
    let site = field(record, "siteName");
    let address = egress_address(record).ok_or(ProjectionError::NoEgress)?;
    let fingerprint = certs::canonical_fingerprint(field(record, "certificate"))
        .map_err(|error| ProjectionError::Fingerprint(error.to_string()))?;

    Ok(json!({
        "apiVersion": "grid.praxis-proxy.io/v1alpha1",
        "kind": "GridSite",
        "metadata": {
            "name": site,
            "annotations": {
                "grid.praxis-proxy.io/enrolled-as": field(record, "spiffeId"),
                "grid.praxis-proxy.io/enrollment-request": field(record, "requestId"),
            },
        },
        "spec": grid_site_spec(record, site, address, &fingerprint),
    }))
}

/// The `spec` of a member's `GridSite`: where to reach it and how to trust it.
fn grid_site_spec(record: &Value, site: &str, address: &str, fingerprint: &str) -> Value {
    json!({
        "gridNetworkRef": field(record, "gridNetworkRef"),
        "egress": {
            "address": address,
            "tls": { "mode": "mutual", "serverName": format!("{site}.grid.internal") },
        },
        "trust": { "canonicalFingerprints": [fingerprint] },
    })
}

/// The MaaS resources that surface a member's models: one `ExternalProvider` plus
/// an `ExternalModel` per model.
///
/// # Errors
///
/// [`ProjectionError::NoEgress`] when the record has no egress address.
pub fn maas(record: &Value, namespace: &str) -> Result<Vec<Value>, ProjectionError> {
    let site = field(record, "siteName");
    let address = egress_address(record).ok_or(ProjectionError::NoEgress)?;

    let mut objects = vec![external_provider(site, namespace, address)];
    let models = record
        .get("capabilities")
        .and_then(|caps| caps.get("inference"))
        .and_then(|inference| inference.get("models"))
        .and_then(Value::as_array);
    for model in models.into_iter().flatten() {
        objects.push(external_model(site, namespace, model));
    }
    Ok(objects)
}

/// The provider entry for one enrolled member; it authenticates by the mTLS
/// wristband Secret the site's enrollment produced.
fn external_provider(site: &str, namespace: &str, address: &str) -> Value {
    json!({
        "apiVersion": "inference.opendatahub.io/v1alpha1",
        "kind": "ExternalProvider",
        "metadata": { "name": site, "namespace": namespace },
        "spec": {
            "provider": site,
            "endpoint": address,
            "auth": { "type": "mtls", "secretRef": { "name": format!("{site}-grid-identity") } },
        },
    })
}

/// One model the member serves, pointed at its provider entry.
fn external_model(site: &str, namespace: &str, model: &Value) -> Value {
    let name = field(model, "name");
    let slug: String = name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch.to_ascii_lowercase() } else { '-' })
        .collect();
    json!({
        "apiVersion": "inference.opendatahub.io/v1alpha1",
        "kind": "ExternalModel",
        "metadata": { "name": format!("{slug}-{site}"), "namespace": namespace },
        "spec": {
            "modelName": slug,
            "externalProviderRefs": [{
                "ref": { "name": site },
                "targetModel": name,
                "path": field(model, "path"),
                "apiFormat": field(model, "apiFormat"),
                "weight": 100,
            }],
        },
    })
}

/// Render objects as a multi-document YAML stream for `kubectl apply -f -`.
///
/// # Errors
///
/// [`ProjectionError::Yaml`] when an object cannot be serialized.
pub fn to_yaml(objects: &[Value]) -> Result<String, ProjectionError> {
    let mut out = String::new();
    for object in objects {
        out.push_str("---\n");
        out.push_str(&serde_yaml::to_string(object)?);
    }
    Ok(out)
}

#[cfg(test)]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::{ProjectionError, Value, grid_site, json, maas, to_yaml};

    /// `canonical_fingerprint` only base64-decodes and hashes the PEM body, so any
    /// well-formed PEM block stands in for an issued certificate here.
    const CERT: &str = "-----BEGIN CERTIFICATE-----\ndGVzdA==\n-----END CERTIFICATE-----\n";

    fn record() -> Value {
        json!({
            "siteName": "site-x",
            "gridNetworkRef": "grid.internal",
            "spiffeId": "spiffe://grid.internal/site/site-x",
            "requestId": "req-123",
            "certificate": CERT,
            "egress": { "address": "site-x.example:8443" },
            "capabilities": { "inference": { "models": [
                { "name": "Qwen/Qwen3-0.6B", "path": "/v1", "apiFormat": "openai-chat" }
            ] } }
        })
    }

    #[test]
    fn grid_site_projects_reachability_and_trust() {
        let site = grid_site(&record()).expect("projects");
        assert_eq!(site["apiVersion"], "grid.praxis-proxy.io/v1alpha1");
        assert_eq!(site["kind"], "GridSite");
        assert_eq!(site["metadata"]["name"], "site-x");
        // GridSite is cluster-scoped, so no namespace is stamped on it.
        assert!(site["metadata"].get("namespace").is_none());
        assert_eq!(
            site["metadata"]["annotations"]["grid.praxis-proxy.io/enrolled-as"],
            "spiffe://grid.internal/site/site-x"
        );
        assert_eq!(site["spec"]["gridNetworkRef"], "grid.internal");
        assert_eq!(site["spec"]["egress"]["address"], "site-x.example:8443");
        assert_eq!(site["spec"]["egress"]["tls"]["mode"], "mutual");
        assert_eq!(site["spec"]["egress"]["tls"]["serverName"], "site-x.grid.internal");
        let fingerprints = site["spec"]["trust"]["canonicalFingerprints"]
            .as_array()
            .expect("fingerprints is an array");
        assert_eq!(fingerprints.len(), 1);
        assert!(!fingerprints[0].as_str().expect("fingerprint is a string").is_empty());
    }

    #[test]
    fn grid_site_requires_egress() {
        let mut record = record();
        record.as_object_mut().expect("object").remove("egress");
        assert!(matches!(grid_site(&record), Err(ProjectionError::NoEgress)));
    }

    #[test]
    fn maas_projects_provider_and_a_model() {
        let objects = maas(&record(), "models-as-a-service").expect("projects");
        assert_eq!(objects.len(), 2, "one ExternalProvider plus one ExternalModel");

        let provider = &objects[0];
        assert_eq!(provider["kind"], "ExternalProvider");
        assert_eq!(provider["metadata"]["name"], "site-x");
        assert_eq!(provider["metadata"]["namespace"], "models-as-a-service");
        assert_eq!(provider["spec"]["endpoint"], "site-x.example:8443");
        assert_eq!(provider["spec"]["auth"]["type"], "mtls");
        assert_eq!(provider["spec"]["auth"]["secretRef"]["name"], "site-x-grid-identity");

        let model = &objects[1];
        assert_eq!(model["kind"], "ExternalModel");
        assert_eq!(model["metadata"]["name"], "qwen-qwen3-0-6b-site-x");
        assert_eq!(model["spec"]["modelName"], "qwen-qwen3-0-6b");
        let refs = model["spec"]["externalProviderRefs"].as_array().expect("refs array");
        assert_eq!(refs[0]["ref"]["name"], "site-x");
        assert_eq!(refs[0]["targetModel"], "Qwen/Qwen3-0.6B");
        assert_eq!(refs[0]["weight"], 100);
    }

    #[test]
    fn to_yaml_emits_a_multi_document_stream() {
        let objects = vec![grid_site(&record()).expect("projects")];
        let yaml = to_yaml(&objects).expect("renders");
        assert!(yaml.starts_with("---\n"));
        assert!(yaml.contains("kind: GridSite"));
    }
}
