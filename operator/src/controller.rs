//! Kubernetes controllers for the Grid Operator.

/// [`AgentToolProvider`] controller.
///
/// [`AgentToolProvider`]: crate::crd::agent_tool_provider::AgentToolProvider
pub mod agent_tool_provider;

/// [`GridNetwork`] controller.
///
/// [`GridNetwork`]: crate::crd::grid_network::GridNetwork
pub mod grid_network;

/// [`GridSite`] controller.
///
/// [`GridSite`]: crate::crd::grid_site::GridSite
pub mod grid_site;

/// [`InferenceProvider`] controller (OP-02).
///
/// [`InferenceProvider`]: crate::crd::inference_provider::InferenceProvider
pub mod inference_provider;

use k8s_openapi::api::core::v1::ObjectReference;
use kube::{
    Client,
    runtime::events::{Event, EventType, Recorder, Reporter},
};

use crate::crd::condition::Rejection;

/// Publish a Warning event for `rejection`; callers skip one their conditions already report.
pub(crate) async fn publish_rejection(
    client: &Client,
    controller: &str,
    object_ref: &ObjectReference,
    rejection: &Rejection,
) {
    tracing::warn!(reason = rejection.reason, message = %rejection.message, "spec rejected");
    let reporter = Reporter {
        controller: controller.to_owned(),
        instance: None,
    };
    let event = Event {
        type_: EventType::Warning,
        reason: rejection.reason.to_owned(),
        note: Some(rejection.message.clone()),
        action: "Validate".to_owned(),
        secondary: None,
    };
    if let Err(error) = Recorder::new(client.clone(), reporter)
        .publish(&event, object_ref)
        .await
    {
        tracing::warn!(%error, "failed to publish spec rejection event");
    }
}

/// Whether `value` is an absolute `http` or `https` URL with a host.
pub(crate) fn is_http_url(value: &str) -> bool {
    reqwest::Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https") && url.host_str().is_some_and(|host| !host.is_empty())
    })
}

/// Whether `value` is a bare `host:port` with a nonzero port.
pub(crate) fn is_host_port(value: &str) -> bool {
    value.rsplit_once(':').is_some_and(|(host, port)| {
        !host.is_empty() && !host.contains(['/', ' ']) && port.parse::<u16>().is_ok_and(|port| port > 0)
    })
}

/// `Some` naming `field` when `value` is neither an http(s) URL nor, if allowed, a `host:port`.
pub(crate) fn endpoint_rejection(
    field: &str,
    reason: &'static str,
    value: &str,
    allow_host_port: bool,
) -> Option<Rejection> {
    let valid = is_http_url(value) || (allow_host_port && is_host_port(value));
    let expected = if allow_host_port {
        "an http(s) URL with a host or host:port"
    } else {
        "an http(s) URL with a host"
    };
    (!valid).then(|| Rejection::new(reason, format!("{field} must be {expected}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_accept_http_urls_and_host_port() {
        assert!(endpoint_rejection("e", "R", "http://backend:8080", true).is_none());
        assert!(endpoint_rejection("e", "R", "https://api.example.com", true).is_none());
        assert!(endpoint_rejection("e", "R", "mock.default.svc:8080", true).is_none());
    }

    #[test]
    fn endpoints_refuse_blank_schemeless_and_foreign_urls() {
        for bad in [
            "",
            "   ",
            "backend",
            "backend:0",
            "ftp://x",
            "http://",
            "unix:///tmp/s",
            "a b:80",
        ] {
            assert_eq!(
                endpoint_rejection("e", "R", bad, true).map(|r| r.reason),
                Some("R"),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_url_only_field_refuses_host_port() {
        assert!(endpoint_rejection("e", "R", "metrics.svc:9090", false).is_some());
        assert!(endpoint_rejection("e", "R", "http://metrics.svc:9090", false).is_none());
    }
}
