//! Mock llm-d inference pool, fronted by an endpoint picker.
//!
//! The grid routes to a pool in a cluster, not to a single model server, so the
//! signal that matters is the pool aggregate the endpoint picker already
//! computes. These are the names it publishes, from the inference pool metrics
//! in gateway-api-inference-extension, which llm-d reuses unchanged.
//!
//! Ready pods stand in for capacity: a pod serves one request at a time, so
//! resizing the pool is resizing capacity, and the queue that builds when
//! offered load exceeds it is a real backlog rather than a number in a config.

use axum::{
    Router,
    body::Body,
    http::{Request, Response, StatusCode},
    middleware::from_fn_with_state,
    routing::{get, post},
};
use serde_json::json;

use crate::{AppState, common};

/// Pool name, as the `name` label on every pool metric.
fn pool_name() -> String {
    std::env::var("MOCK_POOL_NAME").unwrap_or_else(|_| "default-pool".to_owned())
}

/// Which naming the pool publishes.
///
/// The endpoint picker renamed these series at v0.9, and consumers carry a
/// fallback to the older names. A harness that can only produce one of the two
/// cannot test the fallback, and a fallback nothing exercises is a fallback
/// nobody knows is broken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Naming {
    /// The v0.9 names, `llm_d_epp_*`.
    Canonical,
    /// The names before the rename, `inference_pool_*`.
    Legacy,
}

/// Read the naming from the environment, defaulting to current.
fn naming() -> Naming {
    match std::env::var("MOCK_EPP_METRICS").as_deref() {
        Ok("legacy") => Naming::Legacy,
        _ => Naming::Canonical,
    }
}

/// Build the llm-d mock router.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/metrics", get(metrics))
        .route("/admin/capacity", post(set_capacity))
        .route("/health", get(common::health_ok))
        .layer(from_fn_with_state(state.clone(), common::inject_provider_header))
        .with_state(state)
}

/// Serve a request through the pool, holding a slot for its service time.
async fn chat_completions(
    axum::extract::State(state): axum::extract::State<AppState>,
    _req: Request<Body>,
) -> Response<Body> {
    state.load.serve().await;
    common::json_response(
        StatusCode::OK,
        &json!({
            "id": "chatcmpl-llmd-001",
            "object": "chat.completion",
            "model": "mock",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "This is a mock response."},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 6, "total_tokens": 16},
        }),
    )
}

/// Pool aggregates, as the endpoint picker publishes them.
///
/// Queue size and running requests are per pod, because that is what "average"
/// means here: the pool's backlog divided across the pods that can work it off.
/// A pool with a deep queue and many pods is in less trouble than one with the
/// same queue and few, and reporting the total would hide that difference.
async fn metrics(axum::extract::State(state): axum::extract::State<AppState>) -> Response<Body> {
    let pods = state.load.capacity().max(1);
    let per_pod = |total: u64| {
        f64::from(u32::try_from(total).unwrap_or(u32::MAX)) / f64::from(u32::try_from(pods).unwrap_or(u32::MAX))
    };
    let body = render(
        naming(),
        &Aggregate {
            name: pool_name(),
            pods,
            queue: per_pod(state.load.waiting()),
            running: per_pod(state.load.running()),
            cache: state.load.utilization(),
        },
    );
    Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(body))
        .unwrap_or_default()
}

/// What the pool currently looks like, before it is given names.
struct Aggregate {
    /// Pool name, the `name` label on every series.
    name: String,
    /// Endpoints ready to serve.
    pods: u64,
    /// Queued requests per endpoint.
    queue: f64,
    /// Running requests per endpoint.
    running: f64,
    /// Cache occupancy, zero to one.
    cache: f64,
}

/// Render the aggregate under one naming.
///
/// Split from the handler so both namings are testable without reaching for an
/// environment variable, which parallel tests in one process cannot own.
fn render(naming: Naming, pool: &Aggregate) -> String {
    let Aggregate {
        name,
        pods,
        queue,
        running,
        cache,
    } = pool;
    match naming {
        Naming::Canonical => format!(
            "llm_d_epp_average_queue_size{{name=\"{name}\"}} {queue}\n\
             llm_d_epp_average_kv_cache_utilization{{name=\"{name}\"}} {cache}\n\
             llm_d_epp_ready_endpoints{{name=\"{name}\"}} {pods}\n"
        ),
        Naming::Legacy => format!(
            "inference_pool_average_queue_size{{name=\"{name}\"}} {queue}\n\
             inference_pool_average_running_requests{{name=\"{name}\"}} {running}\n\
             inference_pool_average_kv_cache_utilization{{name=\"{name}\"}} {cache}\n\
             inference_pool_ready_pods{{name=\"{name}\"}} {pods}\n"
        ),
    }
}

/// Resize the pool, as a scale event or a lost node would.
///
/// Unauthenticated on purpose: this is a harness control, not part of any real
/// API, and one more reason the mock belongs on a compose network only.
async fn set_capacity(
    axum::extract::State(state): axum::extract::State<AppState>,
    axum::extract::Query(params): axum::extract::Query<Vec<(String, String)>>,
) -> Response<Body> {
    let Some(value) = params
        .iter()
        .find(|(key, _)| key == "value")
        .and_then(|(_, raw)| raw.parse::<u64>().ok())
        .filter(|parsed| (1..=4_096).contains(parsed))
    else {
        return common::json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": {"message": "value must be a whole number from 1 to 4096"}}),
        );
    };
    let previous = state.load.resize(value);
    common::json_response(StatusCode::OK, &json!({"previous": previous, "ready_pods": value}))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::to_bytes;
    use tower::ServiceExt as _;

    use super::*;

    fn state(capacity: u64) -> AppState {
        AppState {
            provider_site: Arc::from("test-site"),
            queue_depth: 0.1,
            load: Arc::new(crate::load::Load::new(capacity, 0)),
        }
    }

    async fn body_of(state: AppState) -> String {
        let request = Request::builder()
            .uri("/metrics")
            .body(Body::empty())
            .unwrap_or_default();
        let response = router(state).oneshot(request).await.unwrap_or_default();
        let bytes = to_bytes(response.into_body(), 8192).await.unwrap_or_default();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    #[tokio::test]
    async fn the_pool_publishes_the_canonical_series() {
        let text = body_of(state(4)).await;
        for series in [
            "llm_d_epp_average_queue_size",
            "llm_d_epp_average_kv_cache_utilization",
            "llm_d_epp_ready_endpoints",
        ] {
            assert!(text.contains(series), "{series} is published: {text}");
        }
        assert!(
            !text.contains("inference_pool_"),
            "and the pre-rename names are not published alongside them: {text}"
        );
    }

    fn aggregate() -> Aggregate {
        Aggregate {
            name: "pool-a".to_owned(),
            pods: 4,
            queue: 2.5,
            running: 1.0,
            cache: 0.5,
        }
    }

    #[test]
    fn the_two_namings_carry_the_same_facts() {
        // The rename at v0.9 changed names, not meaning. A consumer falling
        // back to the older names has to read the same pool.
        let canonical = render(Naming::Canonical, &aggregate());
        let legacy = render(Naming::Legacy, &aggregate());

        assert!(canonical.contains(r#"llm_d_epp_average_queue_size{name="pool-a"} 2.5"#));
        assert!(legacy.contains(r#"inference_pool_average_queue_size{name="pool-a"} 2.5"#));
        assert!(canonical.contains(r#"llm_d_epp_ready_endpoints{name="pool-a"} 4"#));
        assert!(legacy.contains(r#"inference_pool_ready_pods{name="pool-a"} 4"#));
    }

    #[test]
    fn neither_naming_leaks_into_the_other() {
        // A consumer that prefers the canonical names and falls back to the
        // legacy ones would never exercise the fallback if both were present.
        assert!(!render(Naming::Canonical, &aggregate()).contains("inference_pool_"));
        assert!(!render(Naming::Legacy, &aggregate()).contains("llm_d_epp_"));
    }

    #[tokio::test]
    async fn every_series_carries_the_pool_name() {
        let text = body_of(state(2)).await;
        let named = text.lines().filter(|line| line.contains(r#"name=""#)).count();
        let total = text.lines().filter(|line| !line.trim().is_empty()).count();
        assert_eq!(named, total, "the label is what joins a series to a pool: {text}");
    }

    #[tokio::test]
    async fn ready_endpoints_follows_capacity() {
        let text = body_of(state(6)).await;
        assert!(
            text.contains(r#"llm_d_epp_ready_endpoints{name="default-pool"} 6"#),
            "resizing the pool is resizing capacity: {text}"
        );
    }

    #[tokio::test]
    async fn an_idle_pool_reports_no_backlog() {
        let text = body_of(state(4)).await;
        assert!(
            text.contains(r#"llm_d_epp_average_queue_size{name="default-pool"} 0"#),
            "nothing offered, nothing queued: {text}"
        );
    }
}
