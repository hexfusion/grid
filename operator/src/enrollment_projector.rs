//! Materializes a `GridSite` for each approved enrollment.
//!
//! The enrollment service is deliberately Kubernetes-free, so it cannot create
//! resources itself. This projector closes that gap from the operator's side: it
//! polls the service for issued enrollments and server-side-applies the `GridSite`
//! the shared `projection` crate derives from each. It owns the spec — the request
//! is the source of truth for identity, egress and trust — while the SWIM path
//! keeps ownership of status and liveness, under a different field manager.

use std::time::Duration;

use http_body_util::{BodyExt as _, Empty};
use hyper::{Method, Request, body::Bytes};
use hyper_util::{client::legacy::Client as HyperClient, rt::TokioExecutor};
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
};
use serde_json::Value;

use crate::crd::grid_site::GridSite;

/// Field manager for the projector's applies.
///
/// Distinct from the SWIM path's manager so the two own disjoint fields: this
/// projector owns the spec derived from the enrollment, the SWIM path owns status.
const FIELD_MANAGER: &str = "grid-enrollment-projector";

/// Any failure during a projection cycle, reported to the log.
type Failure = Box<dyn std::error::Error + Send + Sync>;

/// Where and how to reach the enrollment service.
pub struct Config {
    /// Base URL of the enrollment service, without a trailing slash.
    url: String,
    /// Operator token authorizing the list.
    token: String,
    /// How often to poll for issued enrollments.
    interval: Duration,
}

impl Config {
    /// Read the projector config from the environment.
    ///
    /// Returns `None` when `GRID_ENROLLMENT_URL` is unset, which leaves the
    /// projector off and the operator relying on manually applied `GridSite`s.
    #[must_use]
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("GRID_ENROLLMENT_URL").ok()?;
        let token = std::env::var("GRID_OPERATOR_TOKEN").unwrap_or_default();
        let interval = std::env::var("GRID_ENROLLMENT_POLL_SECS")
            .ok()
            .and_then(|secs| secs.parse().ok())
            .map_or(Duration::from_secs(15), Duration::from_secs);
        Some(Self {
            url: url.trim_end_matches('/').to_owned(),
            token,
            interval,
        })
    }
}

/// Poll the enrollment service forever, projecting issued enrollments into
/// `GridSite`s. Cycle failures are logged and the loop continues.
pub async fn run(client: Client, config: Config) {
    let interval_ms = u64::try_from(config.interval.as_millis()).unwrap_or(u64::MAX);
    tracing::info!(url = %config.url, interval_ms, "starting enrollment projector");
    poll_loop(&client, &config).await;
}

/// Inner polling loop; separated to satisfy clippy's infinite-loop lint.
async fn poll_loop(client: &Client, config: &Config) -> ! {
    loop {
        tokio::time::sleep(config.interval).await;
        if let Err(error) = project_once(client, config).await {
            tracing::warn!(%error, "enrollment projection cycle failed");
        }
    }
}

/// List issued enrollments and apply a `GridSite` for each.
async fn project_once(client: &Client, config: &Config) -> Result<(), Failure> {
    let api: Api<GridSite> = Api::all(client.clone());
    for record in list_issued(config).await? {
        match projection::grid_site(&record) {
            Ok(value) => apply_grid_site(&api, value).await?,
            Err(error) => tracing::warn!(%error, "skipping an enrollment that will not project"),
        }
    }
    Ok(())
}

/// Server-side apply one projected `GridSite`.
///
/// The applied object carries only the fields the projection set (no status), so
/// under this field manager the projector owns the spec and leaves status to SWIM.
async fn apply_grid_site(api: &Api<GridSite>, value: Value) -> Result<(), Failure> {
    let name = value
        .pointer("/metadata/name")
        .and_then(Value::as_str)
        .ok_or("projected GridSite has no name")?
        .to_owned();
    api.patch(&name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(&value))
        .await?;
    tracing::info!(site = %name, "projected GridSite from enrollment");
    Ok(())
}

/// GET the issued enrollments from the service.
async fn list_issued(config: &Config) -> Result<Vec<Value>, Failure> {
    let tls = hyper_rustls::HttpsConnectorBuilder::new()
        .with_native_roots()?
        .https_or_http()
        .enable_http1()
        .build();
    let http: HyperClient<_, Empty<Bytes>> = HyperClient::builder(TokioExecutor::new()).build(tls);

    let mut builder = Request::builder()
        .method(Method::GET)
        .uri(format!("{}/v1/requests?phase=issued", config.url));
    if !config.token.is_empty() {
        builder = builder.header("authorization", format!("Bearer {}", config.token));
    }
    let response = http.request(builder.body(Empty::new())?).await?;
    let bytes = response.into_body().collect().await?.to_bytes();
    let body: Value = serde_json::from_slice(&bytes)?;
    Ok(body.as_array().cloned().unwrap_or_default())
}
