//! The `grid_site_route` filter: pick a cross-site cluster for a request.
//!
//! Reads the model header, chooses a site for that model from the current snapshot
//! by what each provider publishes, and sets `ctx.cluster` for the downstream load
//! balancer. Each site's load, requests held over capacity (rho), is resolved when
//! the snapshot is built, so a request only filters and draws. A request naming no
//! model gets 400, one for an unknown model 404, and one with no healthy site or a
//! shed model 503 with Retry-After and an OpenAI-style error, logged at debug. Every outcome counts in
//! `grid_route_decisions_total` by site and reason. A routed response names the
//! chosen site and cluster in `x-grid-site` and `x-grid-backend`.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use praxis_filter::{FilterAction, FilterError, HttpFilter, HttpFilterContext, TerminalResponse, parse_filter_config};
use serde::Deserialize;

use crate::{
    decisions::{Refused, SiteDecisions},
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate, validate_model_header},
    health::ClusterHealth,
    snapshot::RouteSnapshot,
};

/// Default request header carrying the model name.
fn default_model_header() -> String {
    "X-Model".to_owned()
}

/// `grid_site_route` configuration as written in the praxis filter section.
///
/// Only the model header lives here. The candidate topology and the poller
/// settings live in the grid serving config the gateway reads, and the snapshot
/// is injected, so the data plane parses nothing about the control plane.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GridSiteRouteConfig {
    /// Request header naming the model (default `X-Model`).
    #[serde(default = "default_model_header")]
    model_header: String,

    /// The `load_balancer` clusters this gateway declares. When set, a candidate
    /// with no peer gateway and a cluster not listed has no route and is skipped.
    #[serde(default)]
    clusters: Option<Vec<DeclaredCluster>>,
}

/// How a cluster's endpoints are dialed, as its `load_balancer` cluster declares.
///
/// Parsed and validated but unused: nothing probes endpoints in this filter yet, and a
/// config carrying a transport must still load rather than fail the gateway's start.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TransportKind {
    /// Grid mutual TLS with the site identity.
    MutualTls,
    /// Server-only TLS, verified with the cluster's CA.
    Tls,
    /// No TLS.
    Plaintext,
}

/// One declared cluster: a bare name, or a name with the transport its endpoints are dialed over.
#[derive(Debug)]
enum DeclaredCluster {
    /// Routable, not probed.
    Name(String),
    /// Routable, and carrying the transport its endpoints are dialed over.
    Probed(ProbedCluster),
}

// By hand, not untagged: untagged hides which field or transport was wrong.
impl<'de> Deserialize<'de> for DeclaredCluster {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = DeclaredCluster;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a cluster name or {name, transport}")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(DeclaredCluster::Name(v.to_owned()))
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                ProbedCluster::deserialize(serde::de::value::MapAccessDeserializer::new(map))
                    .map(DeclaredCluster::Probed)
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

/// A declared cluster that names how its remote endpoints are dialed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[expect(
    dead_code,
    reason = "parsed so a config carrying a transport loads; nothing probes yet"
)]
struct ProbedCluster {
    /// The `load_balancer` cluster name.
    name: String,
    /// How the cluster dials its endpoints.
    transport: TransportKind,
    /// The CA a `tls` cluster verifies with.
    #[serde(default)]
    ca_path: Option<String>,
    /// The server name a `tls` cluster expects.
    #[serde(default)]
    sni: Option<String>,
}

impl DeclaredCluster {
    /// The cluster name.
    fn name(&self) -> &str {
        match self {
            Self::Name(name) | Self::Probed(ProbedCluster { name, .. }) => name,
        }
    }
}

/// Routes a request to a cross-site cluster by model, honouring live-load order.
#[derive(Debug)]
pub(crate) struct GridSiteRouteFilter {
    /// The resolved, pre-ordered candidate snapshot the control step swaps. Shared
    /// with the refresh loop, so every request reads the latest ordering.
    snapshot: Arc<ArcSwap<RouteSnapshot>>,

    /// Header the request carries the model name in.
    model_header: http::header::HeaderName,

    /// Counts requests from a random start, so gateway replicas draw independently.
    turn: AtomicUsize,

    /// Where the filter publishes Praxis's health registry for the control step.
    health: Arc<ClusterHealth>,

    /// Declared clusters, `None` when the config does not list them.
    clusters: Option<HashSet<Arc<str>>>,
}

impl GridSiteRouteFilter {
    /// Build the filter from its config section over an injected snapshot.
    ///
    /// The gateway owns `snapshot` and its refresh loop, and the filter only reads it.
    ///
    /// # Errors
    ///
    /// Returns [`FilterError`] if the config fails to parse or the model header is
    /// invalid.
    pub(crate) fn from_config(
        config: &serde_yaml::Value,
        snapshot: Arc<ArcSwap<RouteSnapshot>>,
        health: Arc<ClusterHealth>,
    ) -> Result<Box<dyn HttpFilter>, FilterError> {
        let cfg: GridSiteRouteConfig = parse_filter_config("grid_site_route", config)?;
        let model_header = validate_model_header(&cfg.model_header)?;
        let declared = cfg.clusters;
        tracing::info!(
            "grid_site_route: choosing sites by published in-flight per capacity: two choices among 3 or more \
             sites with room, a weighted draw between 2, and a capacity draw when none has room or none publishes"
        );
        Ok(Box::new(Self {
            snapshot,
            model_header,
            turn: AtomicUsize::new(random_seed()),
            health,
            clusters: declared.map(|clusters| clusters.iter().map(|cluster| Arc::from(cluster.name())).collect()),
        }))
    }

    /// The site for a request for `model`, or the answer when no site can take it.
    fn choose<'snap>(&self, snapshot: &'snap RouteSnapshot, model: &str) -> Result<Pick<'snap>, FilterAction> {
        let turn = self.turn.fetch_add(1, Ordering::Relaxed);
        if snapshot.shedding.contains(model) {
            return Err(refuse(Refused::Shed, model, turn));
        }
        let routable = |candidate: &RouteCandidate| has_route(candidate, self.clusters.as_ref());
        select_spread(
            snapshot,
            CapabilityKind::InferenceModel,
            model,
            turn,
            routable,
            &KeepAll,
        )
        .ok_or_else(|| refuse(unserved(snapshot, model, routable), model, turn))
    }
}

#[async_trait]
impl HttpFilter for GridSiteRouteFilter {
    fn name(&self) -> &'static str {
        "grid_site_route"
    }

    fn selects_cluster(&self) -> bool {
        true
    }

    async fn on_request(&self, ctx: &mut HttpFilterContext<'_>) -> Result<FilterAction, FilterError> {
        self.health.observe(ctx.health_registry);
        // An earlier selecting filter wins; never override its choice.
        if ctx.cluster.is_some() || ctx.upstream.is_some() {
            return Ok(FilterAction::Continue);
        }
        // Borrow the model header out of *ctx.request; that field is disjoint
        // from the ctx.cluster write below, so no owned copy is needed.
        let Some(model) = ctx
            .request
            .headers
            .get(&self.model_header)
            .and_then(|value| value.to_str().ok())
        else {
            // Nothing after this filter selects a cluster, so answer here rather than fail in the load balancer.
            tracing::debug!(path = %ctx.request.uri.path(), "grid_site_route: no model in the request");
            Refused::NoModel.record();
            return Ok(unrouted(400));
        };

        let snapshot = self.snapshot.load();
        match self.choose(&snapshot, model) {
            Ok(pick) => {
                route_to(ctx, &pick);
                Ok(FilterAction::Continue)
            },
            Err(answer) => Ok(answer),
        }
    }
}

/// Send the request to `pick`: count the decision and select its cluster.
fn route_to(ctx: &mut HttpFilterContext<'_>, pick: &Pick<'_>) {
    pick.decisions.record(pick.fallback);
    ctx.cluster = Some(Arc::clone(&pick.candidate.cluster));
}

/// Count and answer a request no candidate took.
fn refuse(reason: Refused, model: &str, turn: usize) -> FilterAction {
    tracing::debug!(model = %model, reason = ?reason, "grid_site_route: no admitted candidate");
    reason.record();
    // Known but excluded everywhere is a temporary outage, not an unknown model.
    match reason {
        Refused::UnknownModel => error_response(
            404,
            "invalid_request_error",
            "model_not_found",
            "no site serves this model",
            None,
        ),
        // Load, not an outage: 429 is what an OpenAI client backs off and retries on.
        Refused::Shed => retry_later(
            turn,
            429,
            "rate_limit_exceeded",
            "capacity_exhausted",
            "every site serving this model is at capacity",
        ),
        Refused::NotReady | Refused::NoRoute | Refused::NoModel => retry_later(
            turn,
            503,
            "server_error",
            "no_healthy_site",
            "no healthy site serves this model now",
        ),
    }
}

/// Why no candidate took a request for `model`: unknown, unroutable, or excluded.
fn unserved(snapshot: &RouteSnapshot, model: &str, routable: impl Fn(&RouteCandidate) -> bool) -> Refused {
    let mut matches = snapshot
        .candidates
        .iter()
        .filter(|candidate| candidate.kind == CapabilityKind::InferenceModel && &*candidate.name == model)
        .peekable();
    if matches.peek().is_none() {
        return Refused::UnknownModel;
    }
    if matches.any(|candidate| is_admitted_for_new_request(candidate.admission_state) && !routable(candidate)) {
        Refused::NoRoute
    } else {
        Refused::NotReady
    }
}

/// The candidate `select_spread` chose and why.
#[derive(Debug)]
pub(crate) struct Pick<'snap> {
    /// The chosen candidate.
    pub(crate) candidate: &'snap RouteCandidate,
    /// Whether it was demoted, chosen only because no healthy site was left.
    pub(crate) fallback: bool,
    /// The chosen site's decision counters.
    pub(crate) decisions: &'snap SiteDecisions,
}

impl<'snap> Pick<'snap> {
    /// `site` as chosen, a `fallback` when no healthy site was left.
    const fn new((candidate, decisions): Site<'snap>, fallback: bool) -> Self {
        Self {
            candidate,
            fallback,
            decisions,
        }
    }
}

/// The router's own answer to a request it cannot route: a response, not a rejection,
/// since praxis logs every rejection at WARN.
fn unrouted(status: u16) -> FilterAction {
    FilterAction::TerminalResponse(Box::new(TerminalResponse::new(status)))
}

/// Seconds a client should wait before retrying a model with no admitted candidate:
/// about one operator scrape plus one peer poll, spread so retries do not arrive together.
const RETRY_AFTER_SECS: [&str; 5] = ["3", "4", "5", "6", "7"];

/// `status` with a Retry-After picked by `turn` and an OpenAI-style error naming `code`.
fn retry_later(turn: usize, status: u16, kind: &str, code: &str, message: &str) -> FilterAction {
    let retry_after = turn
        .checked_rem(RETRY_AFTER_SECS.len())
        .and_then(|index| RETRY_AFTER_SECS.get(index))
        .copied()
        .unwrap_or("5");
    error_response(status, kind, code, message, Some(retry_after))
}

/// A gateway-originated error: `status`, an OpenAI-style error object, and an optional Retry-After.
fn error_response(
    status: u16,
    kind: &str,
    code: &str,
    message: &str,
    retry_after: Option<&'static str>,
) -> FilterAction {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/json"),
    );
    if let Some(retry_after) = retry_after {
        headers.insert(http::header::RETRY_AFTER, http::HeaderValue::from_static(retry_after));
    }
    // Every code and message is a constant here, so none needs JSON escaping.
    let body = format!(r#"{{"error":{{"message":"{message}","type":"{kind}","code":"{code}"}}}}"#);
    FilterAction::TerminalResponse(Box::new(
        TerminalResponse::new(status).with_headers(headers).with_body(body),
    ))
}

/// Whether a request routed to `candidate` has somewhere to go: a declared cluster.
/// With no declared list, every cluster is assumed to exist.
fn has_route(candidate: &RouteCandidate, clusters: Option<&HashSet<Arc<str>>>) -> bool {
    clusters.is_none_or(|clusters| clusters.contains(&candidate.cluster))
}

/// One site the draw may choose, as a [`Narrow`] stage sees it.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by narrowing stages; this crate ships only KeepAll")
)]
pub(crate) struct SiteView<'snap> {
    /// The candidate.
    pub(crate) candidate: &'snap RouteCandidate,
    /// Requests it holds over its capacity, below 1 for every site the draw sees.
    pub(crate) rho: f64,
    /// Expected delay in seconds; `None` until ranking uses latency.
    pub(crate) delay: Option<f64>,
    /// Recent latency its operator publishes, each `None` until enough requests complete.
    pub(crate) latency: crate::descriptor::Latency,
}

/// A per-request stage that narrows the sites with room before the draw.
///
/// It may only clear entries of `keep`, which start all true, so it never adds a site
/// that health, readiness, or shedding removed. Clearing every entry keeps them all.
pub(crate) trait Narrow {
    /// Clear `keep[i]` for each of `sites` the draw should not consider.
    fn keep(&self, sites: &[SiteView<'_>], keep: &mut [bool]);
}

/// Keeps every site: selection by load alone.
pub(crate) struct KeepAll;

impl Narrow for KeepAll {
    fn keep(&self, _sites: &[SiteView<'_>], _keep: &mut [bool]) {}
}

/// Most sites with room one request weighs. Past it, the rest wait for a later snapshot order.
const MAX_SITES: usize = 64;

/// Bounds of either site's share when two sites with room are drawn between.
const TWO_SITE_SHARE: (f64, f64) = (0.1, 0.9);

/// A candidate with its decision counters.
type Site<'snap> = (&'snap RouteCandidate, &'snap SiteDecisions);

/// Up to [`MAX_SITES`] sites, the first `len` filled.
struct Sites<'snap> {
    /// The sites, filled from the front.
    slots: [Option<Site<'snap>>; MAX_SITES],
    /// How many slots are filled.
    len: usize,
}

impl<'snap> Sites<'snap> {
    /// The first [`MAX_SITES`] of `sites`, warning once when more are offered.
    fn gather(sites: impl Iterator<Item = Site<'snap>>) -> Self {
        let mut gathered = Self {
            slots: [None; MAX_SITES],
            len: 0,
        };
        for site in sites {
            let Some(slot) = gathered.slots.get_mut(gathered.len) else {
                warn_past_max_sites();
                break;
            };
            *slot = Some(site);
            gathered.len = gathered.len.saturating_add(1);
        }
        gathered
    }

    /// The filled sites in order.
    fn iter(&self) -> impl Iterator<Item = Site<'snap>> + '_ {
        self.slots.iter().take(self.len).flatten().copied()
    }

    /// The site at `index`, if filled.
    fn get(&self, index: usize) -> Option<Site<'snap>> {
        self.slots.get(..self.len)?.get(index).copied().flatten()
    }
}

/// Choose a site for `name` among the admitted, `routable` matches.
///
/// Among healthy matches with room (rho below 1) that `narrow` keeps: two choices at
/// three or more, drawn by capacity, taking the lower rho; a draw between two, weighted
/// by capacity over 1 + rho with each share clamped to 0.1..0.9; or the only one. With
/// none, a capacity draw over the healthy matches tied on the best polled queue score (a
/// prefix, since scores ascend), which covers sites that publish no rho without herding. With no healthy
/// match, the front demoted one, as a fallback.
#[expect(
    clippy::too_many_arguments,
    reason = "the request path passes its inputs directly rather than build a struct per request"
)]
pub(crate) fn select_spread<'snap>(
    snapshot: &'snap RouteSnapshot,
    kind: CapabilityKind,
    name: &str,
    turn: usize,
    routable: impl Fn(&RouteCandidate) -> bool,
    narrow: &impl Narrow,
) -> Option<Pick<'snap>> {
    let matches = matching(snapshot, kind, name, &routable);
    let (front, _, front_decisions) = matches.clone().next()?;
    let healthy = matches.filter(|(_, score, _)| !score.is_nan());
    let Some((_, best, _)) = healthy.clone().next() else {
        // Demotion scores a candidate NaN: every healthy site is gone, so try the front one.
        return Some(Pick::new((front, front_decisions), true));
    };
    let site = |(candidate, _, decisions): Scored<'snap>| (candidate, decisions);
    let room = Sites::gather(healthy.clone().filter(|(candidate, ..)| has_room(candidate)).map(site));
    let band = || {
        Sites::gather(
            healthy
                .take_while(|(_, score, _)| score.total_cmp(&best).is_eq())
                .map(site),
        )
    };
    draw(&narrowed(&room, narrow), band, turn).map(|chosen| Pick::new(chosen, false))
}

/// A candidate with its score and decision counters.
type Scored<'snap> = (&'snap RouteCandidate, f64, &'snap SiteDecisions);

/// The admitted, `routable` candidates for `kind` and `name`, in snapshot order.
fn matching<'snap, 'req>(
    snapshot: &'snap RouteSnapshot,
    kind: CapabilityKind,
    name: &'req str,
    routable: &'req impl Fn(&RouteCandidate) -> bool,
) -> impl Iterator<Item = Scored<'snap>> + Clone + 'req
where
    'snap: 'req,
{
    snapshot
        .candidates
        .iter()
        .zip(&snapshot.scores)
        .zip(&snapshot.decisions)
        .map(|((candidate, score), decisions)| (candidate, *score, decisions))
        .filter(move |(candidate, ..)| {
            candidate.kind == kind
                && &*candidate.name == name
                && is_admitted_for_new_request(candidate.admission_state)
                && routable(candidate)
        })
}

/// The tier for `room` sites: two choices at three or more, a weighted draw at two, the
/// only one at one, and with none a capacity draw over `band`.
fn draw<'snap>(room: &Sites<'snap>, band: impl FnOnce() -> Sites<'snap>, turn: usize) -> Option<Site<'snap>> {
    match room.len {
        0 => by_capacity(&band(), turn),
        1 => room.get(0),
        2 => Some(between_two(room.get(0)?, room.get(1)?, turn)),
        _ => two_choices(room, turn),
    }
}

/// Whether `candidate` publishes load and has room for one more request.
fn has_room(candidate: &RouteCandidate) -> bool {
    candidate.rho.is_some_and(|rho| rho < 1.0)
}

/// The sites `narrow` keeps of `room`, or all of them when it keeps none.
fn narrowed<'snap>(room: &Sites<'snap>, narrow: &impl Narrow) -> Sites<'snap> {
    let views: [Option<SiteView<'snap>>; MAX_SITES] = std::array::from_fn(|index| {
        room.get(index).map(|(candidate, _)| SiteView {
            candidate,
            rho: candidate.rho.unwrap_or(0.0),
            delay: None,
            latency: candidate.latency,
        })
    });
    let views: Vec<SiteView<'snap>> = views.iter().flatten().copied().collect();
    let mut keep = [true; MAX_SITES];
    if let Some(kept) = keep.get_mut(..room.len) {
        narrow.keep(&views, kept);
    }
    if !keep.iter().take(room.len).any(|kept| *kept) {
        return Sites::gather(room.iter());
    }
    Sites::gather(room.iter().zip(keep).filter(|(_, kept)| *kept).map(|(site, _)| site))
}

/// A capacity draw over `sites`. Unpublished capacity weighs the mean of the published, or 1.
fn by_capacity<'snap>(sites: &Sites<'snap>, turn: usize) -> Option<Site<'snap>> {
    let published: Vec<f64> = sites.iter().filter_map(|(candidate, _)| candidate.capacity).collect();
    let unpublished = if published.is_empty() {
        1.0
    } else {
        published.iter().sum::<f64>() / published.iter().map(|_| 1.0).sum::<f64>()
    };
    weighted(sites, unit(turn, SALTS.0), None, |candidate| {
        candidate.capacity.unwrap_or(unpublished)
    })
}

/// A draw between two sites by capacity over 1 + rho, each share clamped to 0.1..0.9.
fn between_two<'snap>(first: Site<'snap>, second: Site<'snap>, turn: usize) -> Site<'snap> {
    let rate = |candidate: &RouteCandidate| candidate.capacity.unwrap_or(1.0) / (1.0 + candidate.rho.unwrap_or(0.0));
    let share = (rate(first.0) / (rate(first.0) + rate(second.0))).clamp(TWO_SITE_SHARE.0, TWO_SITE_SHARE.1);
    if unit(turn, SALTS.0) < share { first } else { second }
}

/// Two distinct draws from `sites` by capacity, taking the lower rho; exact ties go to the first.
fn two_choices<'snap>(sites: &Sites<'snap>, turn: usize) -> Option<Site<'snap>> {
    let capacity = |candidate: &RouteCandidate| candidate.capacity.unwrap_or(1.0);
    let first = weighted(sites, unit(turn, SALTS.0), None, capacity)?;
    let second = weighted(sites, unit(turn, SALTS.1), Some(first.0), capacity).unwrap_or(first);
    Some(if second.0.rho < first.0.rho { second } else { first })
}

/// The site `draw` (in 0..1) lands on when `sites` are weighted by `weight`, skipping `skip`.
fn weighted<'snap>(
    sites: &Sites<'snap>,
    draw: f64,
    skip: Option<&RouteCandidate>,
    weight: impl Fn(&RouteCandidate) -> f64,
) -> Option<Site<'snap>> {
    let eligible = || {
        sites
            .iter()
            .filter(|(candidate, _)| skip.is_none_or(|skip| !std::ptr::eq(*candidate, skip)))
    };
    let mut target = draw * eligible().map(|(candidate, _)| weight(candidate)).sum::<f64>();
    let mut last = None;
    for site in eligible() {
        last = Some(site);
        target -= weight(site.0);
        if target < 0.0 {
            return Some(site);
        }
    }
    last
}

/// Salts that make a request's two draws independent.
const SALTS: (u64, u64) = (0x9E37_79B9_7F4A_7C15, 0xC2B2_AE3D_27D4_EB4F);

/// A uniform value in [0, 1) from `turn` and `salt`, mixed so consecutive turns do not correlate.
fn unit(turn: usize, salt: u64) -> f64 {
    let mut mixed = u64::try_from(turn).unwrap_or(u64::MAX) ^ salt;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^= mixed >> 31;
    // The top 53 bits as a fraction, built from two exact halves.
    let bits = mixed >> 11;
    let high = u32::try_from(bits >> 21).unwrap_or(0);
    let low = u32::try_from(bits & 0x1F_FFFF).unwrap_or(0);
    (f64::from(high) * 2_f64.powi(21) + f64::from(low)) / 2_f64.powi(53)
}

/// A random starting turn, so replicas started together do not draw in step.
fn random_seed() -> usize {
    use std::hash::BuildHasher as _;
    let hashed = std::collections::hash_map::RandomState::new().hash_one(std::time::SystemTime::now());
    usize::try_from(hashed).unwrap_or_else(|_| usize::try_from(hashed >> 32).unwrap_or(0))
}

/// Log once that a model has more sites than one request weighs.
fn warn_past_max_sites() {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        tracing::warn!(
            max = MAX_SITES,
            "grid_site_route: more sites than one request weighs; the rest wait for a later order"
        );
    });
}

/// Whether a candidate in this admission state accepts a new request.
fn is_admitted_for_new_request(state: AdmissionState) -> bool {
    matches!(state, AdmissionState::NewAndExisting)
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    clippy::type_complexity,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    reason = "tests"
)]
mod tests {
    use super::*;
    use crate::descriptor::{CandidateConfig, validate_candidates};

    /// The pick for `kind` and `name` over `candidates` in their given order.
    fn front(candidates: Vec<RouteCandidate>, kind: CapabilityKind, name: &str) -> Option<String> {
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("east"));
        select_spread(&snapshot, kind, name, 0, |_| true, &KeepAll).map(|pick| pick.candidate.cluster.to_string())
    }

    /// A validated one-candidate list for `model` at `site`/`cluster`.
    fn one(model: &str, site: &str, cluster: &str, admission: AdmissionState) -> Vec<RouteCandidate> {
        let mut candidates = validate_candidates(vec![CandidateConfig {
            admission: AdmissionState::default(),
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: model.to_owned(),
            site: site.to_owned(),
        }])
        .unwrap();
        candidates[0].admission_state = admission;
        candidates
    }

    /// Three tied candidates for `m`, at sites a, b and d.
    fn three() -> Vec<RouteCandidate> {
        ["a", "b", "d"]
            .iter()
            .flat_map(|site| one("m", site, &format!("pool-{site}"), AdmissionState::NewAndExisting))
            .collect()
    }

    fn pick(snapshot: &RouteSnapshot, turn: usize) -> String {
        select_spread(snapshot, CapabilityKind::InferenceModel, "m", turn, |_| true, &KeepAll)
            .map(|chosen| chosen.candidate.cluster.to_string())
            .unwrap()
    }

    #[test]
    fn tied_candidates_split_evenly() {
        let snapshot = RouteSnapshot::from_static(three(), Arc::from("hub"));
        let picks: Vec<String> = (0..3_000).map(|turn| pick(&snapshot, turn)).collect();
        for cluster in ["pool-a", "pool-b", "pool-d"] {
            let taken = picks.iter().filter(|p| *p == cluster).count();
            assert!((850..=1_150).contains(&taken), "{cluster} took {taken} of 3000");
        }
    }

    #[test]
    fn a_declared_cluster_is_a_name_or_a_name_with_its_transport() {
        let cfg: GridSiteRouteConfig = serde_yaml::from_str(
            "clusters:\n  - site-a\n  - name: east\n    transport: tls\n    ca_path: /etc/ca.crt\n    sni: east.svc\n  - name: west\n    transport: plaintext\n",
        )
        .expect("a config carrying transports still loads");
        let declared = cfg.clusters.expect("declared");
        let names: Vec<&str> = declared.iter().map(DeclaredCluster::name).collect();
        assert_eq!(names, ["site-a", "east", "west"], "every declared cluster is routable");
        let unknown = serde_yaml::from_str::<GridSiteRouteConfig>("clusters:\n  - name: east\n    transport: quic\n")
            .expect_err("an unknown transport is rejected");
        assert!(unknown.to_string().contains("quic"), "{unknown}");
        assert!(
            serde_yaml::from_str::<GridSiteRouteConfig>("clusters:\n  - name: east\n    transport: tls\n    ca: /x\n")
                .is_err(),
            "an unknown key is rejected"
        );
    }

    #[test]
    fn a_candidate_with_no_peer_gateway_and_no_declared_cluster_has_no_route() {
        let mut candidates = one("llama", "site-d", "site-d", AdmissionState::NewAndExisting);
        candidates.extend(one("llama", "dagobah", "site-a", AdmissionState::NewAndExisting));
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("dagobah"));
        let declared: HashSet<Arc<str>> = HashSet::from([Arc::from("site-a")]);
        for turn in 0..4 {
            let picked = select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "llama",
                turn,
                |c| has_route(c, Some(&declared)),
                &KeepAll,
            )
            .expect("site-a is routable")
            .candidate;
            assert_eq!(
                &*picked.cluster, "site-a",
                "site-d is not a declared cluster, turn {turn}"
            );
        }
        assert!(
            snapshot.candidates.iter().all(|c| has_route(c, None)),
            "with no declared list, every cluster is assumed to exist"
        );
        let undeclared = one("llama", "site-d", "site-d", AdmissionState::NewAndExisting).remove(0);
        assert!(
            !has_route(&undeclared, Some(&declared)),
            "a cluster the config never declared is no route"
        );
    }

    /// Whether the pick at `turn` for `llama` over `snapshot` was a fallback.
    fn fallback(snapshot: &RouteSnapshot, turn: usize) -> Option<bool> {
        select_spread(
            snapshot,
            CapabilityKind::InferenceModel,
            "llama",
            turn,
            |_| true,
            &KeepAll,
        )
        .map(|pick| pick.fallback)
    }

    #[test]
    fn a_pick_is_a_fallback_only_when_no_healthy_site_is_left() {
        assert_eq!(fallback(&two_sites(Some(1.0), Some(5.0)), 0), Some(false));
        assert_eq!(fallback(&two_sites(Some(2.0), Some(2.0)), 1), Some(false));
        let alone = RouteSnapshot::from_static(
            one("llama", "east", "pool-a", AdmissionState::NewAndExisting),
            Arc::from("east"),
        );
        assert_eq!(fallback(&alone, 0), Some(false));
        let down = std::collections::BTreeSet::from([Arc::from("pool-a"), Arc::from("pool-b")]);
        assert_eq!(fallback(&two_sites(Some(1.0), Some(5.0)).demote(&down), 0), Some(true));
        // One cluster left healthy is a routed pick, not a fallback.
        let one = std::collections::BTreeSet::from([Arc::from("pool-a")]);
        assert_eq!(fallback(&two_sites(Some(1.0), Some(5.0)).demote(&one), 0), Some(false));
    }

    #[test]
    fn an_unserved_request_names_why() {
        let mut candidates = one("llama", "east", "pool-a", AdmissionState::Excluded);
        candidates.extend(one("llama", "west", "pool-b", AdmissionState::NewAndExisting));
        let snapshot = RouteSnapshot::from_static(candidates, Arc::from("east"));
        assert_eq!(unserved(&snapshot, "granite", |_| true), Refused::UnknownModel);
        assert_eq!(unserved(&snapshot, "llama", |_| false), Refused::NoRoute);
        let excluded = RouteSnapshot::from_static(
            one("llama", "east", "pool-a", AdmissionState::Excluded),
            Arc::from("east"),
        );
        assert_eq!(unserved(&excluded, "llama", |_| false), Refused::NotReady);
    }

    #[test]
    fn front_match_is_selected() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert_eq!(
            front(candidates, CapabilityKind::InferenceModel, "llama").as_deref(),
            Some("pool-a")
        );
    }

    #[test]
    fn a_different_model_does_not_match() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert!(front(candidates, CapabilityKind::InferenceModel, "granite").is_none());
    }

    #[test]
    fn an_excluded_candidate_is_skipped() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::Excluded);
        assert!(front(candidates, CapabilityKind::InferenceModel, "llama").is_none());
    }

    #[test]
    fn an_mcp_kind_does_not_match_an_inference_query() {
        let candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        assert!(front(candidates, CapabilityKind::McpTool, "llama").is_none());
    }

    /// A snapshot of `llama` on east and west, with the given queue depths.
    fn two_sites(east: Option<f64>, west: Option<f64>) -> RouteSnapshot {
        let store = grid_signals::LoadStore::new(std::time::Duration::from_secs(60));
        for (site, cluster, load) in [("east", "pool-a", east), ("west", "pool-b", west)] {
            if let Some(value) = load {
                let line = format!(
                    r#"{}{{grid_site="{site}",grid_provider="{cluster}"}} {value} 1000"#,
                    crate::snapshot::LOAD_METRIC
                );
                store.ingest_at(&line, 1_000, 1_000, site);
            }
        }
        let mut candidates = one("llama", "east", "pool-a", AdmissionState::NewAndExisting);
        candidates.extend(one("llama", "west", "pool-b", AdmissionState::NewAndExisting));
        RouteSnapshot::from_store(candidates, Arc::from("east"), &store, 1_000, 30_000)
    }

    fn picks(snapshot: &RouteSnapshot) -> Vec<String> {
        (0..2_000)
            .map(|turn| {
                let chosen = select_spread(
                    snapshot,
                    CapabilityKind::InferenceModel,
                    "llama",
                    turn,
                    |_| true,
                    &KeepAll,
                )
                .unwrap()
                .candidate;
                chosen.site.to_string()
            })
            .collect()
    }

    #[test]
    fn equal_scores_spread_across_the_tied_candidates() {
        for (snapshot, case) in [
            (two_sites(None, None), "unmeasured"),
            (two_sites(Some(5.0), Some(5.0)), "measured"),
        ] {
            let east = picks(&snapshot).iter().filter(|site| *site == "east").count();
            assert!((850..=1_150).contains(&east), "{case}: east took {east} of 2000");
        }
    }

    #[test]
    fn a_better_score_always_wins() {
        assert_eq!(picks(&two_sites(Some(9.0), Some(1.0))), ["west"; 2_000]);
        assert_eq!(
            picks(&two_sites(Some(1.0), None)),
            ["east"; 2_000],
            "measured beats unmeasured"
        );
    }

    #[test]
    fn an_excluded_tie_is_never_picked() {
        let mut snapshot = two_sites(None, None);
        snapshot.candidates[1].admission_state = AdmissionState::Excluded;
        assert_eq!(picks(&snapshot), ["east"; 2_000]);
    }

    /// Sites publishing (in flight, capacity), each `None` to publish nothing, as one snapshot.
    fn loaded(sites: &[(&str, Option<f64>, Option<f64>)]) -> RouteSnapshot {
        let store = grid_signals::LoadStore::new(std::time::Duration::from_secs(60));
        let mut candidates = Vec::new();
        for (site, in_flight, capacity) in sites {
            let cluster = format!("pool-{site}");
            let labels = format!(r#"grid_site="{site}",grid_provider="{cluster}""#);
            let saturation = in_flight.zip(*capacity).map(|(held, capacity)| held / capacity);
            for (metric, value) in [
                ("grid_provider_saturation_ratio", saturation),
                ("grid_provider_capacity_requests", *capacity),
            ] {
                if let Some(value) = value {
                    store.ingest_at(&format!("{metric}{{{labels}}} {value} 1000"), 1_000, 1_000, site);
                }
            }
            candidates.extend(one("llama", site, &cluster, AdmissionState::NewAndExisting));
        }
        RouteSnapshot::from_store(candidates, Arc::from("hub"), &store, 1_000, 30_000)
    }

    /// How many of `draws` requests each site took.
    fn shares(
        snapshot: &RouteSnapshot,
        draws: usize,
        narrow: &impl Narrow,
    ) -> std::collections::BTreeMap<String, usize> {
        let mut taken = std::collections::BTreeMap::new();
        for turn in 0..draws {
            let chosen = select_spread(
                snapshot,
                CapabilityKind::InferenceModel,
                "llama",
                turn,
                |_| true,
                narrow,
            )
            .unwrap();
            *taken.entry(chosen.candidate.site.to_string()).or_insert(0) += 1;
        }
        taken
    }

    #[test]
    fn two_choices_prefer_the_lighter_site_and_never_a_full_one() {
        let snapshot = loaded(&[
            ("a", Some(2.0), Some(10.0)),
            ("b", Some(8.0), Some(10.0)),
            ("c", Some(5.0), Some(10.0)),
            ("d", Some(10.0), Some(10.0)),
        ]);
        let taken = shares(&snapshot, 3_000, &KeepAll);
        assert_eq!(taken.get("d"), None, "rho 1 has no room");
        let (a, c) = (taken["a"], taken["c"]);
        assert!(a > c && c > 0, "the lighter site takes more: a {a}, c {c}");
        assert_eq!(
            taken.get("b"),
            None,
            "two distinct draws never choose the heaviest of three"
        );
    }

    #[test]
    fn two_sites_split_by_capacity_over_load_within_a_tenth_and_nine_tenths() {
        let even = shares(
            &loaded(&[("a", Some(1.0), Some(10.0)), ("b", Some(1.0), Some(10.0))]),
            4_000,
            &KeepAll,
        );
        assert!(
            (1_800..=2_200).contains(&even["a"]),
            "equal sites split evenly: {even:?}"
        );
        let lopsided = shares(
            &loaded(&[("a", Some(0.0), Some(100.0)), ("b", Some(9.0), Some(10.0))]),
            4_000,
            &KeepAll,
        );
        assert!(
            (280..=520).contains(&lopsided["b"]),
            "the clamp keeps a tenth for the slow site: {lopsided:?}"
        );
    }

    #[test]
    fn a_lone_site_with_room_takes_every_request() {
        let snapshot = loaded(&[("a", Some(12.0), Some(10.0)), ("b", Some(3.0), Some(10.0))]);
        assert_eq!(shares(&snapshot, 200, &KeepAll).get("b"), Some(&200));
    }

    #[test]
    fn with_no_room_anywhere_the_overflow_draws_by_capacity() {
        let snapshot = loaded(&[("a", Some(30.0), Some(30.0)), ("b", Some(10.0), Some(10.0))]);
        let taken = shares(&snapshot, 4_000, &KeepAll);
        assert!(
            (2_700..=3_300).contains(&taken["a"]),
            "a has three quarters of the capacity: {taken:?}"
        );
    }

    #[test]
    fn sites_without_published_capacity_are_drawn_evenly() {
        let snapshot = loaded(&[("a", Some(5.0), None), ("b", Some(1.0), None), ("c", None, None)]);
        let taken = shares(&snapshot, 3_000, &KeepAll);
        for site in ["a", "b", "c"] {
            assert!((850..=1_150).contains(&taken[site]), "no rho, no load input: {taken:?}");
        }
    }

    #[test]
    fn a_demoted_site_is_never_drawn_while_another_is_healthy() {
        let down = std::collections::BTreeSet::from([Arc::from("pool-a")]);
        let snapshot = loaded(&[
            ("a", Some(0.0), Some(100.0)),
            ("b", Some(5.0), Some(10.0)),
            ("c", Some(5.0), Some(10.0)),
        ])
        .demote(&down);
        assert_eq!(shares(&snapshot, 500, &KeepAll).get("a"), None);
    }

    /// Keeps only site `b`, or clears everything when `all` is set.
    struct Only {
        all: bool,
    }

    impl Narrow for Only {
        fn keep(&self, sites: &[SiteView<'_>], keep: &mut [bool]) {
            for (site, kept) in sites.iter().zip(keep) {
                *kept = !self.all
                    && &*site.candidate.site == "b"
                    && site.rho < 1.0
                    && site.delay.is_none()
                    && site.latency.ttft_p90.is_none();
            }
        }
    }

    #[test]
    fn a_narrowing_stage_can_only_remove_sites_and_clearing_all_keeps_all() {
        let snapshot = loaded(&[
            ("a", Some(1.0), Some(10.0)),
            ("b", Some(5.0), Some(10.0)),
            ("c", Some(12.0), Some(10.0)),
        ]);
        assert_eq!(shares(&snapshot, 300, &Only { all: false }).get("b"), Some(&300));
        let taken = shares(&snapshot, 300, &Only { all: true });
        assert_eq!(taken.get("c"), None, "a full site is never added back");
        assert!(taken.contains_key("a") && taken.contains_key("b"), "{taken:?}");
    }

    #[test]
    fn a_shed_answers_429_and_an_outage_503_each_with_an_openai_error() {
        // Load: an OpenAI client backs off on 429 and retries.
        let shed = answered(Refused::Shed);
        assert_eq!(shed.0, 429);
        assert!(shed.1, "a shed says when to come back");
        assert!(
            shed.2.contains(r#""code":"capacity_exhausted""#) && shed.2.contains(r#""type":"rate_limit_exceeded""#),
            "{}",
            shed.2
        );
        // An outage is not load, so it stays 503.
        let outage = answered(Refused::NotReady);
        assert_eq!(outage.0, 503);
        assert!(outage.1);
        assert!(
            outage.2.contains(r#""code":"no_healthy_site""#) && outage.2.contains(r#""type":"server_error""#),
            "{}",
            outage.2
        );
        // An unknown model is the caller's error, with nothing to retry.
        let unknown = answered(Refused::UnknownModel);
        assert_eq!(unknown.0, 404);
        assert!(!unknown.1, "a model that does not exist will not appear");
        assert!(unknown.2.contains(r#""code":"model_not_found""#), "{}", unknown.2);
    }

    /// How `reason` is answered: status, whether it says when to retry, and its body.
    fn answered(reason: Refused) -> (u16, bool, String) {
        match refuse(reason, "llama", 0) {
            FilterAction::TerminalResponse(response) => {
                let body = String::from_utf8(response.body.clone().unwrap_or_default().to_vec()).unwrap();
                (
                    response.status,
                    response.headers.contains_key(http::header::RETRY_AFTER),
                    body,
                )
            },
            FilterAction::Continue
            | FilterAction::Reject(_)
            | FilterAction::StreamingTerminalResponse(_)
            | FilterAction::Release
            | FilterAction::BodyDone => panic!("a refusal is answered here"),
        }
    }

    #[test]
    fn draws_are_uniform_and_seeds_differ() {
        let mean = (0..10_000).map(|turn| unit(turn, SALTS.0)).sum::<f64>() / 10_000.0;
        assert!((0.48..=0.52).contains(&mean), "{mean}");
        assert!((0..10_000).all(|turn| (0.0..1.0).contains(&unit(turn, SALTS.1))));
        assert_ne!(random_seed(), random_seed());
    }
}
