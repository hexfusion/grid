//! The route snapshot the request path reads, and how live load orders it.
//!
//! A snapshot is a pre-ordered candidate list plus the local site. The request
//! path loads one snapshot and takes the front admitted match, doing no store
//! read or scoring itself. Ordering by live load is a control step
//! ([`RouteSnapshot::from_store`]) run off the request path, so the hot path
//! snapshots resolved order once rather than reading raw signals per request.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use grid_signals::LoadStore;

use crate::{
    decisions::SiteDecisions,
    descriptor::{AdmissionState, CapabilityKind, RouteCandidate},
};

/// Load metrics that order candidates, current EPP name first. Lower queue depth is a better target.
///
/// The wire carries each backend's raw metric name, so the consumer maps every name it knows.
pub(crate) const LOAD_METRICS: [&str; 2] = ["llm_d_epp_average_queue_size", "inference_pool_average_queue_size"];

/// The load metric current llm-d EPPs export.
#[cfg(test)]
pub(crate) const LOAD_METRIC: &str = LOAD_METRICS[0];

/// The requests a provider runs at once, as its operator publishes it.
const CAPACITY_METRIC: &str = "grid_provider_capacity_requests";

/// Requests a provider holds over its capacity, as its operator resolves it.
const SATURATION_METRIC: &str = "grid_provider_saturation_ratio";

/// Recent latency a provider's operator publishes: TTFT median and 90th percentile, time per
/// output token, and prefill seconds per uncached token.
const LATENCY_METRICS: [&str; 4] = [
    "grid_provider_ttft_p50_seconds",
    "grid_provider_ttft_p90_seconds",
    "grid_provider_tpot_seconds",
    "grid_provider_prefill_seconds_per_token",
];

/// A model sheds once every healthy site serving it holds at least this rho.
const SHED_AT: f64 = 1.05;

/// A shedding model routes again once any healthy site is at or below this rho.
const RESUME_AT: f64 = 0.95;

/// The operator's per-provider verdict: 0 when the provider cannot serve.
///
/// Absent reads as ready, so a site whose operator predates it is still routed.
const READY_METRIC: &str = "grid_provider_ready";

/// Lower load is a better routing target.
const LOWER_IS_BETTER: bool = true;

/// Score of a candidate whose cluster has no healthy endpoint: after every healthy one,
/// unmeasured included, since `total_cmp` orders a positive NaN above infinity.
const UNHEALTHY: f64 = f64::NAN.abs();

/// A resolved, pre-ordered candidate list read atomically per request.
#[derive(Debug)]
pub struct RouteSnapshot {
    /// Candidates in priority order. `select_admitted` takes the front match.
    pub candidates: Vec<RouteCandidate>,

    /// This gateway's own site identifier.
    pub local_site: Arc<str>,

    /// Each candidate's score, parallel to `candidates`; equal scores share traffic.
    pub scores: Vec<f64>,

    /// Each candidate's site decision counters, parallel to `candidates`.
    pub decisions: Vec<SiteDecisions>,

    /// Models answering 503 because every healthy site serving them is past full.
    pub shedding: BTreeSet<Arc<str>>,
}

impl RouteSnapshot {
    /// Wrap candidates in their given order, without consulting live load.
    ///
    /// The order is whatever the caller supplies (config order). Used before
    /// any signals exist and as the cold-start fallback.
    pub fn from_static(candidates: Vec<RouteCandidate>, local_site: Arc<str>) -> Self {
        let scores = vec![f64::INFINITY; candidates.len()];
        Self {
            decisions: decisions_for(&candidates),
            candidates,
            local_site,
            scores,
            shedding: BTreeSet::new(),
        }
    }

    /// Order candidates least-loaded-first from the live store, then wrap them.
    ///
    /// For each candidate the store is read at its `site/cluster` key over the
    /// last `window_ms`. A candidate with no fresh sample sorts after every
    /// candidate that has one, so a measured-healthy site is preferred over an
    /// unmeasured one; among equals the caller's order is preserved (stable
    /// sort), which keeps cold start deterministic. `select_admitted` over the
    /// result then picks the least-loaded admitted site per capability.
    pub fn from_store(
        candidates: Vec<RouteCandidate>,
        local_site: Arc<str>,
        store: &LoadStore,
        now_ms: i64,
        window_ms: i64,
    ) -> Self {
        // Score each candidate once, then sort the pairs: load_of allocates a
        // store key and scans a window, too costly to repeat inside sort_by.
        let ranked = candidates
            .into_iter()
            .map(|mut candidate| {
                if Self::reported_unready(store, &candidate) {
                    candidate.admission_state = AdmissionState::Excluded;
                }
                candidate.capacity = Self::capacity_of(store, &candidate);
                candidate.rho = Self::fresh(store, &candidate, SATURATION_METRIC, now_ms, window_ms);
                let [ttft_p50, ttft_p90, tpot, prefill] =
                    LATENCY_METRICS.map(|metric| Self::fresh(store, &candidate, metric, now_ms, window_ms));
                candidate.latency = crate::descriptor::Latency {
                    ttft_p50,
                    ttft_p90,
                    tpot,
                    prefill_per_token: prefill,
                };
                let load = if candidate.admission_state == AdmissionState::NewAndExisting {
                    Self::load_of(store, &candidate, now_ms, window_ms)
                } else {
                    UNHEALTHY
                };
                (load, candidate)
            })
            .collect();
        Self::ranked(ranked, local_site)
    }

    /// The candidate's published capacity, `None` when none is published or it is not a positive count.
    fn capacity_of(store: &LoadStore, candidate: &RouteCandidate) -> Option<f64> {
        let key = LoadStore::key(&candidate.site, &candidate.cluster);
        store
            .latest(&key, CAPACITY_METRIC)
            .map(|sample| sample.value)
            .filter(|capacity| capacity.is_finite() && *capacity >= 1.0)
    }

    /// The candidate's latest `metric` sample, `None` when absent or older than `window_ms`.
    fn fresh(store: &LoadStore, candidate: &RouteCandidate, metric: &str, now_ms: i64, window_ms: i64) -> Option<f64> {
        let key = LoadStore::key(&candidate.site, &candidate.cluster);
        store
            .latest(&key, metric)
            .filter(|sample| now_ms.saturating_sub(sample.at_ms) <= window_ms && sample.value.is_finite())
            .map(|sample| sample.value.max(0.0))
    }

    /// This snapshot with the models to shed, given the set `previous` shed.
    ///
    /// A model sheds when every admitted, healthy site serving it has a fresh rho of at
    /// least 1.05, and routes again once one reaches 0.95. Between the two it keeps its
    /// previous state, so a value near the threshold does not flap. A model with any
    /// healthy site of unknown rho never sheds: that site takes the overflow.
    #[must_use]
    pub(crate) fn shed(mut self, previous: &BTreeSet<Arc<str>>) -> Self {
        // Per model: whether every healthy site is measured, and their rho.
        let mut models: BTreeMap<&Arc<str>, (bool, Vec<f64>)> = BTreeMap::new();
        let healthy = self.candidates.iter().zip(&self.scores).filter(|(candidate, score)| {
            candidate.kind == CapabilityKind::InferenceModel
                && candidate.admission_state == AdmissionState::NewAndExisting
                && !score.is_nan()
        });
        for (candidate, _) in healthy {
            let (measured, loads) = models.entry(&candidate.name).or_insert((true, Vec::new()));
            match candidate.rho {
                Some(rho) => loads.push(rho),
                None => *measured = false,
            }
        }
        self.shedding = models
            .into_iter()
            .filter(|(model, (measured, loads))| {
                let full = loads.iter().all(|rho| *rho >= SHED_AT);
                let room = loads.iter().any(|rho| *rho <= RESUME_AT);
                *measured && !loads.is_empty() && (full || (previous.contains(*model) && !room))
            })
            .map(|(model, _)| Arc::clone(model))
            .collect();
        self
    }

    /// Whether the candidate's latest readiness sample says it cannot serve.
    ///
    /// The latest at any age, not the worst in a window: a recovered provider rejoins
    /// on its next poll, and a partitioned peer keeps its last 0 rather than reading as
    /// ready once it ages out. A 0 stands until something newer arrives for the
    /// provider: a later reading without the series means its peer stopped publishing
    /// readiness, which reads as ready, like a peer that never published it.
    fn reported_unready(store: &LoadStore, candidate: &RouteCandidate) -> bool {
        let key = LoadStore::key(&candidate.site, &candidate.cluster);
        store
            .latest(&key, READY_METRIC)
            .is_some_and(|ready| ready.value < 1.0 && store.newest_at(&key).is_none_or(|newest| newest <= ready.at_ms))
    }

    /// This snapshot with every candidate on a `down` cluster ordered after all the rest.
    ///
    /// Last rather than dropped: while any candidate is healthy it is never chosen, and
    /// when none is, the request still has somewhere to go.
    #[must_use]
    pub(crate) fn demote(self, down: &BTreeSet<Arc<str>>) -> Self {
        self.demote_where(down, |candidate| Some(&candidate.cluster))
    }

    /// This snapshot with every candidate whose `key` is in `down` ordered last.
    fn demote_where(self, down: &BTreeSet<Arc<str>>, key: impl Fn(&RouteCandidate) -> Option<&Arc<str>>) -> Self {
        if down.is_empty() {
            return self;
        }
        let ranked = self
            .scores
            .into_iter()
            .zip(self.candidates)
            .map(|(load, candidate)| {
                let demoted = if key(&candidate).is_some_and(|key| down.contains(key)) {
                    UNHEALTHY
                } else {
                    load
                };
                (demoted, candidate)
            })
            .collect();
        Self::ranked(ranked, self.local_site)
    }

    /// This snapshot, after setting `grid_route_site_score` for each of its site/cluster pairs.
    ///
    /// The gauge is the score the order used, NaN when the candidate is excluded or
    /// demoted. A pair in `published` but no longer in the topology is set to NaN,
    /// since the exporter keeps a series until restart. `published` becomes this
    /// snapshot's pairs.
    #[must_use]
    pub(crate) fn published(self, published: &mut Gauged) -> Self {
        let mut current = BTreeSet::new();
        for (candidate, score) in self.candidates.iter().zip(&self.scores) {
            let key = (Arc::clone(&candidate.site), Arc::clone(&candidate.cluster));
            // The front entry for a pair is its best; later ones are other models.
            if current.insert(key.clone()) {
                site_score(&key, *score);
            }
        }
        for gone in published.difference(&current) {
            site_score(gone, f64::NAN);
        }
        *published = current;
        self
    }

    /// Sort `ranked` ascending by score, stable among equals, and wrap it.
    fn ranked(mut ranked: Vec<(f64, RouteCandidate)>, local_site: Arc<str>) -> Self {
        ranked.sort_by(|(left, _), (right, _)| left.total_cmp(right));
        let (scores, ordered): (Vec<f64>, Vec<RouteCandidate>) = ranked.into_iter().unzip();
        Self {
            decisions: decisions_for(&ordered),
            candidates: ordered,
            local_site,
            scores,
            shedding: BTreeSet::new(),
        }
    }

    /// The candidate's worst recent load, or `+inf` when it has no fresh sample.
    ///
    /// `+inf` makes an unmeasured candidate sort last under an ascending order.
    fn load_of(store: &LoadStore, candidate: &RouteCandidate, now_ms: i64, window_ms: i64) -> f64 {
        let key = LoadStore::key(&candidate.site, &candidate.cluster);
        LOAD_METRICS
            .iter()
            .find_map(|metric| store.window_worst(&key, metric, now_ms, window_ms, LOWER_IS_BETTER))
            .unwrap_or(f64::INFINITY)
    }
}

/// Decision counters for each of `candidates`, in order.
fn decisions_for(candidates: &[RouteCandidate]) -> Vec<SiteDecisions> {
    candidates
        .iter()
        .map(|candidate| SiteDecisions::new(&candidate.site))
        .collect()
}

/// A candidate's site and cluster, the labels of its score gauge.
type SitePair = (Arc<str>, Arc<str>);

/// The site/cluster pairs with a published score gauge.
pub(crate) type Gauged = BTreeSet<SitePair>;

/// Set `grid_route_site_score` for one site/cluster pair.
fn site_score((site, cluster): &SitePair, score: f64) {
    metrics::gauge!("grid_route_site_score", "site" => Arc::clone(site), "cluster" => Arc::clone(cluster)).set(score);
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::descriptor::{CandidateConfig, validate_candidates};

    /// One QUEUE sample for `site`/`cluster` at `at_ms`.
    fn line(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{LOAD_METRIC}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    /// A validated candidate for model `name` served by `site`/`cluster`.
    fn cand(name: &str, site: &str, cluster: &str) -> CandidateConfig {
        CandidateConfig {
            admission: AdmissionState::default(),
            cluster: cluster.to_owned(),
            credential: None,
            fresh: true,
            kind: CapabilityKind::InferenceModel,
            name: name.to_owned(),
            site: site.to_owned(),
        }
    }

    #[test]
    fn a_cluster_with_no_healthy_endpoint_is_never_chosen_while_another_is_healthy() {
        let store = LoadStore::new(Duration::from_secs(60));
        // site-a is the least loaded but down; site-d is unmeasured.
        store.ingest_at(&line("local", "site-a", 0.0, 1_000), 1_000, 1_000, "local");
        store.ingest_at(&line("local", "site-b", 50.0, 1_000), 1_000, 1_000, "local");
        let candidates = validate_candidates(vec![
            cand("m", "local", "site-a"),
            cand("m", "local", "site-b"),
            cand("m", "site-d", "site-d"),
        ])
        .unwrap();
        let down = BTreeSet::from([Arc::from("site-a")]);
        let snapshot = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 60_000).demote(&down);
        let order: Vec<&str> = snapshot.candidates.iter().map(|c| &*c.cluster).collect();
        assert_eq!(order, ["site-b", "site-d", "site-a"], "down sorts after unmeasured");
        for turn in 0..10 {
            let picked = crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                turn,
                |_| true,
                &crate::route::KeepAll,
            )
            .unwrap()
            .candidate;
            assert_ne!(&*picked.cluster, "site-a", "turn {turn}");
        }
    }

    /// One readiness sample for `site`/`cluster` at `at_ms`.
    fn ready(site: &str, cluster: &str, value: f64, at_ms: i64) -> String {
        format!(r#"{READY_METRIC}{{grid_site="{site}",grid_provider="{cluster}"}} {value} {at_ms}"#)
    }

    #[test]
    fn a_provider_reported_unready_is_excluded_until_a_newer_sample_says_ready() {
        let store = LoadStore::new(Duration::from_secs(60));
        // site-b is the least loaded, but its operator says it cannot serve.
        store.ingest_at(&line("site-a", "pool-a", 5.0, 1_000), 1_000, 1_000, "site-a");
        store.ingest_at(&line("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        store.ingest_at(&ready("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        let candidates = || {
            validate_candidates(vec![
                cand("m", "site-a", "pool-a"),
                cand("m", "site-b", "pool-b"),
                cand("m", "site-d", "pool-d"),
            ])
            .unwrap()
        };
        let snapshot = RouteSnapshot::from_store(candidates(), Arc::from("hub"), &store, 1_000, 30_000);
        let order: Vec<&str> = snapshot.candidates.iter().map(|c| &*c.cluster).collect();
        assert_eq!(order, ["pool-a", "pool-d", "pool-b"], "unready sorts last");
        for turn in 0..10 {
            let picked = crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                turn,
                |_| true,
                &crate::route::KeepAll,
            )
            .unwrap()
            .candidate;
            assert_ne!(&*picked.cluster, "pool-b", "turn {turn}");
        }

        store.ingest_at(&ready("site-b", "pool-b", 1.0, 2_000), 2_000, 2_000, "site-b");
        let recovered = RouteSnapshot::from_store(candidates(), Arc::from("hub"), &store, 2_000, 30_000);
        assert_eq!(
            &*recovered.candidates[0].cluster, "pool-b",
            "back on the next sample, not after the window"
        );
    }

    #[test]
    fn a_not_ready_verdict_holds_through_silence_and_clears_when_the_peer_stops_publishing_it() {
        let store = LoadStore::new(Duration::from_secs(60));
        store.ingest_at(&line("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        store.ingest_at(&ready("site-b", "pool-b", 0.0, 1_000), 1_000, 1_000, "site-b");
        let order = |now| {
            let candidates =
                validate_candidates(vec![cand("m", "site-b", "pool-b"), cand("m", "site-a", "pool-a")]).unwrap();
            let snapshot = RouteSnapshot::from_store(candidates, Arc::from("hub"), &store, now, 30_000);
            snapshot.candidates.first().map(|c| c.cluster.to_string())
        };
        // Partitioned: nothing newer arrives, long after the load window.
        assert_eq!(
            order(120_000).as_deref(),
            Some("pool-a"),
            "the last 0 stands through silence"
        );
        // Healed, but the peer no longer publishes readiness.
        store.ingest_at(&line("site-b", "pool-b", 0.0, 121_000), 121_000, 121_000, "site-b");
        assert_eq!(
            order(121_000).as_deref(),
            Some("pool-b"),
            "a newer reading without the series is ready"
        );
    }

    #[test]
    fn capacity_comes_from_the_published_series_and_is_unknown_otherwise() {
        let store = LoadStore::new(Duration::from_secs(60));
        let capacity = |site: &str, cluster: &str, value: f64| {
            format!(r#"grid_provider_capacity_requests{{grid_site="{site}",grid_provider="{cluster}"}} {value} 1000"#)
        };
        store.ingest_at(&capacity("site-a", "pool-a", 64.0), 1_000, 1_000, "site-a");
        store.ingest_at(&capacity("site-b", "pool-b", 0.0), 1_000, 1_000, "site-b");
        let candidates = validate_candidates(vec![
            cand("m", "site-a", "pool-a"),
            cand("m", "site-b", "pool-b"),
            cand("m", "site-d", "pool-d"),
        ])
        .unwrap();
        let snapshot = RouteSnapshot::from_store(candidates, Arc::from("hub"), &store, 1_000, 30_000);
        let by_site: Vec<(&str, Option<f64>)> = snapshot.candidates.iter().map(|c| (&*c.site, c.capacity)).collect();
        assert!(by_site.contains(&("site-a", Some(64.0))));
        assert!(by_site.contains(&("site-b", None)), "0 is not a capacity");
        assert!(by_site.contains(&("site-d", None)), "none published");
    }

    #[test]
    fn an_excluded_candidate_from_the_serving_config_is_never_chosen() {
        let store = LoadStore::new(Duration::from_secs(60));
        let mut excluded = cand("m", "site-b", "pool-b");
        excluded.admission = AdmissionState::Excluded;
        let candidates = validate_candidates(vec![excluded, cand("m", "site-a", "pool-a")]).unwrap();
        let snapshot = RouteSnapshot::from_store(candidates, Arc::from("hub"), &store, 1_000, 30_000);
        for turn in 0..4 {
            let picked = crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                turn,
                |_| true,
                &crate::route::KeepAll,
            )
            .unwrap()
            .candidate;
            assert_eq!(&*picked.cluster, "pool-a");
        }
    }

    #[test]
    fn when_every_cluster_is_down_one_is_still_chosen() {
        let store = LoadStore::new(Duration::from_secs(60));
        let candidates = validate_candidates(vec![cand("m", "local", "site-a"), cand("m", "local", "site-b")]).unwrap();
        let down = BTreeSet::from([Arc::from("site-a"), Arc::from("site-b")]);
        let snapshot = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 60_000).demote(&down);
        assert!(
            crate::route::select_spread(
                &snapshot,
                CapabilityKind::InferenceModel,
                "m",
                0,
                |_| true,
                &crate::route::KeepAll
            )
            .is_some()
        );
    }

    #[test]
    fn a_site_reporting_the_legacy_load_name_is_still_ordered() {
        let store = LoadStore::new(Duration::from_secs(60));
        let legacy = |site: &str, cluster: &str, value: f64| {
            format!(r#"inference_pool_average_queue_size{{grid_site="{site}",grid_provider="{cluster}"}} {value} 1000"#)
        };
        store.ingest_at(&legacy("east", "pool-a", 90.0), 1_000, 1_000, "east");
        store.ingest_at(&line("west", "pool-b", 10.0, 1_000), 1_000, 1_000, "west");
        let candidates = validate_candidates(vec![cand("m", "east", "pool-a"), cand("m", "west", "pool-b")]).unwrap();
        let snapshot = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 60_000);
        let order: Vec<&str> = snapshot.candidates.iter().map(|c| &*c.site).collect();
        assert_eq!(
            order,
            ["west", "east"],
            "both names measure load; neither sorts as unmeasured"
        );
    }

    #[test]
    fn the_least_loaded_site_sorts_first() {
        let store = LoadStore::new(Duration::from_secs(60));
        // Same model on two sites: east is busy (90), west is idle (10).
        store.ingest_at(&line("east", "pool-a", 90.0, 1_000), 1_000, 1_000, "east");
        store.ingest_at(&line("west", "pool-b", 10.0, 1_000), 1_000, 1_000, "west");

        let candidates =
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")]).unwrap();
        let snap = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 30_000);

        assert_eq!(
            &*snap.candidates[0].site, "west",
            "idle site should sort ahead of the busy one"
        );
        assert_eq!(&*snap.candidates[1].site, "east");
    }

    #[test]
    fn an_unmeasured_candidate_sorts_after_a_measured_one() {
        let store = LoadStore::new(Duration::from_secs(60));
        store.ingest_at(&line("east", "pool-a", 50.0, 1_000), 1_000, 1_000, "east");
        // west has no sample at all.

        let candidates =
            validate_candidates(vec![cand("llama", "west", "pool-b"), cand("llama", "east", "pool-a")]).unwrap();
        let snap = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 30_000);

        assert_eq!(
            &*snap.candidates[0].site, "east",
            "a measured site beats an unmeasured one"
        );
        assert_eq!(&*snap.candidates[1].site, "west");
    }

    #[test]
    fn cold_start_preserves_config_order() {
        let store = LoadStore::new(Duration::from_secs(60));
        // No signals at all: every candidate is +inf, stable sort keeps input order.
        let candidates =
            validate_candidates(vec![cand("llama", "east", "pool-a"), cand("llama", "west", "pool-b")]).unwrap();
        let snap = RouteSnapshot::from_store(candidates, Arc::from("local"), &store, 1_000, 30_000);

        assert_eq!(
            &*snap.candidates[0].site, "east",
            "cold start keeps the configured order"
        );
        assert_eq!(&*snap.candidates[1].site, "west");
    }

    /// One snapshot of sites `a` and `b` at the given rho, with capacity 100.
    fn at_rho(rhos: [Option<f64>; 2]) -> RouteSnapshot {
        let store = LoadStore::new(Duration::from_secs(60));
        for (site, rho) in ["a", "b"].iter().zip(rhos) {
            let labels = format!(r#"grid_site="{site}",grid_provider="pool-{site}""#);
            store.ingest_at(
                &format!("grid_provider_capacity_requests{{{labels}}} 100 1000"),
                1_000,
                1_000,
                site,
            );
            if let Some(rho) = rho {
                store.ingest_at(
                    &format!("grid_provider_saturation_ratio{{{labels}}} {rho} 1000"),
                    1_000,
                    1_000,
                    site,
                );
            }
        }
        let candidates = validate_candidates(vec![cand("m", "a", "pool-a"), cand("m", "b", "pool-b")]).unwrap();
        RouteSnapshot::from_store(candidates, Arc::from("hub"), &store, 1_000, 30_000)
    }

    #[test]
    fn a_model_sheds_past_full_and_routes_again_once_a_site_has_room() {
        let none = BTreeSet::new();
        let shed = BTreeSet::from([Arc::from("m")]);
        assert!(
            at_rho([Some(1.10), Some(1.06)]).shed(&none).shedding.contains("m"),
            "both past 1.05"
        );
        assert!(
            at_rho([Some(1.10), Some(1.0)]).shed(&none).shedding.is_empty(),
            "one below 1.05 does not start a shed"
        );
        assert!(
            at_rho([Some(1.10), Some(1.0)]).shed(&shed).shedding.contains("m"),
            "nor does it end one"
        );
        assert!(
            at_rho([Some(1.10), Some(0.95)]).shed(&shed).shedding.is_empty(),
            "0.95 ends it"
        );
        assert!(
            at_rho([Some(1.10), None]).shed(&shed).shedding.is_empty(),
            "an unmeasured site never sheds"
        );
    }

    #[test]
    fn published_latency_reaches_the_candidate_and_absent_stays_unknown() {
        let store = LoadStore::new(Duration::from_secs(60));
        let labels = r#"grid_site="a",grid_provider="pool-a""#;
        store.ingest_at(
            &format!("grid_provider_ttft_p90_seconds{{{labels}}} 0.8 1000"),
            1_000,
            1_000,
            "a",
        );
        store.ingest_at(
            &format!("grid_provider_tpot_seconds{{{labels}}} 0.02 1000"),
            1_000,
            1_000,
            "a",
        );
        let candidates = validate_candidates(vec![cand("m", "a", "pool-a")]).unwrap();
        let snapshot = RouteSnapshot::from_store(candidates, Arc::from("hub"), &store, 1_000, 30_000);
        let latency = snapshot.candidates[0].latency;
        assert_eq!(latency.ttft_p90, Some(0.8));
        assert_eq!(latency.tpot, Some(0.02));
        assert_eq!(latency.ttft_p50, None, "unpublished");
        assert_eq!(latency.prefill_per_token, None, "unpublished");
    }
}
