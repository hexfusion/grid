//! Whether each provider can serve a request now, resolved from its scraped metrics.
//!
//! The signals loop records each scrape here. The verdict feeds three readers: the
//! provider's `Ready` condition, the `grid_provider_ready` series peers poll, and
//! admission in the serving config.

use std::{
    collections::HashMap,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use crate::{crd::inference_provider::Condition, signals::Observation};

/// Metric names that count a pool's ready endpoints, preferred first.
pub(crate) const DEFAULT_READY_ENDPOINTS: [&str; 2] = ["llm_d_epp_ready_endpoints", "inference_pool_ready_pods"];

/// Consecutive scrapes reading zero ready endpoints before a provider is not ready,
/// and reading some before it is ready again.
///
/// One reading can catch a pod between its metrics going stale and a fresh one, and a
/// pool that just drained can read ready once before its first request lands.
pub(crate) const STREAK: u32 = 2;

/// The series this site publishes per provider: 1 when ready, 0 when not.
pub const READY_SIGNAL: &str = "grid_provider_ready";

/// The `Ready` condition's status and reason for one provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reason {
    /// Scraped recently with at least one ready endpoint.
    Ready,
    /// The latest scrape counted zero ready endpoints.
    NoReadyEndpoints,
    /// No successful scrape within the staleness window.
    MetricsStale,
    /// No successful scrape within the window, and the latest attempt failed.
    MetricsUnreachable,
    /// The provider itself is `Unavailable`.
    ProviderUnavailable,
    /// The provider declares no metrics, so readiness is unknown.
    MetricsNotConfigured,
}

impl Reason {
    /// The condition reason as written to status.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::NoReadyEndpoints => "NoReadyEndpoints",
            Self::MetricsStale => "MetricsStale",
            Self::MetricsUnreachable => "MetricsUnreachable",
            Self::ProviderUnavailable => "ProviderUnavailable",
            Self::MetricsNotConfigured => "MetricsNotConfigured",
        }
    }

    /// The condition status: `True`, `False`, or `Unknown`.
    #[must_use]
    pub const fn status(self) -> &'static str {
        match self {
            Self::Ready => "True",
            Self::MetricsNotConfigured => "Unknown",
            Self::NoReadyEndpoints | Self::MetricsStale | Self::MetricsUnreachable | Self::ProviderUnavailable => {
                "False"
            },
        }
    }

    /// The one-word status shown in the STATUS column: `Ready`, `NotReady`, or `Unknown`.
    #[must_use]
    pub const fn display(self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::MetricsNotConfigured => "Unknown",
            Self::NoReadyEndpoints | Self::MetricsStale | Self::MetricsUnreachable | Self::ProviderUnavailable => {
                "NotReady"
            },
        }
    }

    /// Whether the provider is known not to serve: unknown counts as ready.
    #[must_use]
    pub const fn excludes(self) -> bool {
        matches!(
            self,
            Self::NoReadyEndpoints | Self::MetricsStale | Self::MetricsUnreachable | Self::ProviderUnavailable
        )
    }
}

/// What the signals loop last saw from one provider.
#[derive(Clone, Debug, Default)]
struct Probe {
    /// When the first scrape was attempted, which starts the grace before any success.
    first_attempt: Option<Instant>,
    /// When the last scrape succeeded.
    last_good: Option<Instant>,
    /// Ready endpoints in that scrape, `None` when it exposed no count.
    ready_endpoints: Option<f64>,
    /// Signals from the latest success, until the signals loop publishes them once.
    unpublished: Option<Vec<Observation>>,
    /// Whether the latest attempt failed.
    failing: bool,
    /// Consecutive successful scrapes that read zero ready endpoints.
    zero_streak: u32,
    /// Consecutive successful scrapes that read some ready endpoints.
    ready_streak: u32,
    /// Whether the count marks the provider not ready, with [`STREAK`] hysteresis both ways.
    no_endpoints: bool,
}

/// One provider's verdict and the detail behind it.
#[derive(Clone, Debug, PartialEq)]
pub struct Verdict {
    /// Status and reason.
    pub reason: Reason,
    /// Human detail for the condition message.
    pub message: String,
}

/// The latest scrape per provider, keyed by network and routing identity.
#[derive(Debug, Default)]
pub struct ReadinessStore(Mutex<HashMap<String, Probe>>);

/// The store key for `identity` in `network`: identities are unique per network only.
pub(crate) fn key(network: &str, identity: &str) -> String {
    format!("{network}/{identity}")
}

impl ReadinessStore {
    /// The probes, recovered if a panicking holder poisoned the lock: each write is whole.
    fn probes(&self) -> std::sync::MutexGuard<'_, HashMap<String, Probe>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record a successful scrape at `now`.
    #[expect(clippy::significant_drop_tightening, reason = "the guard covers one whole update")]
    pub(crate) fn record_success(
        &self,
        key: &str,
        ready_endpoints: Option<f64>,
        observations: Vec<Observation>,
        now: Instant,
    ) {
        let mut probes = self.probes();
        let probe = probes.entry(key.to_owned()).or_default();
        probe.first_attempt.get_or_insert(now);
        match ready_endpoints {
            Some(count) if count < 1.0 => {
                probe.zero_streak = probe.zero_streak.saturating_add(1);
                probe.ready_streak = 0;
                probe.no_endpoints |= probe.zero_streak >= STREAK;
            },
            Some(_) => {
                probe.ready_streak = probe.ready_streak.saturating_add(1);
                probe.zero_streak = 0;
                probe.no_endpoints &= probe.ready_streak < STREAK;
            },
            // No count to read: reachable is all that is known.
            None => {
                probe.zero_streak = 0;
                probe.ready_streak = 0;
                probe.no_endpoints = false;
            },
        }
        probe.last_good = Some(now);
        probe.ready_endpoints = ready_endpoints;
        probe.unpublished = Some(observations);
        probe.failing = false;
    }

    /// Record a failed scrape at `now`, keeping what the last good one saw.
    #[expect(clippy::significant_drop_tightening, reason = "the guard covers one whole update")]
    pub(crate) fn record_failure(&self, key: &str, now: Instant) {
        let mut probes = self.probes();
        let probe = probes.entry(key.to_owned()).or_default();
        probe.first_attempt.get_or_insert(now);
        probe.failing = true;
    }

    /// The verdict for `key` at `now`.
    ///
    /// `None` until the provider has been attempted, and through the first
    /// `stale_after` while no scrape has succeeded yet, so a starting operator
    /// publishes nothing rather than a guess.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the guard covers one small evaluation"
    )]
    pub(crate) fn verdict(&self, key: &str, unavailable: bool, stale_after: Duration, now: Instant) -> Option<Verdict> {
        let probes = self.probes();
        let probe = probes.get(key)?;
        let within = |at: Instant| now.saturating_duration_since(at) <= stale_after;
        if probe.last_good.is_none() && !unavailable && probe.first_attempt.is_some_and(within) {
            return None;
        }
        Some(evaluate(
            probe,
            probe.last_good.is_some_and(within),
            unavailable,
            stale_after,
        ))
    }

    /// The signals of the latest successful scrape, once: stale load is never republished.
    pub(crate) fn take_fresh(&self, key: &str) -> Option<Vec<Observation>> {
        self.probes().get_mut(key)?.unpublished.take()
    }

    /// Forget providers not in `keep`.
    pub(crate) fn retain(&self, keep: &std::collections::BTreeSet<String>) {
        self.probes().retain(|key, _| keep.contains(key));
    }
}

/// The verdict for one probe. `fresh` says whether its last success is within `stale_after`.
fn evaluate(probe: &Probe, fresh: bool, unavailable: bool, stale_after: Duration) -> Verdict {
    if unavailable {
        return Verdict {
            reason: Reason::ProviderUnavailable,
            message: "provider is Unavailable".to_owned(),
        };
    }
    if !fresh {
        let reason = if probe.failing {
            Reason::MetricsUnreachable
        } else {
            Reason::MetricsStale
        };
        return Verdict {
            reason,
            message: format!("no successful metrics scrape in the last {}s", stale_after.as_secs()),
        };
    }
    from_ready_endpoints(probe)
}

/// The verdict for a freshly scraped provider, from its ready-endpoint count.
fn from_ready_endpoints(probe: &Probe) -> Verdict {
    let count = probe.ready_endpoints;
    if probe.no_endpoints {
        let message = match count {
            Some(count) if count >= 1.0 => format!("{count} ready endpoints, awaiting a second scrape"),
            _ => format!("0 ready endpoints for {STREAK} or more scrapes"),
        };
        return Verdict {
            reason: Reason::NoReadyEndpoints,
            message,
        };
    }
    let message = match count {
        Some(count) if count < 1.0 => "0 ready endpoints in the latest scrape only".to_owned(),
        Some(count) => format!("{count} ready endpoints"),
        None => "metrics reachable, no ready-endpoint count exposed".to_owned(),
    };
    Verdict {
        reason: Reason::Ready,
        message,
    }
}

/// The condition type this module owns.
pub const READY_CONDITION: &str = "Ready";

/// The `Ready` condition to write, or `None` when `current` already says the same.
///
/// A changed message alone is not written, so a moving endpoint count does not
/// churn status. A new generation is. `lastTransitionTime` moves only when the status does.
pub(crate) fn ready_condition(
    current: &[Condition],
    verdict: &Verdict,
    now_rfc3339: &str,
    generation: Option<i64>,
) -> Option<Condition> {
    let held = current.iter().find(|c| c.type_ == READY_CONDITION);
    let status = verdict.reason.status();
    if held.is_some_and(|c| {
        c.status == status && c.reason == verdict.reason.as_str() && c.observed_generation == generation
    }) {
        return None;
    }
    let last_transition_time = held
        .filter(|c| c.status == status)
        .map_or_else(|| now_rfc3339.to_owned(), |c| c.last_transition_time.clone());
    Some(Condition {
        type_: READY_CONDITION.to_owned(),
        status: status.to_owned(),
        reason: verdict.reason.as_str().to_owned(),
        message: verdict.message.clone(),
        last_transition_time,
        observed_generation: generation,
    })
}

/// Ready endpoints for `pool` in `observations`, from the first of `names` present.
///
/// With a pool, only series whose `name` label is that pool count, so an EPP that
/// serves several pools is read for the configured one.
pub(crate) fn ready_endpoints(observations: &[Observation], names: &[&str], pool: Option<&str>) -> Option<f64> {
    names.iter().find_map(|name| {
        observations
            .iter()
            .filter(|o| o.metric == *name)
            .filter(|o| pool.is_none_or(|pool| o.labels.get("name").is_some_and(|n| n == pool)))
            .map(|o| o.value)
            .reduce(f64::max)
    })
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::too_many_lines,
    clippy::shadow_unrelated,
    reason = "tests"
)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    const STALE: Duration = Duration::from_secs(15);

    fn sample(metric: &str, pool: &str, value: f64) -> Observation {
        Observation {
            metric: metric.to_owned(),
            labels: BTreeMap::from([("name".to_owned(), pool.to_owned())]),
            value,
            timestamp_ms: None,
        }
    }

    #[test]
    fn ready_endpoints_prefers_the_current_name_and_reads_the_configured_pool() {
        let observations = [
            sample("inference_pool_ready_pods", "qwen3", 4.0),
            sample("llm_d_epp_ready_endpoints", "other", 9.0),
            sample("llm_d_epp_ready_endpoints", "qwen3", 2.0),
        ];
        assert_eq!(
            ready_endpoints(&observations, &DEFAULT_READY_ENDPOINTS, Some("qwen3")),
            Some(2.0)
        );
        assert_eq!(
            ready_endpoints(&observations[..1], &DEFAULT_READY_ENDPOINTS, Some("qwen3")),
            Some(4.0),
            "falls back to the deprecated name"
        );
        assert_eq!(
            ready_endpoints(&observations, &DEFAULT_READY_ENDPOINTS, Some("absent")),
            None
        );
    }

    fn reason(store: &ReadinessStore, now: Instant) -> Option<Reason> {
        store.verdict("p", false, STALE, now).map(|verdict| verdict.reason)
    }

    #[test]
    fn verdict_follows_the_latest_scrape_and_staleness() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        assert_eq!(reason(&store, start), None, "never attempted");

        store.record_success("p", Some(2.0), vec![sample("q", "p", 1.0)], start);
        assert_eq!(reason(&store, start), Some(Reason::Ready));
        assert_eq!(store.take_fresh("p").map(|held| held.len()), Some(1));
        assert_eq!(store.take_fresh("p"), None, "fresh signals are published once");

        store.record_success("p", None, Vec::new(), start);
        assert_eq!(
            reason(&store, start),
            Some(Reason::Ready),
            "a scrape without the count is not read as zero"
        );
        assert_eq!(store.take_fresh("p").map(|held| held.len()), Some(0));

        store.record_failure("p", start + STALE);
        assert_eq!(
            reason(&store, start + STALE),
            Some(Reason::Ready),
            "a failure within the window keeps the last verdict"
        );
        assert_eq!(store.take_fresh("p"), None, "a failure publishes no held load");
        assert_eq!(
            reason(&store, start + STALE + Duration::from_secs(1)),
            Some(Reason::MetricsUnreachable)
        );
    }

    #[test]
    fn zero_endpoints_take_two_scrapes_to_exclude_and_two_to_readmit() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        let scrape = |count| {
            store.record_success("p", Some(count), Vec::new(), now);
            reason(&store, now)
        };
        assert_eq!(scrape(0.0), Some(Reason::Ready), "one zero reading is not enough");
        assert_eq!(scrape(0.0), Some(Reason::NoReadyEndpoints));
        assert_eq!(
            scrape(1.0),
            Some(Reason::NoReadyEndpoints),
            "one ready reading is not enough"
        );
        assert_eq!(
            scrape(0.0),
            Some(Reason::NoReadyEndpoints),
            "a relapse restarts the count"
        );
        assert_eq!(scrape(1.0), Some(Reason::NoReadyEndpoints));
        assert_eq!(scrape(1.0), Some(Reason::Ready));
    }

    #[test]
    fn a_first_scrape_that_fails_gets_the_staleness_window_before_a_verdict() {
        let store = ReadinessStore::default();
        let start = Instant::now();
        store.record_failure("p", start);
        assert_eq!(reason(&store, start + STALE), None, "grace, not a guess");
        assert_eq!(
            reason(&store, start + STALE + Duration::from_secs(1)),
            Some(Reason::MetricsUnreachable)
        );
    }

    #[test]
    fn providers_are_keyed_per_network() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        for _ in 0..STREAK {
            store.record_success(&key("east", "p"), Some(0.0), Vec::new(), now);
        }
        store.record_success(&key("west", "p"), Some(3.0), Vec::new(), now);
        let reason_in = |network| {
            store
                .verdict(&key(network, "p"), false, STALE, now)
                .map(|verdict| verdict.reason)
        };
        assert_eq!(reason_in("east"), Some(Reason::NoReadyEndpoints));
        assert_eq!(reason_in("west"), Some(Reason::Ready));
    }

    #[test]
    fn an_unavailable_provider_is_not_ready_whatever_its_metrics_say() {
        let store = ReadinessStore::default();
        let now = Instant::now();
        store.record_success("p", Some(3.0), Vec::new(), now);
        let verdict = store.verdict("p", true, STALE, now).unwrap();
        assert_eq!(verdict.reason, Reason::ProviderUnavailable);
        assert!(verdict.reason.excludes());
    }

    #[test]
    fn the_status_column_reads_like_a_node() {
        assert_eq!(Reason::Ready.display(), "Ready");
        assert_eq!(Reason::MetricsNotConfigured.display(), "Unknown");
        for reason in [
            Reason::NoReadyEndpoints,
            Reason::MetricsStale,
            Reason::MetricsUnreachable,
            Reason::ProviderUnavailable,
        ] {
            assert_eq!(reason.display(), "NotReady", "{reason:?}");
        }
    }

    #[test]
    fn the_condition_is_written_on_a_status_or_reason_change_only() {
        let ready = Verdict {
            reason: Reason::Ready,
            message: "2 ready endpoints".to_owned(),
        };
        let first = ready_condition(&[], &ready, "t0", Some(3)).expect("absent is written");
        assert_eq!(
            (first.status.as_str(), first.last_transition_time.as_str()),
            ("True", "t0")
        );

        let recount = Verdict {
            message: "3 ready endpoints".to_owned(),
            ..ready
        };
        assert!(
            ready_condition(std::slice::from_ref(&first), &recount, "t1", Some(3)).is_none(),
            "a new count alone is not news"
        );
        let regenerated = ready_condition(std::slice::from_ref(&first), &recount, "t1", Some(4))
            .expect("a new generation is written");
        assert_eq!(
            regenerated.last_transition_time, "t0",
            "the same status keeps its transition time"
        );

        let down = Verdict {
            reason: Reason::NoReadyEndpoints,
            message: "0 ready endpoints".to_owned(),
        };
        let second = ready_condition(std::slice::from_ref(&first), &down, "t2", Some(3)).unwrap();
        assert_eq!(
            (second.status.as_str(), second.last_transition_time.as_str()),
            ("False", "t2")
        );

        let unreachable = Verdict {
            reason: Reason::MetricsUnreachable,
            message: "no scrape".to_owned(),
        };
        let third = ready_condition(std::slice::from_ref(&second), &unreachable, "t3", Some(3)).unwrap();
        assert_eq!(
            third.last_transition_time, "t2",
            "a new reason under the same status keeps the transition time"
        );
    }

    #[test]
    fn statuses_match_the_condition_contract() {
        assert_eq!(Reason::Ready.status(), "True");
        assert_eq!(Reason::MetricsNotConfigured.status(), "Unknown");
        assert!(!Reason::MetricsNotConfigured.excludes(), "unknown is not down");
        for reason in [
            Reason::NoReadyEndpoints,
            Reason::MetricsStale,
            Reason::MetricsUnreachable,
            Reason::ProviderUnavailable,
        ] {
            assert_eq!(reason.status(), "False");
            assert!(reason.excludes());
        }
    }
}
