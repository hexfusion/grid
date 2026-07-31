//! Prometheus metrics for gateway probe observability.
//!
//! All label values are bounded enum variants — no site names,
//! addresses, fingerprints, or PEM content.

use std::{sync::LazyLock, time::Duration};

use prometheus::{
    Encoder as _, Histogram, HistogramOpts, IntCounterVec, Opts, Registry, TextEncoder, proto::MetricFamily,
};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Global registry for operator metrics.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let r = Registry::new();
    r.register(Box::new(PROBE_TOTAL.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PROBE_DURATION.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r.register(Box::new(PHASE_TRANSITIONS.clone()))
        .unwrap_or_else(|_| std::process::abort());
    r
});

/// Total gateway probe attempts by outcome and TLS mode.
static PROBE_TOTAL: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_gateway_probe_total", "Total gateway probe attempts"),
        &["outcome", "tls_mode"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

/// Gateway probe duration in seconds.
static PROBE_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
    Histogram::with_opts(HistogramOpts::new(
        "grid_gateway_probe_duration_seconds",
        "Gateway probe duration",
    ))
    .unwrap_or_else(|_| std::process::abort())
});

/// `GridSite` phase transitions by source phase, target phase, and reason.
static PHASE_TRANSITIONS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("grid_site_phase_transition_total", "GridSite phase transitions"),
        &["from_phase", "to_phase", "reason"],
    )
    .unwrap_or_else(|_| std::process::abort())
});

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Record a completed gateway probe.
pub(crate) fn record_probe(outcome: &str, tls_mode: &str, duration: Duration) {
    PROBE_TOTAL.with_label_values(&[outcome, tls_mode]).inc();
    PROBE_DURATION.observe(duration.as_secs_f64());
}

/// Record a `GridSite` phase transition.
pub(crate) fn record_phase_transition(from: &str, to: &str, reason: &str) {
    PHASE_TRANSITIONS.with_label_values(&[from, to, reason]).inc();
}

/// Gather all registered metrics for serialization.
pub(crate) fn gather_metrics() -> Vec<MetricFamily> {
    REGISTRY.gather()
}

/// Encode all metrics as Prometheus text format.
pub fn encode_metrics() -> Vec<u8> {
    let encoder = TextEncoder::new();
    let families = gather_metrics();
    let mut buffer = Vec::new();
    encoder
        .encode(&families, &mut buffer)
        .unwrap_or_else(|_| std::process::abort());
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_probe_increments_counter() {
        record_probe("Verified", "mtls", Duration::from_millis(42));
        let val = PROBE_TOTAL.with_label_values(&["Verified", "mtls"]).get();
        assert!(val >= 1, "probe counter should be >= 1, got {val}");
    }

    #[test]
    fn record_phase_transition_increments_counter() {
        record_phase_transition("Connecting", "Active", "TlsVerified");
        let val = PHASE_TRANSITIONS
            .with_label_values(&["Connecting", "Active", "TlsVerified"])
            .get();
        assert!(val >= 1, "transition counter should be >= 1, got {val}");
    }

    #[test]
    fn probe_duration_records_observation() {
        record_probe("ConnectTimeout", "mtls", Duration::from_millis(100));
        let count = PROBE_DURATION.get_sample_count();
        assert!(count >= 1, "histogram should have at least 1 observation");
    }

    #[test]
    fn encode_metrics_produces_prometheus_text() {
        record_probe("ConnectionFailed", "mtls", Duration::from_millis(1));
        let buf = encode_metrics();
        let text = String::from_utf8(buf).unwrap_or_else(|_| std::process::abort());
        assert!(
            text.contains("grid_gateway_probe_total"),
            "output should contain probe counter"
        );
        assert!(
            text.contains("grid_gateway_probe_duration_seconds"),
            "output should contain duration histogram"
        );
    }
}
