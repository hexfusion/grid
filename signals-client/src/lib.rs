//! Grid signal poller: the mTLS scrape transport that feeds the signals source.
//!
//! Polls the local operator's [`SIGNALS_PATH`] and writes each scraped
//! exposition into the `grid-signals` `LoadStore`. This is the transport layer
//! of the signals source. `grid-signals` stays a dashmap-only leaf. Tokio and
//! the pinned client live here.

mod mtls;
mod poller;
mod scrape;

pub use mtls::{MtlsError, PeerScraper};

/// The mTLS path the operator serves the signals rollup on.
///
/// Versioned with the grid API. The operator keeps a twin constant, since it does not depend on this crate.
pub const SIGNALS_PATH: &str = "/v1beta1/site/signals";
pub use poller::{
    FetchError, PinnedTls, PollHandle, PollerConfig, Scrape, SignalSource, build_url, deserialize_interval_ms, spawn,
    spawn_from_config, spawn_on_thread,
};

#[cfg(test)]
mod tests {
    #[test]
    fn the_signals_path_is_pinned_to_the_shared_literal() {
        // operator::signals::SIGNALS_PATH pins the same literal.
        assert_eq!(super::SIGNALS_PATH, "/v1beta1/site/signals");
    }
}
