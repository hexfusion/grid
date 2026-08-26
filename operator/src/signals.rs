//! Provider signals, collected by this operator and served for others to read.
//!
//! The operator already scrapes each provider to score it. This keeps what it
//! parsed and serves it, so a peer or gateway reads a provider's signals
//! without scraping it or holding its credentials.
//!
//! A multi-target exporter, not Prometheus federation: `target` picks one
//! provider, `collect[]` picks signals by name.
//!
//! Held values expire rather than being marked stale, so absence is what says a
//! writer stopped refreshing and no clock is compared against another's.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::{StreamExt as _, future::BoxFuture};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};

use crate::metrics_scraper::{MetricsScrapeError, scrape_metrics};

/// Label naming the site a sample was observed at.
pub const SITE_LABEL: &str = "grid_site";

/// Label naming the provider a sample was observed from.
pub const PROVIDER_LABEL: &str = "grid_provider";

/// Whether a metric name is one component of a histogram or summary.
fn is_aggregate_part(metric: &str) -> bool {
    metric.ends_with("_bucket") || metric.ends_with("_sum") || metric.ends_with("_count")
}

/// One parsed sample, owned so labels can be attributed and ordering is stable.
///
/// A `BTreeMap` rather than a hash map: label order then depends only on the
/// labels, so two renders of the same sample are byte-identical.
#[derive(Clone, Debug, PartialEq)]
pub struct Observation {
    /// Metric name.
    pub metric: String,
    /// Labels, including any this site attributed.
    pub labels: BTreeMap<String, String>,
    /// Reported value.
    pub value: f64,
}

/// Parse an exposition response into observations.
///
/// An unparseable response yields nothing rather than an error: it is a copy of
/// somebody else's scrape and the caller cannot repair it.
#[must_use]
pub fn parse(text: &str) -> Vec<Observation> {
    // Types first: a declaration may follow its samples, and reading in one
    // pass would admit a counter that had not been typed yet.
    let types: HashMap<&str, &str> = text
        .lines()
        .filter_map(|l| l.strip_prefix('#'))
        .filter_map(|rest| {
            let mut field = rest.split_whitespace();
            (field.next() == Some("TYPE")).then(|| field.next().zip(field.next()))?
        })
        .collect();

    text.lines()
        .filter(|l| !l.trim_start().starts_with('#'))
        .filter_map(parse_sample)
        .filter(|o| {
            // A counter relayed through a cache reports this process's restarts
            // rather than the provider's, and an aggregate describes a
            // distribution this operator did not observe and cannot recombine.
            // Names catch the aggregates that arrive untyped.
            matches!(types.get(o.metric.as_str()), None | Some(&("gauge" | "untyped")))
                && !is_aggregate_part(&o.metric)
                // The consumer ranks a non-finite score above every finite one.
                && o.value.is_finite()
        })
        .collect()
}

/// Parse one sample line: `name{label="v",...} value [timestamp]`.
///
/// Written here rather than taken from a crate because the obvious ones match
/// the name with `\w+`, which excludes the colon every vLLM metric uses, and
/// delimit labels with `[^}]+`, which stops at a brace inside a quoted value.
/// Both failures are silent: the line is skipped and the scrape looks short.
fn parse_sample(line: &str) -> Option<Observation> {
    let line = line.trim();
    let (metric, rest) = split_metric_name(line)?;
    let (labels, rest) = if rest.starts_with('{') {
        parse_labels(rest)?
    } else {
        (BTreeMap::new(), rest)
    };
    Some(Observation {
        metric: metric.to_owned(),
        labels,
        value: rest.split_whitespace().next()?.parse().ok()?,
    })
}

/// Split a leading metric name from the rest of the line.
fn split_metric_name(line: &str) -> Option<(&str, &str)> {
    let end = line
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or(line.len());
    let (name, rest) = line.split_at(end);
    let first = name.chars().next()?;
    (first.is_ascii_alphabetic() || first == '_' || first == ':').then_some((name, rest))
}

/// Parse a `{...}` label set, returning it and what follows the closing brace.
fn parse_labels(rest: &str) -> Option<(BTreeMap<String, String>, &str)> {
    let mut labels = BTreeMap::new();
    let mut s = rest.strip_prefix('{')?;
    loop {
        s = s.trim_start();
        if let Some(tail) = s.strip_prefix('}') {
            return Some((labels, tail));
        }
        let (name, tail) = split_label_name(s)?;
        let tail = tail.trim_start().strip_prefix('=')?.trim_start();
        let (value, tail) = parse_label_value(tail)?;
        labels.insert(name.to_owned(), value);
        s = tail.trim_start();
        s = s.strip_prefix(',').unwrap_or(s);
    }
}

/// Split a leading label name.
fn split_label_name(s: &str) -> Option<(&str, &str)> {
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(s.len());
    (end > 0).then(|| s.split_at(end))
}

/// Parse a quoted label value, honouring the three escapes the format defines.
fn parse_label_value(s: &str) -> Option<(String, &str)> {
    let mut rest = s.strip_prefix('"')?.chars();
    let mut value = String::new();
    loop {
        match rest.next()? {
            '"' => return Some((value, rest.as_str())),
            '\\' => value.push(match rest.next()? {
                'n' => '\n',
                other => other,
            }),
            c => value.push(c),
        }
    }
}

/// Attach the labels this site is authoritative for.
///
/// A provider that already set one has its value preserved under an `exported_`
/// name, which is what a scrape does when `honor_labels` is unset. Working on
/// parsed labels rather than on text is what makes a duplicate label impossible
/// to emit, and a duplicate would make a scraper reject the whole response.
#[must_use]
pub fn attribute(observations: Vec<Observation>, site: &str, provider: &str) -> Vec<Observation> {
    observations
        .into_iter()
        .map(|mut o| {
            for (key, value) in [(SITE_LABEL, site), (PROVIDER_LABEL, provider)] {
                if let Some(theirs) = o.labels.remove(key) {
                    o.labels.insert(format!("exported_{key}"), theirs);
                }
                o.labels.insert(key.to_owned(), value.to_owned());
            }
            o
        })
        .collect()
}

/// What is held for one target.
#[derive(Clone, Debug)]
struct Cached {
    /// Parsed samples, already attributed.
    samples: Arc<[Observation]>,
    /// When the value was collected, on this process's monotonic clock.
    ///
    /// Reported as `Age`, which is a duration rather than a time, so a reader
    /// learns how old a value is without either clock being compared.
    collected_at: Instant,
    /// When this stops being served.
    expires_at: Instant,
}

/// Signals held per target, where a target is a provider routing identity.
#[derive(Clone, Debug, Default)]
pub struct SignalStore {
    /// Target to what is held for it.
    inner: Arc<RwLock<BTreeMap<String, Cached>>>,
}

impl SignalStore {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Refresh the given targets and drop anything past its deadline.
    ///
    /// A target absent from `collected` is left alone rather than removed, so a
    /// single failed scrape does not erase what is known. It expires on its own
    /// once nothing refreshes it.
    pub fn refresh(&self, collected: BTreeMap<String, Vec<Observation>>, ttl: Duration) {
        let now = Instant::now();
        let Ok(mut guard) = self.inner.write() else {
            return;
        };
        for (target, samples) in collected {
            guard.insert(
                target,
                Cached {
                    samples: samples.into(),
                    collected_at: now,
                    expires_at: now + ttl,
                },
            );
        }
        guard.retain(|_, held| held.expires_at > now);
    }

    /// Render exposition for `target`, or for every target when it is `None`.
    ///
    /// `collect` narrows to the named metrics; empty returns all of them.
    ///
    /// Returns the body and the age of its oldest value, so `Age` bounds the
    /// whole response rather than describing it on average.
    #[must_use]
    pub fn render(&self, target: Option<&str>, collect: &[String]) -> (String, Duration) {
        let Ok(guard) = self.inner.read() else {
            return (String::new(), Duration::ZERO);
        };
        let now = Instant::now();
        let now_wall = SystemTime::now();
        let mut out = String::new();
        let mut oldest = Duration::ZERO;
        for (name, held) in guard.iter() {
            if held.expires_at <= now || target.is_some_and(|t| t != name) {
                continue;
            }
            // TTL is measured against a monotonic instant, immune to clock
            // steps. Exposition wants epoch millis, so derive from the age
            // rather than storing a wall clock that could move underneath us.
            let age = now.saturating_duration_since(held.collected_at);
            let collected_at_ms = wall_millis(now_wall, age);
            let mut used = false;
            for sample in held.samples.iter() {
                if collect.is_empty() || collect.iter().any(|c| c == &sample.metric) {
                    render_sample(&mut out, sample, collected_at_ms);
                    out.push('\n');
                    used = true;
                }
            }
            if used {
                oldest = oldest.max(now.saturating_duration_since(held.collected_at));
            }
        }
        (out, oldest)
    }

    /// Targets currently held and unexpired.
    #[must_use]
    pub fn targets(&self) -> Vec<String> {
        let Ok(guard) = self.inner.read() else {
            return Vec::new();
        };
        let now = Instant::now();
        guard
            .iter()
            .filter(|(_, held)| held.expires_at > now)
            .map(|(name, _)| name.clone())
            .collect()
    }
}

/// Render one observation as an exposition line, without a timestamp.
fn render_sample(out: &mut String, o: &Observation, collected_at_ms: i64) {
    // Written into the caller's buffer rather than returned. Building a string
    // per label, collecting them, joining them and then formatting the result
    // allocated once per label plus four more per sample, all of it discarded
    // into a buffer that was going to be grown anyway.
    out.push_str(&o.metric);
    if !o.labels.is_empty() {
        out.push('{');
        for (i, (key, value)) in o.labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            escape_into(out, value);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&o.value.to_string());
    out.push(' ');
    out.push_str(&collected_at_ms.to_string());
}

/// Epoch milliseconds for an observation collected `age` ago.
///
/// The consumer reads this against the response `Date`, so both ends of the
/// subtraction come from this host's clock and no foreign clock is imported.
fn wall_millis(now: SystemTime, age: Duration) -> i64 {
    let since_epoch = now
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .saturating_sub(age);
    i64::try_from(since_epoch.as_millis()).unwrap_or(i64::MAX)
}

/// Escape a label value for exposition.
fn escape_into(out: &mut String, value: &str) {
    // One pass. The previous form ran replace three times, allocating a whole
    // new string on each, for values that almost never contain any of them.
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str(r"\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str(r"\n"),
            other => out.push(other),
        }
    }
}

/// A peer site to collect from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSite {
    /// Site name, as used in membership.
    pub name: String,
    /// Where to read that site's signals.
    pub url: String,
}

/// Alive peers other than this site, addressed at their signals port.
///
/// Takes the name and advertised address of each member rather than a
/// membership snapshot, so deriving an address stays independent of how
/// membership is discovered.
pub fn peer_sites<'member, Members>(members: Members, local_site: &str, port: u16, scheme: &str) -> Vec<PeerSite>
where
    Members: Iterator<Item = (&'member str, &'member str)>,
{
    members
        .filter(|(site, _)| *site != local_site)
        .filter_map(|(site, endpoint)| {
            // The advertised address is the membership listener, so only its
            // host is meaningful here.
            let host = match endpoint.rsplit_once(':') {
                Some((host, _)) if !host.is_empty() => host,
                _ => return None,
            };
            Some(PeerSite {
                name: site.to_owned(),
                url: format!("{scheme}://{host}:{port}/metrics"),
            })
        })
        .collect()
}

/// Everything outside the RFC 3986 unreserved set is escaped in a query value.
const QUERY_VALUE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// Where another site's signals come from.
///
/// Grid replicates provider metrics through gossip today, and this design polls
/// them instead. Both are ways of collecting the same data, so they sit behind
/// one trait: choosing between them is configuration.
pub trait PeerSignals: Send + Sync {
    /// Name, as it would be written in configuration.
    fn name(&self) -> &'static str;

    /// Observations for each site, keyed by site name.
    ///
    /// A site that did not answer is absent rather than empty, so a caller can
    /// tell silence from a site that genuinely has nothing to report.
    fn collect<'poll>(&'poll self, sites: &'poll [PeerSite]) -> BoxFuture<'poll, BTreeMap<String, Vec<Observation>>>;
}

/// Why a poll ended, at the granularity a response differs by.
///
/// Collapsing these into success and failure would leave the question that
/// actually gets asked, why is this peer not being scored, unanswerable. A
/// refused connection is a peer that is down. A TLS failure is a trust problem
/// that will still be there in two hundred milliseconds. A 403 is the peer
/// declining to answer a question it understood.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollOutcome {
    /// The peer answered.
    Ok,
    /// No answer within the timeout.
    Timeout,
    /// The connection was refused or the host was unreachable.
    Refused,
    /// The TLS handshake or certificate verification failed.
    Tls,
    /// Any other transport failure.
    Transport,
    /// The peer answered with a 4xx.
    ClientError,
    /// The peer answered with a 5xx.
    ServerError,
    /// The body was not decodable.
    Encoding,
    /// This site is misconfigured for that peer.
    Config,
    /// The process is shutting down and the poll stood down.
    Cancelled,
}

impl PollOutcome {
    /// Label value, stable because dashboards and alerts are written against it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Timeout => "timeout",
            Self::Refused => "refused",
            Self::Tls => "tls",
            Self::Transport => "transport",
            Self::ClientError => "client_error",
            Self::ServerError => "server_error",
            Self::Encoding => "encoding",
            Self::Config => "config",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether trying again within this round could plausibly help.
    ///
    /// Retrying a trust failure or a refusal to answer wastes the budget that a
    /// genuinely transient failure needs, and turns one misconfiguration into a
    /// burst against a peer that already said no.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::Timeout | Self::Refused | Self::Transport | Self::ServerError
        )
    }
}

/// Classify a scrape failure.
fn classify(error: &MetricsScrapeError) -> PollOutcome {
    match error {
        MetricsScrapeError::Timeout(_) => PollOutcome::Timeout,
        MetricsScrapeError::NonOkStatus { status, .. } => {
            if (500..600).contains(status) {
                PollOutcome::ServerError
            } else {
                PollOutcome::ClientError
            }
        },
        MetricsScrapeError::Encoding(_) => PollOutcome::Encoding,
        MetricsScrapeError::InvalidUrl(_) | MetricsScrapeError::HttpWithTls(_) | MetricsScrapeError::TlsMaterial(_) => {
            PollOutcome::Config
        },
        MetricsScrapeError::Transport(inner) => classify_transport(&**inner),
    }
}

/// Separate a trust failure from an unreachable peer.
///
/// Both arrive as transport errors, and they call for opposite responses: one
/// is worth retrying and the other will fail identically until a certificate is
/// replaced. The chain is walked rather than the message matched, because an
/// error string is not an interface.
#[expect(clippy::wildcard_enum_match_arm, reason = "std::io::ErrorKind is non_exhaustive")]
fn classify_transport(error: &(dyn std::error::Error + 'static)) -> PollOutcome {
    let mut current = Some(error);
    while let Some(err) = current {
        if err.downcast_ref::<rustls::Error>().is_some() {
            return PollOutcome::Tls;
        }
        if let Some(io) = err.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::ConnectionReset => PollOutcome::Refused,
                std::io::ErrorKind::TimedOut => PollOutcome::Timeout,
                _ => PollOutcome::Transport,
            };
        }
        current = err.source();
    }
    PollOutcome::Transport
}

/// Delay before attempt `attempt`, counting the first retry as zero.
///
/// Exponential, capped, and spread by a value derived from the peer's name
/// rather than from a random source. Fifty sites that all failed on the same
/// partition would otherwise retry in step and arrive together when it heals,
/// which is the herd the whole design is trying not to build. Deriving the
/// spread from the name decorrelates peers without making a run unrepeatable.
fn backoff(base: Duration, attempt: u32, peer: &str) -> Duration {
    let factor = 1_u32 << attempt.min(5);
    let scaled = base.saturating_mul(factor);
    let spread = peer
        .bytes()
        .fold(0_u32, |acc, b| acc.wrapping_mul(31).wrapping_add(u32::from(b)));
    // Up to a quarter of the interval, added rather than subtracted so a delay
    // is never shorter than the backoff asked for.
    let jitter = (scaled / 4).saturating_mul(spread % 100) / 100;
    scaled.saturating_add(jitter)
}

/// Reads each peer's signals endpoint directly.
pub struct PollPeers {
    /// Per-attempt request timeout.
    pub timeout: Duration,
    /// Client TLS, once the endpoint requires it.
    pub tls: Option<Arc<rustls::ClientConfig>>,
    /// Signals to ask each peer for; empty asks for all of them.
    pub collect: Vec<String>,
    /// How many peers to poll at once.
    ///
    /// A round fans out to every peer, and at fifty sites that is fifty sockets
    /// opened at once against fifty different networks. Bounding it keeps a
    /// round's cost proportional to the pool rather than to the grid.
    pub concurrency: usize,
    /// Attempts per peer, including the first.
    pub attempts: u32,
    /// Base delay between attempts.
    pub backoff: Duration,
    /// Total time a single peer may take, retries included.
    ///
    /// Without this a peer that fails slowly could hold a round open past the
    /// next one, and rounds would overlap until the pool was full of work
    /// nobody is waiting for any more.
    pub budget: Duration,
    /// Duration past which a poll is counted as slow.
    pub slow_after: Duration,
    /// Signal that the process is stopping.
    ///
    /// A round can be in flight against a dozen peers when a pod is told to
    /// terminate. Without this the only way to stop is to drop the future,
    /// which stops it wherever it happens to be and leaves the accounting for
    /// that poll half done.
    pub shutdown: crate::shutdown::Shutdown,
}

impl Default for PollPeers {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(2),
            tls: None,
            collect: Vec::new(),
            concurrency: 8,
            attempts: 3,
            backoff: Duration::from_millis(50),
            budget: Duration::from_secs(5),
            slow_after: Duration::from_secs(1),
            shutdown: crate::shutdown::Shutdown::never(),
        }
    }
}

impl PollPeers {
    /// Poll one peer, retrying what is worth retrying, and record what happened.
    ///
    /// Returns the body, or nothing if every attempt failed. The outcome is
    /// recorded either way, because a peer that is never reachable has to be
    /// distinguishable from one that has nothing to say.
    async fn poll_one(&self, peer: &str, url: &str) -> Option<String> {
        // The guard owns the accounting. Every path out of this function runs
        // it, including the one where the future is dropped mid-await, which is
        // the path that used to leave the in-flight gauge counting a poll that
        // no longer exists.
        let mut guard = PollGuard::enter(peer, self.slow_after);

        let (outcome, body) = self.attempt_until(peer, url, guard.started).await;
        guard.finish(outcome, body.as_ref().map_or(0, String::len));
        body
    }

    /// One request, abandoned if the process is stopping.
    ///
    /// The request races the signal. A peer that is slow to answer must not
    /// hold termination open for the length of a timeout it was never going to
    /// beat. `None` means the signal won.
    async fn scrape_or_stand_down(&self, url: &str) -> Option<Result<String, MetricsScrapeError>> {
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => None,
            result = scrape_metrics(url, self.timeout, self.tls.clone()) => Some(result),
        }
    }

    /// Back off before the next attempt, unless the process is stopping.
    ///
    /// Waiting out a backoff is the easiest place to be stuck during shutdown,
    /// and the least excusable. Returns whether the wait completed.
    async fn wait_before_retry(&self, peer: &str, attempt: u32, started: Instant) -> bool {
        let wait = backoff(self.backoff, attempt, peer);
        let remaining = self.budget.saturating_sub(started.elapsed());
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => false,
            () = tokio::time::sleep(wait.min(remaining)) => true,
        }
    }

    /// Attempt until one succeeds, the budget runs out, or retrying is pointless.
    async fn attempt_until(&self, peer: &str, url: &str, started: Instant) -> (PollOutcome, Option<String>) {
        let attempts = self.attempts.max(1);
        let mut outcome = PollOutcome::Transport;
        for attempt in 0..attempts {
            if self.shutdown.is_triggered() {
                return (PollOutcome::Cancelled, None);
            }
            if started.elapsed() >= self.budget {
                break;
            }

            let Some(scrape) = self.scrape_or_stand_down(url).await else {
                return (PollOutcome::Cancelled, None);
            };

            match scrape {
                Ok(text) => return (PollOutcome::Ok, Some(text)),
                Err(error) => {
                    outcome = classify(&error);
                    if attempt + 1 >= attempts || !outcome.is_retryable() {
                        tracing::warn!(site = %peer, outcome = outcome.as_str(), %error, "peer poll failed");
                        break;
                    }
                    crate::metrics::record_peer_retry(peer, outcome.as_str());
                    if !self.wait_before_retry(peer, attempt, started).await {
                        return (PollOutcome::Cancelled, None);
                    }
                },
            }
        }
        (outcome, None)
    }
}

impl PeerSignals for PollPeers {
    fn name(&self) -> &'static str {
        "poll"
    }

    fn collect<'poll>(&'poll self, sites: &'poll [PeerSite]) -> BoxFuture<'poll, BTreeMap<String, Vec<Observation>>> {
        Box::pin(async move {
            let collect_query = self
                .collect
                .iter()
                .map(|c| format!("collect[]={}", utf8_percent_encode(c, QUERY_VALUE)))
                .collect::<Vec<_>>()
                .join("&");

            let fetches = peer_urls(sites, &collect_query)
                .into_iter()
                .map(|(peer, url)| async move {
                    let body = self.poll_one(&peer, &url).await?;
                    let observations = retain_origin(parse(&body), &peer);
                    Some((peer, observations))
                });

            // Bounded fan-out. A peer that fails is absent from the result
            // rather than empty, so silence and nothing-to-report stay
            // distinguishable to the store.
            futures::stream::iter(fetches)
                .buffer_unordered(self.concurrency.max(1))
                .collect::<Vec<Option<(String, Vec<Observation>)>>>()
                .await
                .into_iter()
                .flatten()
                .collect()
        })
    }
}

/// Owns the accounting for one poll, however that poll ends.
///
/// A cancelled poll is recorded as cancelled rather than silently dropped,
/// because a round that stopped because the process is stopping is not the same
/// as a round that failed, and a graph that cannot tell them apart will show a
/// deployment as an outage.
struct PollGuard<'poll> {
    /// Peer being polled.
    peer: &'poll str,
    /// When the poll began.
    started: Instant,
    /// Threshold for counting the poll slow.
    slow_after: Duration,
    /// Set once the outcome has been recorded.
    recorded: bool,
}

impl<'poll> PollGuard<'poll> {
    /// Begin a poll, counting it in flight.
    fn enter(peer: &'poll str, slow_after: Duration) -> Self {
        crate::metrics::peer_polls_in_flight(1);
        Self {
            peer,
            started: Instant::now(),
            slow_after,
            recorded: false,
        }
    }

    /// Record a poll that ran to a conclusion.
    fn finish(&mut self, outcome: PollOutcome, bytes: usize) {
        self.record(outcome, bytes);
    }

    /// Record an outcome once and only once.
    fn record(&mut self, outcome: PollOutcome, bytes: usize) {
        if self.recorded {
            return;
        }
        self.recorded = true;
        crate::metrics::record_peer_poll(
            self.peer,
            outcome.as_str(),
            self.started.elapsed(),
            bytes,
            self.slow_after,
        );
        // Reachability is only claimed either way when the poll actually
        // concluded. A cancelled poll learned nothing about the peer, and
        // marking it down would turn a rolling restart into a false outage.
        if outcome != PollOutcome::Cancelled {
            crate::metrics::set_peer_collection_up(self.peer, outcome == PollOutcome::Ok, SystemTime::now());
        }
    }
}

impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        crate::metrics::peer_polls_in_flight(-1);
        self.record(PollOutcome::Cancelled, 0);
    }
}

/// Build the request URL for each peer.
///
/// No `target`: it names a provider, not a site, so passing a site name matches
/// nothing and the peer answers empty. Scoping a peer's answer is the
/// publisher's job; the reader re-checks the site label on receipt.
fn peer_urls(sites: &[PeerSite], collect_query: &str) -> Vec<(String, String)> {
    sites
        .iter()
        .map(|site| {
            let url = if collect_query.is_empty() {
                site.url.clone()
            } else {
                format!("{}?{collect_query}", site.url)
            };
            (site.name.clone(), url)
        })
        .collect()
}

/// Keep only the observations a peer made itself.
///
/// A relayed copy is dropped rather than trusted, so every site's data reaches
/// this one from the site that observed it. The publisher already scopes what it
/// offers; checking the label on receipt means a reader does not depend on every
/// peer having done so.
fn retain_origin(observations: Vec<Observation>, peer: &str) -> Vec<Observation> {
    observations
        .into_iter()
        .filter(|o| o.labels.get(SITE_LABEL).is_some_and(|s| s == peer))
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn a_colon_in_a_metric_name_is_a_name_not_a_delimiter() {
        // Every vLLM metric is named this way. The obvious parser crates match
        // the name with \w+, drop the whole line, and report a short scrape.
        let o = parse("vllm:num_requests_waiting{model=\"a\"} 7");
        let first = o.first().expect("colon-named metric must survive");
        assert_eq!(first.metric, "vllm:num_requests_waiting");
        assert_eq!(first.labels.get("model").map(String::as_str), Some("a"));
        assert!((first.value - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_brace_inside_a_label_value_does_not_end_the_label_set() {
        let o = parse(r#"m{note="has } brace",k="v"} 3"#);
        let first = o.first().expect("line must parse");
        assert_eq!(first.labels.get("note").map(String::as_str), Some("has } brace"));
        assert_eq!(first.labels.get("k").map(String::as_str), Some("v"));
    }

    #[test]
    fn an_escaped_quote_stays_inside_the_value() {
        let o = parse(r#"m{a="say \"hi\"",b="c"} 1"#);
        let first = o.first().expect("line must parse");
        assert_eq!(first.labels.get("a").map(String::as_str), Some(r#"say "hi""#));
        assert_eq!(first.labels.get("b").map(String::as_str), Some("c"));
    }

    #[test]
    fn a_type_declared_after_its_samples_still_applies() {
        // Reading in one pass would admit this counter.
        let o = parse("c_total 5\n# TYPE c_total counter\n");
        assert!(o.is_empty(), "late TYPE must still drop the counter: {o:?}");
    }

    #[test]
    fn a_trailing_timestamp_is_not_read_as_the_value() {
        let o = parse("m{a=\"b\"} 2 1700000000000");
        let first = o.first().expect("line must parse");
        assert!((first.value - 2.0).abs() < f64::EPSILON, "value, not timestamp");
    }

    #[test]
    fn a_peer_is_not_asked_for_a_target() {
        // target names one provider, and a peer serves from a store keyed by
        // provider. Asking for a site by name matched nothing, so every peer
        // answered with an empty body and no site ever relayed another.
        let sites = vec![PeerSite {
            name: "pool-b".to_owned(),
            url: "http://10.0.0.2:9091/metrics".to_owned(),
        }];
        let urls = peer_urls(&sites, "");
        assert_eq!(
            urls.first().map(|(_, u)| u.as_str()),
            Some("http://10.0.0.2:9091/metrics"),
            "the whole site is asked for, with no target"
        );
    }

    #[test]
    fn requested_signal_names_still_reach_the_peer() {
        let sites = vec![PeerSite {
            name: "pool-b".to_owned(),
            url: "http://10.0.0.2:9091/metrics".to_owned(),
        }];
        let urls = peer_urls(&sites, "collect[]=queue");
        assert_eq!(
            urls.first().map(|(_, u)| u.as_str()),
            Some("http://10.0.0.2:9091/metrics?collect[]=queue")
        );
    }

    #[test]
    fn a_trust_failure_is_not_retried() {
        // Retrying a certificate problem burns the budget a transient failure
        // needs, and it will fail identically until somebody replaces a cert.
        assert!(!PollOutcome::Tls.is_retryable());
        assert!(!PollOutcome::Config.is_retryable());
    }

    #[test]
    fn a_peer_declining_to_answer_is_not_retried() {
        // A 403 is the scope rule working. Retrying it turns one misconfigured
        // reader into a burst against a peer that already said no.
        assert!(!PollOutcome::ClientError.is_retryable());
    }

    #[test]
    fn transient_failures_are_retried() {
        for outcome in [
            PollOutcome::Timeout,
            PollOutcome::Refused,
            PollOutcome::Transport,
            PollOutcome::ServerError,
        ] {
            assert!(outcome.is_retryable(), "{} is worth another attempt", outcome.as_str());
        }
    }

    #[test]
    fn a_status_is_split_at_five_hundred() {
        let client = classify(&MetricsScrapeError::NonOkStatus {
            status: 403,
            url: "https://peer/metrics".to_owned(),
        });
        let server = classify(&MetricsScrapeError::NonOkStatus {
            status: 503,
            url: "https://peer/metrics".to_owned(),
        });
        assert_eq!(client, PollOutcome::ClientError, "the peer answered and refused");
        assert_eq!(server, PollOutcome::ServerError, "the peer is having a bad time");
        assert!(!client.is_retryable() && server.is_retryable());
    }

    #[test]
    fn a_refused_connection_is_told_apart_from_a_trust_failure() {
        // Both arrive as transport errors and call for opposite responses, so
        // the chain is walked rather than the message matched.
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        assert_eq!(classify_transport(&refused), PollOutcome::Refused);

        let tls = rustls::Error::DecryptError;
        assert_eq!(classify_transport(&tls), PollOutcome::Tls);
    }

    #[test]
    fn outcome_labels_are_stable() {
        // Dashboards and alert rules are written against these strings, so a
        // rename is a break for anyone already watching.
        assert_eq!(PollOutcome::Ok.as_str(), "ok");
        assert_eq!(PollOutcome::Timeout.as_str(), "timeout");
        assert_eq!(PollOutcome::Refused.as_str(), "refused");
        assert_eq!(PollOutcome::Tls.as_str(), "tls");
        assert_eq!(PollOutcome::ClientError.as_str(), "client_error");
        assert_eq!(PollOutcome::ServerError.as_str(), "server_error");
    }

    #[test]
    fn backoff_grows_and_then_stops_growing() {
        let base = Duration::from_millis(50);
        let delays: Vec<_> = (0..8).map(|a| backoff(base, a, "west")).collect();
        for pair in delays.windows(2).take(5) {
            let [first, second] = pair else { continue };
            assert!(second > first, "each attempt waits longer: {first:?} then {second:?}");
        }
        let last = delays.last().copied().unwrap_or_default();
        assert!(
            last <= base * 64,
            "and the growth is capped so a retry cannot outlive a round: {last:?}"
        );
    }

    #[test]
    fn two_peers_failing_together_do_not_retry_together() {
        // Fifty sites cut off by one partition would otherwise retry in step
        // and arrive together when it heals, which is the herd the design is
        // trying not to build.
        let base = Duration::from_millis(50);
        assert_ne!(
            backoff(base, 2, "east"),
            backoff(base, 2, "west"),
            "the spread is derived from the peer name"
        );
    }

    #[test]
    fn the_same_peer_backs_off_the_same_way_every_run() {
        // Spread, not randomness: a failing run has to be repeatable.
        let base = Duration::from_millis(50);
        assert_eq!(backoff(base, 3, "north"), backoff(base, 3, "north"));
    }

    const QUEUE: &str = "llm_d_epp_average_queue_size";

    fn scraped() -> Vec<Observation> {
        parse(&format!(
            "# HELP {QUEUE} depth\n# TYPE {QUEUE} gauge\n{QUEUE}{{name=\"pool-a\"}} 3\n"
        ))
    }

    #[test]
    fn comments_and_types_are_not_samples() {
        assert_eq!(scraped().len(), 1, "one sample, no comment lines");
    }

    #[test]
    fn a_provider_cannot_set_the_labels_this_site_attributes() {
        let observations = parse(&format!(r#"{QUEUE}{{grid_site="evil",name="pool-a"}} 3"#));
        let out = attribute(observations, "east", "pool-a");
        let o = out.first().expect("one observation");
        assert_eq!(
            o.labels.get(SITE_LABEL).map(String::as_str),
            Some("east"),
            "this site decides"
        );
        assert_eq!(
            o.labels.get("exported_grid_site").map(String::as_str),
            Some("evil"),
            "the provider's value is kept under an exported name"
        );
        assert_eq!(
            o.labels.get("name").map(String::as_str),
            Some("pool-a"),
            "unrelated labels survive"
        );
    }

    #[test]
    fn a_label_value_containing_a_space_survives_a_round_trip() {
        let out = attribute(parse(r#"queue{path="/a b"} 3"#), "east", "p");
        let mut line = String::new();
        render_sample(&mut line, out.first().expect("one"), 1_700_000_000_000);
        assert!(line.contains(r#"path="/a b""#), "the space is preserved: {line}");
        // The consumer splits the timestamp off the end, then the value, so a
        // space inside a label must not shift either token.
        let (head, timestamp) = line.rsplit_once(' ').expect("timestamp token");
        let (_, value) = head.rsplit_once(' ').expect("value token");
        assert_eq!(timestamp, "1700000000000", "timestamp is the last token: {line}");
        assert_eq!(value, "3", "value is the token before it: {line}");
    }

    #[test]
    fn aggregates_are_not_republished() {
        // Type declared after the samples, which is how a naive exporter emits
        // it and how the parser is forced to treat them as untyped.
        let text = "h_bucket{le=\"1\"} 1\nh_sum 2\nh_count 1\nq 4\n";
        let names: Vec<String> = parse(text).into_iter().map(|o| o.metric).collect();
        assert_eq!(names, vec!["q".to_owned()], "only the plain gauge survives: {names:?}");
    }

    #[test]
    fn target_selects_one_provider() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([
                ("pool-a".to_owned(), attribute(scraped(), "east", "pool-a")),
                ("pool-b".to_owned(), attribute(scraped(), "east", "pool-b")),
            ]),
            Duration::from_secs(60),
        );
        let (one, _) = store.render(Some("pool-a"), &[]);
        assert!(one.contains(r#"grid_provider="pool-a""#), "the asked-for target: {one}");
        assert!(!one.contains(r#"grid_provider="pool-b""#), "and only that one: {one}");
        assert_eq!(
            store.render(None, &[]).0.lines().count(),
            2,
            "no target returns every target"
        );
    }

    #[test]
    fn collect_selects_signals_by_name() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), attribute(scraped(), "east", "pool-a"))]),
            Duration::from_secs(60),
        );
        assert_eq!(
            store.render(None, &[QUEUE.to_owned()]).0.lines().count(),
            1,
            "the named signal"
        );
        assert_eq!(
            store.render(None, &["absent".to_owned()]).0.lines().count(),
            0,
            "and nothing else"
        );
    }

    #[test]
    fn age_reports_the_oldest_value_in_the_response() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), scraped())]),
            Duration::from_secs(60),
        );
        let (_, age) = store.render(None, &[]);
        assert!(
            age < Duration::from_secs(1),
            "a value just collected is reported as new: {age:?}"
        );
    }

    #[test]
    fn a_target_nothing_refreshes_stops_being_served() {
        let store = SignalStore::new();
        store.refresh(BTreeMap::from([("pool-a".to_owned(), scraped())]), Duration::ZERO);
        assert_eq!(store.render(None, &[]).0, "", "absence is what says it is stale");
    }

    #[test]
    fn a_target_absent_from_a_refresh_is_kept_until_it_expires() {
        let store = SignalStore::new();
        store.refresh(
            BTreeMap::from([("pool-a".to_owned(), scraped())]),
            Duration::from_secs(60),
        );
        store.refresh(
            BTreeMap::from([("pool-b".to_owned(), scraped())]),
            Duration::from_secs(60),
        );
        assert_eq!(store.targets().len(), 2, "one failed scrape must not erase a target");
    }
    #[test]
    fn non_finite_values_never_reach_the_store() {
        let scraped = "\
q{pool=\"a\"} NaN
q{pool=\"b\"} +Inf
q{pool=\"c\"} -Inf
q{pool=\"d\"} 0.35
";
        let kept = parse(scraped);
        assert_eq!(kept.len(), 1, "only the finite sample survives: {kept:?}");
        let kept_value = kept.first().map(|o| o.value).expect("one sample kept");
        assert!((kept_value - 0.35).abs() < f64::EPSILON);
        assert!(
            kept.iter().all(|o| o.value.is_finite()),
            "a non-finite score outranks every finite one at the consumer"
        );
    }
}
