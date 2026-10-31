//! Durable gateway age evidence, independent of peer polling and reconciliation.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, to_bytes},
    response::{IntoResponse as _, Response},
};
use grid_signals::feedback::{ACK_EPOCH, ACK_SEQUENCE, IngestReport, REPORT_HEADERS};
use http::{HeaderMap, StatusCode};
use kube::{
    Api, Client,
    api::{ListParams, Patch, PatchParams},
};

use crate::{
    crd::{
        grid_network::{GatewayAgeReport, GridNetwork},
        inference_provider::{Condition, ConditionStatus},
    },
    metrics::{GatewayFeedbackEvent, record_gateway_feedback, record_gateway_feedback_heartbeat},
    shutdown::Shutdown,
};

/// Gateway condition type; True means actual fresh rejection evidence.
pub const GATEWAY_AGES_REJECTED: &str = "GatewayAgesRejected";
/// Status owner, separate from reconciliation and peer diagnostics.
pub const FIELD_MANAGER: &str = "grid-operator-gateway-signals";
/// Monotonic evidence freshness; expiration never proves recovery.
const FRESHNESS: Duration = Duration::from_secs(300);
/// Total request-body, admission, lookup, and publication budget.
const WRITE_BUDGET: Duration = Duration::from_secs(5);
/// Maximum durable reporter identities, including unresolved retired processes.
const MAX_REPORTERS: usize = 128;

/// Serialized publication with bounded process-local monotonic receipt tracking.
#[derive(Clone)]
pub struct GatewayFeedback {
    /// API client for the local network status.
    client: Client,
    /// Serializes the CAS flow and receipt commit under the request budget.
    publication: Arc<tokio::sync::Semaphore>,
    /// Updated only after durable acceptance; empty after operator restart.
    receipts: Arc<Mutex<BTreeMap<String, Receipt>>>,
    /// Cancels publication when the operator terminates.
    shutdown: Shutdown,
}

impl GatewayFeedback {
    /// Start with conservative freshness for all previously persisted evidence.
    #[must_use]
    pub fn new(client: Client, shutdown: Shutdown) -> Self {
        Self {
            client,
            publication: Arc::new(tokio::sync::Semaphore::new(1)),
            receipts: Arc::default(),
            shutdown,
        }
    }

    /// Accept only the connection's cryptographically proven local scope.
    pub async fn respond(&self, local: bool, request: http::Request<Body>) -> Response {
        if !local {
            record_gateway_feedback(GatewayFeedbackEvent::Unauthorized);
            return StatusCode::FORBIDDEN.into_response();
        }
        tokio::select! {
            biased;
            () = self.shutdown.triggered() => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            result = tokio::time::timeout(WRITE_BUDGET, self.publish(request)) => {
                result.unwrap_or_else(|_| StatusCode::SERVICE_UNAVAILABLE.into_response())
            },
        }
    }

    /// Parse and publish one report before acknowledging its epoch and sequence.
    async fn publish(&self, request: http::Request<Body>) -> Response {
        let report = match read_report(request).await {
            Ok(report) => report,
            Err(status) => return status.into_response(),
        };
        let Ok(_publication) = self.publication.acquire().await else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        let receipts = self.receipts.lock().unwrap_or_else(PoisonError::into_inner).clone();
        let api: Api<GridNetwork> = Api::all(self.client.clone());
        for _attempt in 0..3 {
            match attempt(&api, &report, &receipts).await {
                Ok((update, instant)) => {
                    let mut accepted_receipts = self.receipts.lock().unwrap_or_else(PoisonError::into_inner);
                    match update.outcome {
                        Outcome::Changed => {
                            accepted_receipts.insert(report.epoch.clone(), Receipt::new(instant, report.sequence));
                        },
                        Outcome::Heartbeat => {
                            if let Some(receipt) = accepted_receipts.get_mut(&report.epoch) {
                                receipt.sequence = report.sequence;
                            }
                        },
                        Outcome::Duplicate | Outcome::Refused => {},
                    }
                    accepted_receipts.retain(|epoch, _| update.evidence.reports.iter().any(|row| &row.epoch == epoch));
                    drop(accepted_receipts);
                    return acknowledgment(&report, update.outcome);
                },
                Err(StatusCode::CONFLICT) => {},
                Err(status) => return status.into_response(),
            }
        }
        record_gateway_feedback(GatewayFeedbackEvent::ConflictExhausted);
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

/// One CAS attempt; concurrent status writes are retried from a new live lookup.
async fn attempt(
    api: &Api<GridNetwork>,
    report: &IngestReport,
    receipts: &BTreeMap<String, Receipt>,
) -> Result<(Update, Instant), StatusCode> {
    let network = Box::pin(read_network(api)).await?;
    let now = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_format| StatusCode::SERVICE_UNAVAILABLE)?;
    let instant = Instant::now();
    let update = fold(&network, report, &now, receipts, instant).map_err(|()| StatusCode::BAD_REQUEST)?;
    let Some(patch) = &update.patch else {
        record_gateway_feedback_heartbeat();
        return Ok((update, instant));
    };
    let name = network
        .metadata
        .name
        .as_deref()
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    let applied = Box::pin(api.patch_status(name, &PatchParams::apply(FIELD_MANAGER).force(), &Patch::Apply(patch)))
        .await
        .map_err(|error| write_error(&error))?;
    if !durable(&applied, &update.evidence) {
        record_gateway_feedback(GatewayFeedbackEvent::WriteFailure);
        tracing::warn!("gateway feedback response did not contain the intended evidence");
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok((update, instant))
}

/// Echo exact report identity only after storage and capacity acceptance.
fn acknowledgment(report: &IngestReport, outcome: Outcome) -> Response {
    if outcome == Outcome::Refused {
        record_gateway_feedback(GatewayFeedbackEvent::Overflow);
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    (
        StatusCode::NO_CONTENT,
        [
            (ACK_EPOCH, report.epoch.clone()),
            (ACK_SEQUENCE, report.sequence.to_string()),
        ],
    )
        .into_response()
}

/// Bound the body before admitting the request to the serialized status writer.
async fn read_report(request: http::Request<Body>) -> Result<IngestReport, StatusCode> {
    let (parts, body) = request.into_parts();
    let report = parse(&parts.headers).ok_or(StatusCode::BAD_REQUEST)?;
    to_bytes(body, 0).await.map_err(|_body| StatusCode::PAYLOAD_TOO_LARGE)?;
    Ok(report)
}

/// Resolve exactly one live network, keeping the status API future on the heap.
async fn read_network(api: &Api<GridNetwork>) -> Result<Box<GridNetwork>, StatusCode> {
    let mut items = Box::pin(api.list(&ListParams::default().limit(2)))
        .await
        .map_err(|_read| StatusCode::SERVICE_UNAVAILABLE)?
        .items
        .into_iter();
    let network = items.next().ok_or(StatusCode::SERVICE_UNAVAILABLE)?;
    if items.next().is_some() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(Box::new(network))
}

/// Retry only resourceVersion races; other API failures retain client evidence.
fn write_error(error: &kube::Error) -> StatusCode {
    if matches!(error, kube::Error::Api(error) if error.code == 409) {
        record_gateway_feedback(GatewayFeedbackEvent::Conflict);
        StatusCode::CONFLICT
    } else {
        record_gateway_feedback(GatewayFeedbackEvent::WriteFailure);
        tracing::warn!(%error, "gateway age feedback was not persisted");
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Strict single-value parsing; no field is trusted before its scalar bound.
fn parse(headers: &HeaderMap) -> Option<IngestReport> {
    let values: Option<Vec<String>> = REPORT_HEADERS
        .iter()
        .map(|name| {
            let mut all = headers.get_all(*name).iter();
            let value = all.next()?.to_str().ok()?;
            (all.next().is_none() && value.len() <= 128).then(|| value.to_owned())
        })
        .collect();
    let [version, replica, epoch, sequence, skew, rejected] = <[String; 6]>::try_from(values?).ok()?;
    if version != "1" {
        return None;
    }
    let report = IngestReport {
        replica,
        epoch,
        sequence: sequence.parse().ok()?,
        skew: skew.parse().ok()?,
        rejected: rejected.parse().ok()?,
    };
    (report.valid() && report.sequence.to_string() == sequence && report.skew.to_string() == skew).then_some(report)
}

/// Atomic desired status, with freshness committed only after publication.
struct Update {
    /// SSA document including the read resourceVersion; absent when nothing durable changed.
    patch: Option<serde_json::Value>,
    /// Structured desired evidence for verifying the storage response.
    evidence: Evidence,
    /// How the report relates to the retained evidence.
    outcome: Outcome,
}

/// What an accepted report did to its epoch's retained row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    /// New epoch, new evidence, or a clean row re-stamped past the receipt floor.
    Changed,
    /// Strictly newer clean report for a clean row; acknowledged without re-stamping.
    Heartbeat,
    /// Exact replay of the accepted head; nothing renews.
    Duplicate,
    /// Reporter capacity refused a new identity; the uncertainty is still published.
    Refused,
}

/// Process-local acceptance state for one epoch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Receipt {
    /// Monotonic time of the last durable change to the row.
    accepted: Instant,
    /// Highest acknowledged sequence, heartbeats included.
    sequence: i64,
}

impl Receipt {
    /// A receipt whose row was just written at `accepted`.
    const fn new(accepted: Instant, sequence: i64) -> Self {
        Self { accepted, sequence }
    }

    /// Whether the row's last write is inside the freshness window at `instant`.
    fn fresh_at(&self, instant: Instant) -> bool {
        instant.saturating_duration_since(self.accepted) < FRESHNESS
    }
}

/// Separately owned durable evidence and its diagnostic condition.
struct Evidence {
    /// Retained process records.
    reports: Vec<GatewayAgeReport>,
    /// Capacity uncertainty requiring explicit verification.
    uncertain: bool,
    /// Aggregate condition from these records.
    condition: Condition,
}

/// Monotonic receipt context; missing receipt knowledge remains stale after restart.
struct Freshness<'receipt> {
    /// Live accepted receipts.
    receipts: &'receipt BTreeMap<String, Receipt>,
    /// Shared reading for this operation.
    instant: Instant,
    /// Only a strictly newer observation renews its epoch.
    epoch: Option<&'receipt str>,
}

impl Freshness<'_> {
    /// Whether this rejected row has a current monotonic receipt.
    fn contains(&self, row: &GatewayAgeReport) -> bool {
        self.epoch == Some(row.epoch.as_str())
            || self
                .receipts
                .get(&row.epoch)
                .is_some_and(|receipt| receipt.fresh_at(self.instant))
    }
}

/// Fold one observation using durable sequence state and monotonic live freshness.
fn fold(
    network: &GridNetwork,
    report: &IngestReport,
    now: &str,
    receipts: &BTreeMap<String, Receipt>,
    instant: Instant,
) -> Result<Update, ()> {
    let status = network.status.as_ref();
    let mut reports = status.map_or_else(Vec::new, |status| status.gateway_age_reports.clone());
    let outcome = observe(&mut reports, report, now, receipts.get(&report.epoch), instant)?;
    let uncertain = outcome == Outcome::Refused || status.is_some_and(|status| status.gateway_age_report_overflow);
    reports.sort_by(|left, right| left.epoch.cmp(&right.epoch));
    let freshness = Freshness {
        receipts,
        instant,
        epoch: (outcome == Outcome::Changed).then_some(report.epoch.as_str()),
    };
    let condition = condition(network, &reports, uncertain, now, &freshness);
    let evidence = Evidence {
        reports,
        uncertain,
        condition,
    };
    // A heartbeat still writes when expiry or overflow moved the condition.
    let write = outcome != Outcome::Heartbeat || !durable(network, &evidence);
    Ok(Update {
        patch: write.then(|| patch(network, &evidence)),
        evidence,
        outcome,
    })
}

/// SSA document for `evidence` against the read resourceVersion.
fn patch(network: &GridNetwork, evidence: &Evidence) -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "grid.praxis.fast/v1alpha1", "kind": "GridNetwork",
        "metadata": { "name": network.metadata.name, "resourceVersion": network.metadata.resource_version },
        "status": {
            "gatewayAgeReports": evidence.reports,
            "gatewayAgeReportOverflow": evidence.uncertain,
            "conditions": [evidence.condition],
        }
    })
}

/// Admit a new identity only by reclaiming clean bookkeeping.
fn observe(
    reports: &mut Vec<GatewayAgeReport>,
    report: &IngestReport,
    now: &str,
    receipt: Option<&Receipt>,
    instant: Instant,
) -> Result<Outcome, ()> {
    if let Some(held) = reports.iter_mut().find(|held| held.epoch == report.epoch) {
        return replace(held, report, now, receipt, instant);
    }
    if reports.len() >= MAX_REPORTERS {
        reports.sort_by(|left, right| (&left.received_at, &left.epoch).cmp(&(&right.received_at, &right.epoch)));
        let Some(position) = reports.iter().position(|held| !held.rejected) else {
            return Ok(Outcome::Refused);
        };
        reports.remove(position);
    }
    reports.push(row(report, report.rejected || report.skew > 0, now));
    Ok(Outcome::Changed)
}

/// Reject replay or changed duplicates; counter rises override a clean claim.
///
/// The acknowledged head is the durable sequence or the receipt's, whichever is higher, so a
/// heartbeat that was never written still refuses replay below it.
fn replace(
    held: &mut GatewayAgeReport,
    report: &IngestReport,
    now: &str,
    receipt: Option<&Receipt>,
    instant: Instant,
) -> Result<Outcome, ()> {
    let acknowledged = receipt
        .map_or(held.sequence, |receipt| receipt.sequence)
        .max(held.sequence);
    if held.replica != report.replica || report.sequence < acknowledged || report.skew < held.skew {
        return Err(());
    }
    if report.sequence == acknowledged {
        return (report.skew == held.skew && report.rejected == held.reported_rejected)
            .then_some(Outcome::Duplicate)
            .ok_or(());
    }
    let clean = !report.rejected && !held.rejected && report.skew == held.skew;
    if clean && receipt.is_some_and(|receipt| receipt.fresh_at(instant)) {
        return Ok(Outcome::Heartbeat);
    }
    *held = row(report, report.rejected || report.skew > held.skew, now);
    Ok(Outcome::Changed)
}

/// Preserve transition time while updating diagnostic detail and observed generation.
fn condition(
    network: &GridNetwork,
    reports: &[GatewayAgeReport],
    uncertain: bool,
    now: &str,
    freshness: &Freshness<'_>,
) -> Condition {
    let rejecting = reports.iter().filter(|held| held.rejected).collect::<Vec<_>>();
    let (standing, reason) = if rejecting.iter().any(|held| freshness.contains(held)) {
        (ConditionStatus::True, "GatewayAgesRejected")
    } else if uncertain || !rejecting.is_empty() {
        (ConditionStatus::Unknown, "GatewayEvidenceStale")
    } else {
        (ConditionStatus::False, "GatewayAgesAccepted")
    };
    let labels = rejecting
        .iter()
        .take(10)
        .map(|held| held.replica.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "{} unresolved gateway processes ({labels}); reporter capacity uncertainty: {uncertain}",
        rejecting.len()
    );
    let held = network.status.as_ref().and_then(|status| {
        status
            .conditions
            .iter()
            .find(|held| held.type_ == GATEWAY_AGES_REJECTED)
    });
    Condition {
        type_: GATEWAY_AGES_REJECTED.to_owned(),
        status: standing,
        reason: reason.to_owned(),
        message,
        last_transition_time: held
            .filter(|held| held.status == standing)
            .map_or_else(|| now.to_owned(), |held| held.last_transition_time.clone()),
        observed_generation: network.metadata.generation,
    }
}

/// Acknowledgment requires the response to contain the intended durable evidence.
fn durable(network: &GridNetwork, evidence: &Evidence) -> bool {
    network.status.as_ref().is_some_and(|status| {
        status.gateway_age_reports == evidence.reports
            && status.gateway_age_report_overflow == evidence.uncertain
            && status
                .conditions
                .iter()
                .find(|condition| condition.type_ == GATEWAY_AGES_REJECTED)
                == Some(&evidence.condition)
    })
}

/// Preserve the original flag separately from counter-derived rejection.
fn row(report: &IngestReport, rejected: bool, now: &str) -> GatewayAgeReport {
    GatewayAgeReport {
        replica: report.replica.clone(),
        epoch: report.epoch.clone(),
        sequence: report.sequence,
        skew: report.skew,
        rejected,
        reported_rejected: report.rejected,
        received_at: now.to_owned(),
    }
}

#[cfg(test)]
#[expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "tests and integration fixtures"
)]
mod tests {
    use super::*;

    #[test]
    fn clean_other_replica_and_replacement_epoch_cannot_clear_bad() {
        let mut network = network();
        let instant = Instant::now();
        let first = report("a", 1, 1, true);
        let update = fold(&network, &first, "t0", &BTreeMap::new(), instant).expect("bad");
        apply(&mut network, &update);
        let receipts = BTreeMap::from([(first.epoch, Receipt::new(instant, 1))]);
        let other = fold(&network, &report("b", 1, 0, false), "t1", &receipts, instant).expect("other");
        assert_eq!(
            standing(&other),
            "True",
            "another replica's clean report cannot clear fresh rejection"
        );
        apply(&mut network, &other);
        let restart = fold(&network, &report("c", 1, 0, false), "t2", &BTreeMap::new(), instant).expect("restart");
        assert_eq!(standing(&restart), "Unknown", "new epoch cannot refute old epoch");
        apply(&mut network, &restart);
        let recovered = fold(&network, &report("a", 2, 1, false), "t3", &receipts, instant).expect("recovery");
        assert_eq!(
            standing(&recovered),
            "False",
            "newer clean evidence from the rejecting epoch proves recovery"
        );
    }

    #[test]
    fn counter_increase_wins_and_exact_duplicates_never_refresh() {
        let mut network = network();
        let report = report("a", 1, 3, false);
        let instant = Instant::now();
        let update = fold(&network, &report, "t0", &BTreeMap::new(), instant).expect("counter delta");
        assert_eq!(
            standing(&update),
            "True",
            "new skew takes precedence over a claimed clean observation"
        );
        apply(&mut network, &update);
        let duplicate = fold(&network, &report, "t1", &BTreeMap::new(), instant).expect("exact duplicate");
        assert_eq!(
            duplicate.outcome,
            Outcome::Duplicate,
            "an exact replay must not renew the receipt"
        );
        assert_eq!(
            standing(&duplicate),
            "Unknown",
            "restart cannot renew receipt from a duplicate"
        );
        let conflicting = IngestReport {
            rejected: true,
            ..report.clone()
        };
        assert!(
            fold(&network, &conflicting, "t2", &BTreeMap::new(), instant).is_err(),
            "changed evidence at the same sequence must be rejected"
        );
        assert!(
            fold(
                &network,
                &IngestReport {
                    sequence: 2,
                    skew: 2,
                    ..report
                },
                "t2",
                &BTreeMap::new(),
                instant
            )
            .is_err(),
            "a newer sequence must not regress the cumulative skew counter"
        );
    }

    #[test]
    fn expiry_retains_uncertainty_and_overflow_cannot_be_cleared_by_client() {
        let mut network = network();
        let instant = Instant::now();
        for number in 0..MAX_REPORTERS {
            let report = IngestReport {
                epoch: format!("{number:032x}"),
                ..report("a", 1, 1, true)
            };
            let update = fold(&network, &report, "t0", &BTreeMap::new(), instant).expect("bounded bad");
            apply(&mut network, &update);
        }
        let overflow =
            fold(&network, &report("f", 1, 1, true), "t1", &BTreeMap::new(), instant).expect("overflow flag");
        assert!(
            overflow.outcome == Outcome::Refused && overflow.evidence.reports.len() == MAX_REPORTERS,
            "overflow retains all bounded unresolved evidence"
        );
        assert_eq!(
            standing(&overflow),
            "Unknown",
            "capacity exhaustion cannot claim complete rejection coverage"
        );
        apply(&mut network, &overflow);
        for number in 0..MAX_REPORTERS {
            let report = IngestReport {
                epoch: format!("{number:032x}"),
                ..report("a", 2, 1, false)
            };
            let update = fold(&network, &report, "t2", &BTreeMap::new(), instant).expect("existing recovery");
            apply(&mut network, &update);
        }
        let held = fold(&network, &report("f", 1, 0, false), "t3", &BTreeMap::new(), instant).expect("new admitted");
        assert_eq!(
            standing(&held),
            "Unknown",
            "coverage overflow requires explicit verification"
        );
    }

    #[test]
    fn monotonic_expiry_and_message_updates_never_claim_recovery() {
        let mut network = network();
        let report = report("a", 2, 1, true);
        let instant = Instant::now();
        let first = fold(&network, &report, "t0", &BTreeMap::new(), instant).expect("bad");
        apply(&mut network, &first);
        let receipts = BTreeMap::from([(report.epoch.clone(), Receipt::new(instant, 2))]);
        let expired = instant.checked_add(FRESHNESS).expect("bounded instant");
        let stale = fold(&network, &report, "t1", &receipts, expired).expect("duplicate");
        assert_eq!(standing(&stale), "Unknown", "receipt expiry cannot prove recovery");
        assert!(
            stale.evidence.reports[0].rejected,
            "expired rejection remains durable evidence"
        );
        assert!(
            fold(
                &network,
                &IngestReport { sequence: 1, ..report },
                "t2",
                &receipts,
                expired
            )
            .is_err(),
            "a stale lower sequence must not replace retained evidence"
        );
    }

    #[test]
    fn clean_heartbeat_keeps_the_row_until_the_receipt_floor_or_a_condition_change() {
        let mut network = network();
        let instant = Instant::now();
        let first = fold(&network, &report("a", 1, 0, false), "t0", &BTreeMap::new(), instant).expect("clean");
        assert_eq!(first.outcome, Outcome::Changed, "a new epoch is always written");
        apply(&mut network, &first);
        let receipts = BTreeMap::from([(first.evidence.reports[0].epoch.clone(), Receipt::new(instant, 1))]);
        let heartbeat = fold(&network, &report("a", 2, 0, false), "t1", &receipts, instant).expect("heartbeat");
        assert_eq!(
            heartbeat.outcome,
            Outcome::Heartbeat,
            "a newer clean report for a clean row"
        );
        assert!(
            heartbeat.patch.is_none(),
            "nothing durable changed, so nothing is written"
        );
        assert_eq!(
            heartbeat.evidence.reports[0].sequence, 1,
            "the row keeps its durable sequence and receipt time"
        );
        let replay = fold(&network, &report("a", 1, 0, false), "t2", &receipts, instant);
        assert!(
            replay.is_ok(),
            "the durable head is still accepted as an exact duplicate"
        );
        let acknowledged = BTreeMap::from([(first.evidence.reports[0].epoch.clone(), Receipt::new(instant, 2))]);
        assert!(
            fold(&network, &report("a", 1, 0, false), "t2", &acknowledged, instant).is_err(),
            "a replay below the acknowledged heartbeat is refused"
        );
        let expired = instant.checked_add(FRESHNESS).expect("bounded instant");
        let restamped = fold(&network, &report("a", 3, 0, false), "t3", &acknowledged, expired).expect("re-stamp");
        assert_eq!(
            restamped.outcome,
            Outcome::Changed,
            "past the receipt floor the clean row is written again"
        );
        assert!(restamped.patch.is_some(), "the re-stamp is written");
        assert_eq!(
            restamped.evidence.reports[0].sequence, 3,
            "the re-stamp carries the new head"
        );
        let counted = fold(&network, &report("a", 3, 1, false), "t3", &acknowledged, instant).expect("skew");
        assert_eq!(counted.outcome, Outcome::Changed, "a counter rise is never a heartbeat");
        assert!(counted.patch.is_some(), "a counter rise is written");
        assert!(
            counted.evidence.reports[0].rejected,
            "a counter rise is rejection evidence"
        );
    }

    #[test]
    fn heartbeat_still_writes_when_another_epoch_goes_stale() {
        let mut network = network();
        let instant = Instant::now();
        let bad = report("b", 1, 1, true);
        let rejected = fold(&network, &bad, "t0", &BTreeMap::new(), instant).expect("bad");
        apply(&mut network, &rejected);
        let clean = report("a", 1, 0, false);
        let receipts = BTreeMap::from([(bad.epoch.clone(), Receipt::new(instant, 1))]);
        let admitted = fold(&network, &clean, "t1", &receipts, instant).expect("clean");
        assert_eq!(standing(&admitted), "True", "the fresh rejection stands");
        apply(&mut network, &admitted);
        let expired = instant.checked_add(FRESHNESS).expect("bounded instant");
        let both = BTreeMap::from([
            (bad.epoch, Receipt::new(instant, 1)),
            (clean.epoch, Receipt::new(expired, 1)),
        ]);
        let unchanged = fold(&network, &report("a", 2, 0, false), "t2", &both, instant).expect("heartbeat");
        assert_eq!(unchanged.outcome, Outcome::Heartbeat);
        assert!(
            unchanged.patch.is_none(),
            "the condition is unchanged, so the heartbeat is silent"
        );
        let flipped = fold(&network, &report("a", 2, 0, false), "t3", &both, expired).expect("heartbeat");
        assert_eq!(flipped.outcome, Outcome::Heartbeat, "the clean row itself is unchanged");
        assert!(
            flipped.patch.is_some(),
            "the rejected epoch went stale, so the condition must be written"
        );
        assert_eq!(standing(&flipped), "Unknown", "expiry never proves recovery");
    }

    #[test]
    fn parsing_refuses_duplicates_versions_bounds_and_noncanonical_numbers() {
        let report = report("a", 1, 0, false);
        let mut headers = headers(&report);
        assert_eq!(parse(&headers), Some(report), "valid canonical headers must round trip");
        headers.append(REPORT_HEADERS[0], http::HeaderValue::from_static("1"));
        assert!(
            parse(&headers).is_none(),
            "duplicate report version headers must be rejected"
        );
        for (name, bad) in [
            (REPORT_HEADERS[0], "2"),
            (REPORT_HEADERS[3], "01"),
            (REPORT_HEADERS[4], "-1"),
            (REPORT_HEADERS[2], "bad"),
        ] {
            let mut invalid_headers = self::headers(&self::report("a", 1, 0, false));
            invalid_headers.insert(name, http::HeaderValue::from_str(bad).expect("header"));
            assert!(
                parse(&invalid_headers).is_none(),
                "invalid report header {name}={bad:?} must be rejected"
            );
        }
    }

    #[tokio::test]
    async fn durable_publication_sets_then_clears_and_preserves_peer_condition() {
        let (client, stored, patches) = mock_api(StorageMode::Normal);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let bad = report("a", 1, 1, true);
        let response = feedback.respond(true, request(&bad, "")).await;
        assert_eq!(
            response.status(),
            StatusCode::NO_CONTENT,
            "durable rejection must be acknowledged"
        );
        assert_eq!(
            response.headers().get(ACK_EPOCH).expect("epoch"),
            bad.epoch.as_str(),
            "the acknowledgment must identify the accepted epoch"
        );
        assert_eq!(
            stored.lock().expect("stored")["status"]["conditions"][1]["status"],
            "True",
            "durable bad evidence must set the gateway rejection condition"
        );
        let clean = report("a", 2, 1, false);
        assert_eq!(
            feedback.respond(true, request(&clean, "")).await.status(),
            StatusCode::NO_CONTENT,
            "durable newer clean evidence must be acknowledged"
        );
        let network = stored.lock().expect("stored");
        assert_eq!(
            network["status"]["conditions"][1]["status"], "False",
            "same-epoch clean evidence clears the rejection"
        );
        assert_eq!(
            network["status"]["conditions"][0]["type"], "PeerTimeValid",
            "gateway feedback preserves independent peer diagnostics"
        );
        assert_eq!(
            network["status"]["conditions"][0]["status"], "False",
            "gateway recovery preserves the peer timestamp rejection"
        );
        assert_eq!(
            network["status"]["conditions"][0]["reason"], "TimestampOutOfRange",
            "gateway recovery preserves the independent peer reason"
        );
        assert_eq!(
            network["status"]["phase"], "Active",
            "diagnostic feedback cannot change network phase"
        );
        drop(network);
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "bad and clean reports each require one durable write"
        );
    }

    #[tokio::test]
    async fn heartbeats_are_acknowledged_without_a_status_write() {
        let (client, stored, patches) = mock_api(StorageMode::Normal);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let heartbeats = heartbeat_count();
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 1, 0, false), ""))
                .await
                .status(),
            StatusCode::NO_CONTENT,
            "the first clean report is written and acknowledged"
        );
        for sequence in 2..=4 {
            let response = feedback
                .respond(true, request(&report("a", sequence, 0, false), ""))
                .await;
            assert_eq!(
                response.status(),
                StatusCode::NO_CONTENT,
                "a clean heartbeat is acknowledged"
            );
            assert_eq!(
                response.headers().get(ACK_SEQUENCE).expect("sequence"),
                sequence.to_string().as_str(),
                "the acknowledgment names the heartbeat's own sequence"
            );
        }
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "heartbeats never write status"
        );
        assert_eq!(
            heartbeat_count(),
            heartbeats.saturating_add(3),
            "each skipped write is counted"
        );
        assert_eq!(
            stored.lock().expect("stored")["status"]["gatewayAgeReports"][0]["sequence"],
            1,
            "the durable row keeps the written sequence"
        );
        assert_eq!(
            feedback.receipts.lock().expect("receipts")[&"a".repeat(32)].sequence,
            4,
            "the receipt tracks the acknowledged head"
        );
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 3, 0, false), ""))
                .await
                .status(),
            StatusCode::BAD_REQUEST,
            "a replay below the acknowledged head is refused"
        );
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 5, 1, true), ""))
                .await
                .status(),
            StatusCode::NO_CONTENT,
            "new evidence is written and acknowledged"
        );
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the counter rise is the second write"
        );
    }

    #[tokio::test]
    async fn repeated_conflicts_count_every_race_and_stop_after_three_attempts() {
        let (client, _stored, patches) = mock_api(StorageMode::Contention);
        let feedback = GatewayFeedback::new(client, Shutdown::never());
        let conflicts = metric_count("conflict");
        let exhausted = metric_count("conflict_exhausted");
        let response = feedback.respond(true, request(&report("a", 1, 0, false), "")).await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "contention is not acknowledged"
        );
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "retry budget is unchanged"
        );
        assert!(
            metric_count("conflict") >= conflicts.saturating_add(3),
            "all three races are counted"
        );
        assert!(
            metric_count("conflict_exhausted") > exhausted,
            "exhaustion is separately visible"
        );
    }

    #[tokio::test]
    async fn conflict_retries_against_live_version_and_exact_duplicate_keeps_receipt() {
        let (client, stored, patches) = mock_api(StorageMode::Conflict);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let clean = report("a", 1, 0, false);
        assert_eq!(
            feedback.respond(true, request(&clean, "")).await.status(),
            StatusCode::NO_CONTENT,
            "a successful live-version retry must acknowledge the accepted clean epoch"
        );
        let persisted = stored.lock().expect("stored").clone();
        assert_eq!(
            persisted["status"]["gatewayAgeReports"]
                .as_array()
                .expect("reports")
                .len(),
            2,
            "retry preserves another writer's rejection evidence"
        );
        assert_eq!(
            persisted["status"]["conditions"][1]["status"], "Unknown",
            "a clean incoming epoch cannot erase injected unresolved evidence"
        );
        let held = feedback.receipts.lock().expect("receipts").clone();
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one conflict requires exactly one retry"
        );
        assert_eq!(
            feedback.respond(true, request(&clean, "")).await.status(),
            StatusCode::NO_CONTENT,
            "an exact durable replay can be acknowledged again"
        );
        assert_eq!(
            *feedback.receipts.lock().expect("receipts"),
            held,
            "duplicate cannot renew freshness"
        );
    }

    #[tokio::test]
    async fn failed_or_changed_storage_never_acknowledges_or_commits_receipts() {
        for mode in [StorageMode::Changed, StorageMode::Failure] {
            let (client, _stored, _patches) = mock_api(mode);
            let (_trigger, shutdown) = crate::shutdown::Trigger::new();
            let feedback = GatewayFeedback::new(client, shutdown);
            assert_eq!(
                feedback
                    .respond(true, request(&report("a", 1, 1, true), ""))
                    .await
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "failed or mismatched persistence must not acknowledge evidence"
            );
            assert!(
                feedback.receipts.lock().expect("receipts").is_empty(),
                "failed persistence must not establish freshness"
            );
        }
    }

    #[tokio::test]
    async fn peer_scope_malformed_and_nonempty_reports_never_reach_storage() {
        let (client, _stored, patches) = mock_api(StorageMode::Normal);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let bad = report("a", 1, 1, true);
        assert_eq!(
            feedback.respond(false, request(&bad, "")).await.status(),
            StatusCode::FORBIDDEN,
            "a peer caller cannot report local gateway diagnostics"
        );
        assert_eq!(
            feedback.respond(true, request(&bad, "body")).await.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "ingest reports require an empty body"
        );
        let mut malformed = request(&bad, "");
        malformed
            .headers_mut()
            .append(REPORT_HEADERS[0], http::HeaderValue::from_static("1"));
        assert_eq!(
            feedback.respond(true, malformed).await.status(),
            StatusCode::BAD_REQUEST,
            "malformed authenticated reports must be rejected"
        );
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "unauthorized or invalid reports never write status"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_lookup_and_shutdown_are_bounded() {
        let client = Client::new(
            tower::service_fn(|_request: http::Request<kube::client::Body>| async {
                std::future::pending::<Result<http::Response<Body>, std::convert::Infallible>>().await
            }),
            "default",
        );
        let (trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        let started = tokio::time::Instant::now();
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 1, 1, true), ""))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "stalled persistence must time out without acknowledgment"
        );
        assert_eq!(
            started.elapsed(),
            WRITE_BUDGET,
            "the timeout bounds the entire publication"
        );
        assert_eq!(
            feedback.publication.available_permits(),
            1,
            "timeout releases publication admission"
        );
        trigger.trigger();
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 1, 1, true), ""))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "shutdown must reject new publication immediately"
        );
    }

    #[tokio::test]
    async fn closed_admission_never_reaches_storage() {
        let (client, _stored, patches) = mock_api(StorageMode::Normal);
        let (_trigger, shutdown) = crate::shutdown::Trigger::new();
        let feedback = GatewayFeedback::new(client, shutdown);
        feedback.publication.close();
        assert_eq!(
            feedback
                .respond(true, request(&report("a", 1, 1, true), ""))
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "closed publication admission must not accept or acknowledge a report"
        );
        assert_eq!(
            patches.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "closed admission never writes status"
        );
        assert!(
            feedback.receipts.lock().expect("receipts").is_empty(),
            "closed admission commits no receipts"
        );
    }

    /// Report request with an explicitly bounded body.
    fn request(report: &IngestReport, body: &str) -> http::Request<Body> {
        let mut request = http::Request::new(Body::from(body.to_owned()));
        *request.headers_mut() = headers(report);
        request
    }

    /// Independent storage failure modes for publication proofs.
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum StorageMode {
        /// Successfully store and return the patch.
        Normal,
        /// First write races another manager.
        Conflict,
        /// Returned evidence differs from the intended write.
        Changed,
        /// Every mocked write returns a resource-version conflict.
        Contention,
        /// Every write fails.
        Failure,
    }

    /// In-memory Kubernetes endpoint with CAS, failure, and response mutation fixtures.
    fn mock_api(
        mode: StorageMode,
    ) -> (
        Client,
        Arc<Mutex<serde_json::Value>>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mut fixture = serde_json::to_value(network()).expect("network");
        fixture["status"] = serde_json::json!({"phase":"Active","conditions":[{"type":"PeerTimeValid","status":"False","reason":"TimestampOutOfRange","message":"peer","lastTransitionTime":"2026-10-12T00:00:00Z"}]});
        let stored = Arc::new(Mutex::new(fixture));
        let patches = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let service = tower::service_fn({
            let (stored, patches) = (Arc::clone(&stored), Arc::clone(&patches));
            move |request: http::Request<kube::client::Body>| {
                let (stored, patches) = (Arc::clone(&stored), Arc::clone(&patches));
                async move {
                    use http_body_util::BodyExt as _;
                    if request.method() == http::Method::GET {
                        let network = stored.lock().expect("stored").clone();
                        return Ok::<_, std::convert::Infallible>(http::Response::new(Body::from(serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"GridNetworkList","items":[network]}).to_string())));
                    }
                    let count = patches.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if mode == StorageMode::Failure
                        || mode == StorageMode::Contention
                        || (mode == StorageMode::Conflict && count == 0)
                    {
                        if mode == StorageMode::Conflict {
                            let mut concurrent = stored.lock().expect("stored");
                            concurrent["metadata"]["resourceVersion"] = serde_json::json!("2");
                            concurrent["status"]["gatewayAgeReports"] =
                                serde_json::json!([row(&report("b", 7, 4, true), true, "2026-10-12T00:00:00Z")]);
                        }
                        let code = if mode == StorageMode::Failure { 500 } else { 409 };
                        let error = serde_json::json!({"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict","message":"fixture","code":code});
                        return Ok(http::Response::builder()
                            .status(code)
                            .body(Body::from(error.to_string()))
                            .expect("response"));
                    }
                    let bytes = request.into_body().collect().await.expect("body").to_bytes();
                    let patch: serde_json::Value = serde_json::from_slice(&bytes).expect("patch");
                    let mut network = stored.lock().expect("stored");
                    assert_eq!(
                        patch["metadata"]["resourceVersion"], network["metadata"]["resourceVersion"],
                        "CAS uses live version"
                    );
                    let peer = network["status"]["conditions"][0].clone();
                    network["status"] = patch["status"].clone();
                    network["status"]["phase"] = serde_json::json!("Active");
                    network["status"]["conditions"]
                        .as_array_mut()
                        .expect("conditions")
                        .insert(0, peer);
                    network["metadata"]["resourceVersion"] = serde_json::json!(count.saturating_add(2).to_string());
                    let mut response = network.clone();
                    drop(network);
                    if mode == StorageMode::Changed {
                        response["status"] = serde_json::json!({"phase":"Active"});
                    }
                    Ok(http::Response::new(Body::from(response.to_string())))
                }
            }
        });
        (Client::new(service, "default"), stored, patches)
    }

    // ---------------------------------------------------------------------------
    // Test Utilities
    // ---------------------------------------------------------------------------

    /// Current exported count for one bounded feedback event.
    fn metric_count(event: &str) -> u64 {
        let text = String::from_utf8(crate::metrics::encode_metrics()).expect("metrics text");
        let prefix = format!("grid_gateway_feedback_events_total{{event=\"{event}\"}} ");
        text.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .map_or(0, |value| value.parse().expect("integer counter"))
    }

    /// Current exported heartbeat count.
    fn heartbeat_count() -> u64 {
        let text = String::from_utf8(crate::metrics::encode_metrics()).expect("metrics text");
        text.lines()
            .find_map(|line| line.strip_prefix("grid_gateway_feedback_heartbeats_total "))
            .map_or(0, |value| value.parse().expect("integer counter"))
    }

    /// Named network with a CAS version.
    fn network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({"apiVersion":"grid.praxis.fast/v1alpha1","kind":"GridNetwork","metadata":{"name":"grid","resourceVersion":"1"},"spec":{"seeds":[],"gridId":"grid"}})).expect("network")
    }

    /// A bounded process observation.
    fn report(epoch: &str, sequence: i64, skew: i64, rejected: bool) -> IngestReport {
        IngestReport {
            replica: "gateway".to_owned(),
            epoch: epoch.repeat(32),
            sequence,
            skew,
            rejected,
        }
    }

    /// Durable application from the atomic patch.
    fn apply(network: &mut GridNetwork, update: &Update) {
        let patch = update.patch.as_ref().expect("patch");
        network.status = Some(serde_json::from_value(patch["status"].clone()).expect("status"));
    }

    /// Aggregate condition standing.
    fn standing(update: &Update) -> &str {
        update.patch.as_ref().expect("patch")["status"]["conditions"][0]["status"]
            .as_str()
            .expect("standing")
    }

    /// Scalar report request headers.
    fn headers(report: &IngestReport) -> HeaderMap {
        REPORT_HEADERS
            .into_iter()
            .zip(report.values())
            .map(|(name, value)| {
                (
                    http::HeaderName::from_static(name),
                    http::HeaderValue::from_str(&value).expect("header"),
                )
            })
            .collect()
    }
}
