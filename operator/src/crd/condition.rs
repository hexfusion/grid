//! Status conditions shared by every grid resource, in the `metav1.Condition` shape.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Whether a condition holds.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ConditionStatus {
    /// The condition holds.
    True,
    /// The condition does not hold.
    False,
    /// The operator cannot tell yet.
    Unknown,
}

impl From<bool> for ConditionStatus {
    fn from(holds: bool) -> Self {
        if holds { Self::True } else { Self::False }
    }
}

/// One observed condition, keyed by `type`.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Condition type, unique within the list.
    #[serde(rename = "type")]
    pub type_: String,

    /// Whether the condition holds.
    pub status: ConditionStatus,

    /// The `metadata.generation` this condition was computed from.
    #[serde(default)]
    pub observed_generation: i64,

    /// RFC 3339 time the status last changed.
    pub last_transition_time: String,

    /// Machine-readable reason.
    pub reason: String,

    /// Human-readable detail.
    #[serde(default)]
    pub message: String,
}

/// The status fields of a [`Condition`], without its transition time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observed {
    /// Condition type.
    pub type_: &'static str,
    /// Whether it holds.
    pub status: ConditionStatus,
    /// Machine-readable reason.
    pub reason: String,
    /// Human-readable detail.
    pub message: String,
}

impl Observed {
    /// An observed condition of `type_` with `status` and `reason`.
    #[must_use]
    pub fn new(type_: &'static str, status: ConditionStatus, reason: &str) -> Self {
        Self {
            type_,
            status,
            reason: reason.to_owned(),
            message: String::new(),
        }
    }

    /// Attach a human-readable message.
    #[must_use]
    pub fn with_message(mut self, message: String) -> Self {
        self.message = message;
        self
    }
}

/// Build the condition list for `observed`, keeping each previous transition time
/// whose status did not change, so an unchanged reconcile writes identical status.
#[must_use]
pub fn reconcile_conditions(
    previous: &[Condition],
    observed: Vec<Observed>,
    generation: i64,
    now: &str,
) -> Vec<Condition> {
    observed
        .into_iter()
        .map(|next| {
            let last_transition_time = previous
                .iter()
                .find(|old| old.type_ == next.type_ && old.status == next.status)
                .map_or_else(|| now.to_owned(), |old| old.last_transition_time.clone());
            Condition {
                type_: next.type_.to_owned(),
                status: next.status,
                observed_generation: generation,
                last_transition_time,
                reason: next.reason,
                message: next.message,
            }
        })
        .collect()
}

/// Conditions for `observed` at `generation`, stamped now.
#[must_use]
pub fn refresh(previous: Option<&[Condition]>, observed: Vec<Observed>, generation: i64) -> Vec<Condition> {
    reconcile_conditions(previous.unwrap_or_default(), observed, generation, &now_rfc3339())
}

/// The spec passed operator validation.
pub const ACCEPTED: &str = "Accepted";

/// A spec the operator refuses: the `Accepted=False` reason and a human-readable message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Rejection {
    /// Stable machine-readable reason.
    pub reason: &'static str,
    /// What to fix.
    pub message: String,
}

impl Rejection {
    /// A rejection with `reason` and `message`.
    #[must_use]
    pub fn new<M: Into<String>>(reason: &'static str, message: M) -> Self {
        Self {
            reason,
            message: message.into(),
        }
    }

    /// Whether `previous` already reports this rejection on `Accepted`, so no new event is due.
    #[must_use]
    pub fn already_reported(&self, previous: &[Condition]) -> bool {
        self.reported_on(previous, ACCEPTED)
    }

    /// Whether `previous` already reports this as condition `type_` being `False`.
    #[must_use]
    pub fn reported_on(&self, previous: &[Condition], type_: &str) -> bool {
        find(previous, type_)
            .is_some_and(|condition| condition.status == ConditionStatus::False && condition.reason == self.reason)
    }
}

/// The resource is serving.
pub const READY: &str = "Ready";

/// A provider is reachable and admitted for routing.
pub const AVAILABLE: &str = "Available";

/// Provider status reasons that reject the spec itself rather than a runtime dependency.
const PROVIDER_SPEC_REJECTIONS: [&str; 6] = [
    "UnsupportedAuthStrategy",
    "CredentialSecretRefInvalid",
    "EndpointInvalid",
    "MetricsEndpointInvalid",
    "ModelNameInvalid",
    "GridNetworkRefInvalid",
];

/// `Accepted` and `Available` for a provider in `phase` with status `reason`.
#[must_use]
pub fn provider_conditions(phase: &super::inference_provider::ProviderPhase, reason: Option<&str>) -> Vec<Observed> {
    use super::inference_provider::ProviderPhase;

    let rejected = reason.filter(|reason| PROVIDER_SPEC_REJECTIONS.contains(reason));
    let accepted = rejected.map_or_else(
        || Observed::new(ACCEPTED, ConditionStatus::True, "Valid"),
        |rejection| {
            Observed::new(ACCEPTED, ConditionStatus::False, rejection)
                .with_message(format!("spec rejected: {rejection}"))
        },
    );
    let (status, fallback) = match phase {
        ProviderPhase::Available => (ConditionStatus::True, "Available"),
        ProviderPhase::Pending => (ConditionStatus::Unknown, "Pending"),
        ProviderPhase::Degraded => (ConditionStatus::False, "Degraded"),
        ProviderPhase::Unavailable => (ConditionStatus::False, "Unavailable"),
    };
    let reason = if status == ConditionStatus::True {
        fallback
    } else {
        reason.unwrap_or(fallback)
    };
    vec![accepted, Observed::new(AVAILABLE, status, reason)]
}

/// A site record exists for a SWIM member.
pub const DISCOVERED: &str = "Discovered";

/// The site's gateway passed the trust check.
pub const CONNECTED: &str = "Connected";

/// A discovered SWIM member's `GridSite` name is taken by another network.
pub const FIELD_CONFLICT: &str = "FieldConflict";

/// The network discovered members it cannot adopt; never a verdict on its own spec.
pub const DISCOVERY_CONFLICT: &str = "DiscoveryConflict";

/// `GridSite` status reasons that reject the spec itself.
const SITE_SPEC_REJECTIONS: [&str; 3] = [
    "TrustConflictsWithPeerTrust",
    "ServerNameForbidden",
    "ServerNameInvalid",
];

/// `Accepted`, `Discovered`, `Connected`, and `Ready` for a site in `phase` with status `reason`.
#[must_use]
pub fn site_conditions(phase: &super::grid_site::GridSitePhase, reason: &str) -> Vec<Observed> {
    use super::grid_site::GridSitePhase;

    let accepted = if SITE_SPEC_REJECTIONS.contains(&reason) {
        Observed::new(ACCEPTED, ConditionStatus::False, reason)
    } else {
        Observed::new(ACCEPTED, ConditionStatus::True, "Valid")
    };
    let discovered = match phase {
        GridSitePhase::Pending | GridSitePhase::Left => Observed::new(DISCOVERED, ConditionStatus::False, reason),
        GridSitePhase::Discovered | GridSitePhase::Connecting | GridSitePhase::Active | GridSitePhase::Unreachable => {
            Observed::new(DISCOVERED, ConditionStatus::True, "MemberKnown")
        },
    };
    let connected = match phase {
        GridSitePhase::Active => Observed::new(CONNECTED, ConditionStatus::True, reason),
        GridSitePhase::Unreachable | GridSitePhase::Left => Observed::new(CONNECTED, ConditionStatus::False, reason),
        GridSitePhase::Pending | GridSitePhase::Discovered | GridSitePhase::Connecting => {
            Observed::new(CONNECTED, ConditionStatus::Unknown, reason)
        },
    };
    let ready = if *phase == GridSitePhase::Active {
        Observed::new(READY, ConditionStatus::True, "Active")
    } else {
        Observed::new(READY, ConditionStatus::False, reason)
    };
    vec![accepted, discovered, connected, ready]
}

/// `Accepted`, `Ready`, and `DiscoveryConflict` for a network in `phase`.
///
/// `Accepted` reflects only the network's own spec, `unready` holds `Ready` false whatever the phase,
/// and `conflicts` name discovered members it could not adopt.
#[must_use]
pub fn network_conditions(
    phase: &super::grid_network::GridNetworkPhase,
    rejection: Option<&Rejection>,
    unready: Option<&Rejection>,
    conflicts: &[String],
) -> Vec<Observed> {
    use super::grid_network::GridNetworkPhase;

    let ready = match phase {
        GridNetworkPhase::Active => Observed::new(READY, ConditionStatus::True, "Active"),
        GridNetworkPhase::Pending => Observed::new(READY, ConditionStatus::Unknown, "Pending"),
        GridNetworkPhase::Initializing => Observed::new(READY, ConditionStatus::Unknown, "Initializing"),
        GridNetworkPhase::Degraded => Observed::new(READY, ConditionStatus::False, "Degraded"),
    };
    let ready = unready.map_or(ready, |unready| {
        Observed::new(READY, ConditionStatus::False, unready.reason).with_message(unready.message.clone())
    });
    let accepted = rejection.map_or_else(
        || Observed::new(ACCEPTED, ConditionStatus::True, "Valid"),
        |rejection| {
            Observed::new(ACCEPTED, ConditionStatus::False, rejection.reason).with_message(rejection.message.clone())
        },
    );
    let conflict = if conflicts.is_empty() {
        Observed::new(DISCOVERY_CONFLICT, ConditionStatus::False, "NoConflict")
    } else {
        Observed::new(DISCOVERY_CONFLICT, ConditionStatus::True, FIELD_CONFLICT).with_message(conflicts.join("; "))
    };
    vec![accepted, ready, conflict]
}

/// A provider's metric signal names resolve to something scrapeable.
pub const METRICS_SIGNALS: &str = "MetricsSignals";

/// `MetricsSignals` for a provider whose `metricsConfig` resolves with `issue`.
#[must_use]
pub fn metrics_signals_condition(issue: Option<super::inference_provider::SignalNamesIssue>) -> Observed {
    use super::inference_provider::SignalNamesIssue;

    match issue {
        None => Observed::new(METRICS_SIGNALS, ConditionStatus::True, "Resolved"),
        Some(issue @ SignalNamesIssue::NoSignalNames) => {
            Observed::new(METRICS_SIGNALS, ConditionStatus::False, issue.reason()).with_message(
                "set metricsConfig.preset or metricsConfig.signalNames; signals score neutrally".to_owned(),
            )
        },
        Some(issue @ SignalNamesIssue::MissingQueueCapacity) => {
            Observed::new(METRICS_SIGNALS, ConditionStatus::False, issue.reason())
                .with_message("the preset queue metric is a raw count; set metricsConfig.queueCapacity".to_owned())
        },
    }
}

/// The condition of `type_`, if present.
#[must_use]
pub fn find<'list>(conditions: &'list [Condition], type_: &str) -> Option<&'list Condition> {
    conditions.iter().find(|condition| condition.type_ == type_)
}

/// Current time as RFC 3339, the format conditions record, empty on a format failure.
#[must_use]
pub fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    fn accepted(holds: bool, reason: &str) -> Observed {
        Observed::new("Accepted", holds.into(), reason)
    }

    #[test]
    fn an_unchanged_status_keeps_its_transition_time() {
        let first = reconcile_conditions(&[], vec![accepted(true, "Valid")], 1, "t1");
        let second = reconcile_conditions(&first, vec![accepted(true, "Valid")], 2, "t2");
        assert_eq!(second[0].last_transition_time, "t1", "no transition, no new time");
        assert_eq!(second[0].observed_generation, 2, "the generation still advances");
    }

    #[test]
    fn a_status_change_records_a_new_transition_time() {
        let first = reconcile_conditions(&[], vec![accepted(true, "Valid")], 1, "t1");
        let second = reconcile_conditions(&first, vec![accepted(false, "InvalidSpec")], 2, "t2");
        assert_eq!(second[0].last_transition_time, "t2");
        assert_eq!(second[0].status, ConditionStatus::False);
        assert_eq!(second[0].reason, "InvalidSpec");
    }

    fn status_of(observed: &[Observed], type_: &str) -> ConditionStatus {
        observed
            .iter()
            .find(|o| o.type_ == type_)
            .map_or_else(|| std::process::abort(), |o| o.status)
    }

    #[test]
    fn an_active_site_is_discovered_connected_and_ready() {
        let observed = site_conditions(&crate::crd::grid_site::GridSitePhase::Active, "Verified");
        for type_ in [ACCEPTED, DISCOVERED, CONNECTED, READY] {
            assert_eq!(status_of(&observed, type_), ConditionStatus::True, "{type_}");
        }
    }

    #[test]
    fn a_pending_site_is_not_discovered_and_connection_is_unknown() {
        let observed = site_conditions(&crate::crd::grid_site::GridSitePhase::Pending, "AwaitingDiscovery");
        assert_eq!(status_of(&observed, DISCOVERED), ConditionStatus::False);
        assert_eq!(status_of(&observed, CONNECTED), ConditionStatus::Unknown);
        assert_eq!(status_of(&observed, READY), ConditionStatus::False);
    }

    #[test]
    fn an_unreachable_site_carries_the_probe_reason_on_connected() {
        let observed = site_conditions(&crate::crd::grid_site::GridSitePhase::Unreachable, "PinMismatch");
        let connected = observed
            .iter()
            .find(|o| o.type_ == CONNECTED)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(connected.status, ConditionStatus::False);
        assert_eq!(connected.reason, "PinMismatch");
        assert_eq!(status_of(&observed, DISCOVERED), ConditionStatus::True);
    }

    #[test]
    fn invalid_secret_trust_material_is_not_a_spec_rejection() {
        let observed = site_conditions(
            &crate::crd::grid_site::GridSitePhase::Connecting,
            "TrustMaterialInvalid",
        );
        let missing = site_conditions(
            &crate::crd::grid_site::GridSitePhase::Connecting,
            "TrustMaterialMissing",
            "",
        );
        assert_eq!(
            status_of(&observed, ACCEPTED),
            ConditionStatus::True,
            "bad Secret content is not the spec's fault"
        );
        for type_ in [CONNECTED, READY] {
            assert_eq!(
                status_of(&observed, type_),
                status_of(&missing, type_),
                "{type_} reports it like TrustMaterialMissing"
            );
        }
        assert_eq!(status_of(&observed, READY), ConditionStatus::False);
    }

    #[test]
    fn network_ready_follows_the_phase() {
        use crate::crd::grid_network::GridNetworkPhase;
        assert_eq!(
            status_of(&network_conditions(&GridNetworkPhase::Active, None, None, &[]), READY),
            ConditionStatus::True
        );
        assert_eq!(
            status_of(&network_conditions(&GridNetworkPhase::Degraded, None, None, &[]), READY),
            ConditionStatus::False
        );
        assert_eq!(
            status_of(
                &network_conditions(&GridNetworkPhase::Initializing, None, None, &[]),
                READY
            ),
            ConditionStatus::Unknown
        );
    }

    #[test]
    fn a_discovery_name_collision_is_a_discovery_conflict_not_a_rejection() {
        use crate::crd::grid_network::GridNetworkPhase;
        let conflicts = vec!["SWIM member east not adopted: GridSite east belongs to network other".to_owned()];
        let observed = network_conditions(&GridNetworkPhase::Active, None, None, &conflicts);
        let conflict = observed
            .iter()
            .find(|o| o.type_ == DISCOVERY_CONFLICT)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(conflict.status, ConditionStatus::True);
        assert_eq!(conflict.reason, FIELD_CONFLICT);
        assert_eq!(conflict.message, conflicts[0]);
        assert_eq!(
            status_of(&observed, ACCEPTED),
            ConditionStatus::True,
            "the network's own spec is valid"
        );
        assert_eq!(
            status_of(&observed, READY),
            ConditionStatus::True,
            "a conflict does not degrade serving"
        );
    }

    #[test]
    fn missing_trust_material_holds_network_ready_false_whatever_the_phase() {
        use crate::crd::grid_network::GridNetworkPhase;
        let missing = Rejection::new("TrustMaterialMissing", "Secret grid/site not found");
        let observed = network_conditions(&GridNetworkPhase::Active, None, Some(&missing), &[]);
        let ready = observed
            .iter()
            .find(|o| o.type_ == READY)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (ready.status, ready.reason.as_str()),
            (ConditionStatus::False, "TrustMaterialMissing")
        );
        assert_eq!(
            status_of(&observed, ACCEPTED),
            ConditionStatus::True,
            "the spec itself is valid"
        );
    }

    #[test]
    fn a_network_without_conflicts_reports_discovery_conflict_false() {
        use crate::crd::grid_network::GridNetworkPhase;
        let observed = network_conditions(&GridNetworkPhase::Active, None, None, &[]);
        assert_eq!(status_of(&observed, DISCOVERY_CONFLICT), ConditionStatus::False);
    }

    #[test]
    fn a_network_rejection_and_conflicts_report_separately() {
        use crate::crd::grid_network::GridNetworkPhase;
        let rejection = Rejection::new("BudgetPolicyInvalid", "duplicate tenantId a");
        let observed = network_conditions(&GridNetworkPhase::Pending, Some(&rejection), None, &["c".to_owned()]);
        let accepted = observed
            .iter()
            .find(|o| o.type_ == ACCEPTED)
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(accepted.reason, "BudgetPolicyInvalid");
        assert_eq!(accepted.message, "duplicate tenantId a");
        assert_eq!(status_of(&observed, DISCOVERY_CONFLICT), ConditionStatus::True);
    }

    #[test]
    fn a_rejection_is_reported_once_per_reason() {
        let rejection = Rejection::new("EndpointInvalid", "bad");
        let reported = reconcile_conditions(
            &[],
            vec![Observed::new(ACCEPTED, ConditionStatus::False, "EndpointInvalid")],
            1,
            "t",
        );
        assert!(rejection.already_reported(&reported));
        assert!(!Rejection::new("ModelNameInvalid", "bad").already_reported(&reported));
        assert!(!rejection.already_reported(&[]));
    }

    #[test]
    fn a_spec_rejection_marks_a_provider_not_accepted() {
        use crate::crd::inference_provider::ProviderPhase;
        let observed = provider_conditions(&ProviderPhase::Unavailable, Some("UnsupportedAuthStrategy"));
        assert_eq!(status_of(&observed, ACCEPTED), ConditionStatus::False);
        assert_eq!(status_of(&observed, AVAILABLE), ConditionStatus::False);
        let pending = provider_conditions(&ProviderPhase::Pending, None);
        assert_eq!(status_of(&pending, ACCEPTED), ConditionStatus::True);
        assert_eq!(status_of(&pending, AVAILABLE), ConditionStatus::Unknown);
    }

    #[test]
    fn condition_serializes_in_the_metav1_shape() {
        let condition = reconcile_conditions(&[], vec![accepted(true, "Valid")], 3, "2026-10-01T00:00:00Z")
            .pop()
            .unwrap_or_else(|| std::process::abort());
        let json = serde_json::to_value(&condition).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            json,
            serde_json::json!({
                "type": "Accepted",
                "status": "True",
                "observedGeneration": 3,
                "lastTransitionTime": "2026-10-01T00:00:00Z",
                "reason": "Valid",
                "message": ""
            })
        );
    }
}
