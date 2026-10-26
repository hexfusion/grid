//! The `SignalsListener` condition: whether the signals endpoint this install declares is served.
//!
//! The listener runs only under the poll transport, so a site that declares it under gossip
//! renders a port and a Service with nothing behind them.

use serde_json::Value;

use crate::crd::{
    grid_network::{GridNetwork, SignalMode},
    inference_provider::Condition,
};

/// The condition's type.
pub const SIGNALS_LISTENER: &str = "SignalsListener";

/// Reason while the declared listener serves.
pub const SIGNALS_LISTENER_SERVING: &str = "SignalsListenerServing";

/// Reason while the install declares a listener the transport never starts.
pub const SIGNALS_LISTENER_ABSENT: &str = "SignalsListenerAbsent";

/// Server-side apply manager, apart from `GridNetwork` reconciliation.
pub const FIELD_MANAGER: &str = "grid-operator-signals-listener";

/// Whether the declared signals listener runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Listener {
    /// It runs.
    Serving,
    /// It is declared, but this transport runs none.
    Absent,
}

impl Listener {
    /// Judge a listener the install `declared`, under the `transport` this process runs.
    /// `None` when none is declared, since nothing is promised.
    #[must_use]
    pub const fn of(declared: bool, transport: SignalMode) -> Option<Self> {
        if !declared {
            return None;
        }
        Some(match transport {
            SignalMode::Poll => Self::Serving,
            SignalMode::Gossip => Self::Absent,
        })
    }

    /// The status, reason and message the condition carries.
    const fn verdict(self) -> (&'static str, &'static str, &'static str) {
        match self {
            Self::Serving => ("True", SIGNALS_LISTENER_SERVING, "the signals listener runs under poll"),
            Self::Absent => (
                "False",
                SIGNALS_LISTENER_ABSENT,
                "the signals Service is rendered but the signal transport is gossip, so nothing listens on \
                 it: set the GridNetwork's signalTransport to poll, or turn signals off",
            ),
        }
    }
}

/// The condition for `listener`, or `None` when the live status already says it.
#[must_use]
pub fn condition(network: &GridNetwork, listener: Listener, now_rfc3339: &str) -> Option<Condition> {
    let (state, reason, message) = listener.verdict();
    let generation = network.metadata.generation;
    let held = network
        .status
        .as_ref()
        .and_then(|live| live.conditions.iter().find(|held| held.type_ == SIGNALS_LISTENER));
    if held.is_some_and(|held| {
        held.status == state
            && held.reason == reason
            && held.message == message
            && held.observed_generation == generation
    }) {
        return None;
    }
    let last_transition_time = held
        .filter(|held| held.status == state)
        .map_or_else(|| now_rfc3339.to_owned(), |held| held.last_transition_time.clone());
    Some(Condition {
        type_: SIGNALS_LISTENER.to_owned(),
        status: state.to_owned(),
        reason: reason.to_owned(),
        message: message.to_owned(),
        last_transition_time,
        observed_generation: generation,
    })
}

/// The status patch carrying the condition, or `None` when the live status already says it.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    listener: Listener,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    let name = network.metadata.name.as_deref()?;
    let condition = condition(network, listener, now_rfc3339)?;
    Some((
        name,
        serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": name },
            "status": { "conditions": [condition] }
        }),
    ))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::crd::grid_network::GridNetworkStatus;

    fn network() -> GridNetwork {
        serde_json::from_value(serde_json::json!({
            "apiVersion": "grid.praxis.fast/v1alpha1",
            "kind": "GridNetwork",
            "metadata": { "name": "grid", "generation": 3 },
            "spec": {}
        }))
        .expect("network")
    }

    #[test]
    fn nothing_is_said_when_no_listener_is_declared() {
        assert_eq!(Listener::of(false, SignalMode::Gossip), None);
        assert_eq!(Listener::of(false, SignalMode::Poll), None);
    }

    #[test]
    fn a_declared_listener_is_absent_under_gossip_and_serves_under_poll() {
        assert_eq!(Listener::of(true, SignalMode::Gossip), Some(Listener::Absent));
        assert_eq!(Listener::of(true, SignalMode::Poll), Some(Listener::Serving));
        let absent = condition(&network(), Listener::Absent, "t").expect("written");
        assert_eq!(
            (absent.status.as_str(), absent.reason.as_str()),
            ("False", SIGNALS_LISTENER_ABSENT)
        );
        assert!(absent.message.contains("gossip"), "{}", absent.message);
        let serving = condition(&network(), Listener::Serving, "t").expect("written");
        assert_eq!(
            (serving.status.as_str(), serving.reason.as_str()),
            ("True", SIGNALS_LISTENER_SERVING)
        );
    }

    #[test]
    fn an_unchanged_condition_is_not_rewritten_and_a_flip_moves_the_transition() {
        let mut live = network();
        let written = condition(&live, Listener::Absent, "t0").expect("written");
        live.status = Some(GridNetworkStatus {
            conditions: vec![written],
            ..GridNetworkStatus::default()
        });
        assert!(condition(&live, Listener::Absent, "t1").is_none(), "already says it");
        let flipped = condition(&live, Listener::Serving, "t1").expect("flipped");
        assert_eq!(flipped.last_transition_time, "t1");
    }
}
