//! The `PeerSettingsMatch` condition: whether every polled peer states the grid-wide settings
//! this site does in its `x-grid-settings` header, an RFC 8941 dictionary to which a later
//! setting adds a key, never a new header or condition.

use std::{
    collections::BTreeMap,
    sync::{Arc, PoisonError, RwLock},
};

use serde_json::Value;

use crate::crd::{grid_network::GridNetwork, inference_provider::Condition};

/// The header carrying a site's settings on every signals and providers answer.
pub const SETTINGS_HEADER: &str = "x-grid-settings";

/// The condition's type, and its True reason.
pub const PEER_SETTINGS_MATCH: &str = "PeerSettingsMatch";

/// Reason while some polled peer states a setting otherwise.
pub const SETTINGS_MISMATCH: &str = "SettingsMismatch";

/// Server-side apply manager, apart from the other `GridNetwork` writers.
pub const FIELD_MANAGER: &str = "grid-operator-peer-settings";

/// Peers a message names before it counts the rest.
const MAX_NAMED_PEERS: usize = 10;

/// The grid-wide settings a site states.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Settings {
    /// How the site learns its members, `Swim` or `Local`.
    pub membership: Option<String>,
    /// The grid it belongs to, its `GridNetwork`'s `spec.gridId`.
    pub grid_id: Option<String>,
}

/// One compared setting: its header key, what an admin sets for it, and its value.
type Field = (&'static str, &'static str, fn(&Settings) -> Option<&str>);

/// Every compared setting, in header order.
const FIELDS: [Field; 2] = [
    ("membership", "grid.membership.source", |settings| {
        settings.membership.as_deref()
    }),
    ("grid-id", "spec.gridId", |settings| settings.grid_id.as_deref()),
];

impl Settings {
    /// As an RFC 8941 dictionary: the membership a token, the grid ID a string.
    #[must_use]
    pub fn header(&self) -> String {
        let membership = self.membership.as_deref().map(|mode| format!("membership={mode}"));
        let grid_id = self.grid_id.as_deref().map(|id| format!("grid-id=\"{id}\""));
        [membership, grid_id]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// The settings an RFC 8941 dictionary states, keeping only the keys and values this
    /// release knows. Anything else is not stated.
    #[must_use]
    pub fn parse(header: &str) -> Self {
        let mut settings = Self::default();
        for member in header.split(',') {
            let Some((key, value)) = member.split_once('=') else {
                continue;
            };
            let value = value.split(';').next().unwrap_or_default().trim();
            match key.trim() {
                "membership" => {
                    settings.membership = crate::cli::Membership::parse(value).map(|mode| mode.as_str().to_owned());
                },
                "grid-id" => settings.grid_id = grid_id(value),
                _ => {},
            }
        }
        settings
    }

    /// Each setting `self` and `peer` both state and state apart, with the peer's value.
    fn differences(&self, peer: &Self) -> Vec<(&'static str, String)> {
        FIELDS
            .iter()
            .filter_map(|(key, _, value)| match (value(self), value(peer)) {
                (Some(ours), Some(theirs)) if ours != theirs => Some((*key, theirs.to_owned())),
                _ => None,
            })
            .collect()
    }
}

/// A quoted grid ID, `None` unless it is a DNS label as `spec.gridId` requires.
fn grid_id(value: &str) -> Option<String> {
    let id = value.strip_prefix('"')?.strip_suffix('"')?;
    let label = (1..=63).contains(&id.len())
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !id.starts_with('-')
        && !id.ends_with('-');
    label.then(|| id.to_owned())
}

/// This site's own settings and the header stating them, shared by the listener that sends
/// them and the poller that compares them.
#[derive(Clone, Debug, Default)]
pub struct OwnSettings(Arc<RwLock<(Settings, Option<http::HeaderValue>)>>);

impl OwnSettings {
    /// Settings that state `membership` alone, until the grid ID is known.
    #[must_use]
    pub fn new(membership: crate::cli::Membership) -> Self {
        let own = Self::default();
        own.update(|settings| settings.membership = Some(membership.as_str().to_owned()));
        own
    }

    /// Record the grid ID this site belongs to, `None` while it has none.
    pub fn set_grid_id(&self, grid_id: Option<&str>) {
        let grid_id = grid_id.filter(|id| !id.is_empty()).map(str::to_owned);
        if self.current().grid_id != grid_id {
            self.update(|settings| settings.grid_id = grid_id);
        }
    }

    /// The settings as last recorded.
    #[must_use]
    pub fn current(&self) -> Settings {
        self.0.read().unwrap_or_else(PoisonError::into_inner).0.clone()
    }

    /// The header stating them, `None` while there is nothing to state.
    #[must_use]
    pub fn header(&self) -> Option<http::HeaderValue> {
        self.0.read().unwrap_or_else(PoisonError::into_inner).1.clone()
    }

    /// Change the settings and the header with them.
    fn update<Change: FnOnce(&mut Settings)>(&self, change: Change) {
        let mut held = self.0.write().unwrap_or_else(PoisonError::into_inner);
        change(&mut held.0);
        let header = held.0.header();
        held.1 = (!header.is_empty())
            .then(|| http::HeaderValue::from_str(&header).ok())
            .flatten();
    }
}

/// Peers stating a setting otherwise, carried from round to round.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PeerSettings {
    /// Each mismatched peer and the settings it states apart, in name order for a stable message.
    mismatched: BTreeMap<String, Vec<(&'static str, String)>>,
}

impl PeerSettings {
    /// Fold in one round: `polled` the peers dialed, `answered` the settings each answering
    /// peer stated, and `ours` this site's.
    ///
    /// A peer that did not answer, or answered without the header, keeps its standing, and a
    /// peer no longer polled is forgotten.
    pub fn observe(&mut self, polled: &[String], answered: &BTreeMap<String, Option<Settings>>, ours: &Settings) {
        self.mismatched.retain(|peer, _| polled.contains(peer));
        for (peer, stated) in answered {
            let Some(stated) = stated else {
                continue;
            };
            let differences = ours.differences(stated);
            if differences.is_empty() {
                self.mismatched.remove(peer);
            } else {
                self.mismatched.insert(peer.clone(), differences);
            }
        }
    }

    /// Peers stating a setting otherwise, in name order.
    #[must_use]
    pub const fn mismatched(&self) -> &BTreeMap<String, Vec<(&'static str, String)>> {
        &self.mismatched
    }

    /// The condition to write over `current`, `None` when it already says this. It carries no
    /// `observedGeneration`: the poller reads no `GridNetwork` spec.
    #[must_use]
    pub fn condition(&self, current: &[Condition], ours: &Settings, now_rfc3339: &str) -> Option<Condition> {
        let (status, reason) = if self.mismatched.is_empty() {
            ("True", PEER_SETTINGS_MATCH)
        } else {
            ("False", SETTINGS_MISMATCH)
        };
        let stamp = crate::rollup::Stamp {
            generation: None,
            now: now_rfc3339,
        };
        let condition = crate::rollup::condition(
            current,
            PEER_SETTINGS_MATCH,
            (status, reason, self.message(ours)),
            stamp,
        );
        crate::rollup::differs(current, std::slice::from_ref(&condition)).then_some(condition)
    }

    /// Up to [`MAX_NAMED_PEERS`] mismatched peers with the settings each states, then a count.
    fn named(&self) -> String {
        let named: Vec<String> = self
            .mismatched
            .iter()
            .take(MAX_NAMED_PEERS)
            .map(|(peer, differences)| {
                let stated: Vec<String> = differences
                    .iter()
                    .map(|(key, value)| format!("{key} {value}"))
                    .collect();
                format!("{peer} ({})", stated.join(", "))
            })
            .collect();
        let more = self.mismatched.len().saturating_sub(named.len());
        if more == 0 {
            named.join(", ")
        } else {
            format!("{} and {more} more", named.join(", "))
        }
    }

    /// What the condition says: each mismatched peer by name with the settings it states, and
    /// what to set alike.
    fn message(&self, ours: &Settings) -> String {
        if self.mismatched.is_empty() {
            return format!("every polled peer states this site's settings: {}", ours.header());
        }
        let peers = self.named();
        let set: Vec<&str> = FIELDS
            .iter()
            .filter(|(key, ..)| self.mismatched.values().flatten().any(|(differs, _)| differs == key))
            .map(|(_, admin, _)| *admin)
            .collect();
        format!(
            "this site states {}, but {peers} differ; set {} alike on every site",
            ours.header(),
            set.join(" and ")
        )
    }
}

/// The status patch carrying `network`'s `PeerSettingsMatch` condition, with the name to
/// apply it to, or `None` when the live status already says this.
#[must_use]
pub fn patch<'network>(
    network: &'network GridNetwork,
    peers: &PeerSettings,
    ours: &Settings,
    now_rfc3339: &str,
) -> Option<(&'network str, Value)> {
    crate::peer_reach::network_patch(network, |current| peers.condition(current, ours, now_rfc3339))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{OwnSettings, PEER_SETTINGS_MATCH, PeerSettings, SETTINGS_MISMATCH, Settings};
    use crate::cli::Membership;

    fn settings(membership: &str, grid_id: Option<&str>) -> Settings {
        Settings {
            membership: Some(membership.to_owned()),
            grid_id: grid_id.map(str::to_owned),
        }
    }

    fn after(held: &PeerSettings, polled: &[&str], answered: &[(&str, Option<Settings>)]) -> PeerSettings {
        let mut next = held.clone();
        let polled: Vec<String> = polled.iter().map(|peer| (*peer).to_owned()).collect();
        let answered = answered
            .iter()
            .map(|(peer, stated)| ((*peer).to_owned(), stated.clone()))
            .collect::<BTreeMap<_, _>>();
        next.observe(&polled, &answered, &settings("Local", Some("dagobah")));
        next
    }

    #[test]
    fn the_header_is_an_rfc_8941_dictionary_and_reads_back() {
        let ours = settings("Local", Some("dagobah"));
        assert_eq!(ours.header(), r#"membership=Local, grid-id="dagobah""#);
        assert_eq!(Settings::parse(&ours.header()), ours);
        assert_eq!(
            Settings::parse(r#"grid-id="dagobah";p=1, digest=:AAA=:, membership=Swim"#),
            settings("Swim", Some("dagobah")),
            "parameters and keys this release does not know are not read"
        );
        for refused in ["membership=Gossip", "grid-id=dagobah", r#"grid-id="Not A Label""#] {
            assert_eq!(Settings::parse(refused), Settings::default(), "{refused}");
        }
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "three peers stating three ways, then all matching")]
    fn a_peer_stating_a_setting_otherwise_is_a_mismatch_until_it_matches() {
        let held = after(
            &PeerSettings::default(),
            &["east", "west", "north"],
            &[
                ("east", Some(settings("Swim", Some("dagobah")))),
                ("west", Some(settings("Local", Some("hoth")))),
                ("north", Some(settings("Local", None))),
            ],
        );
        let condition = held
            .condition(&[], &settings("Local", Some("dagobah")), "t")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (condition.status.as_str(), condition.reason.as_str()),
            ("False", SETTINGS_MISMATCH)
        );
        assert!(
            condition
                .message
                .contains("east (membership Swim), west (grid-id hoth)"),
            "{}",
            condition.message
        );
        assert!(
            condition
                .message
                .contains("set grid.membership.source and spec.gridId alike"),
            "{}",
            condition.message
        );
        assert_eq!(condition.observed_generation, None, "the poller reads no spec");
        assert!(
            !held.mismatched().contains_key("north"),
            "a setting one side omits is not compared"
        );
        let cleared = after(
            &held,
            &["east", "west"],
            &[
                ("east", Some(settings("Local", Some("dagobah")))),
                ("west", Some(settings("Local", Some("dagobah")))),
            ],
        );
        let matched = cleared
            .condition(&[], &settings("Local", Some("dagobah")), "t")
            .unwrap_or_else(|| std::process::abort());
        assert_eq!(
            (matched.status.as_str(), matched.reason.as_str()),
            ("True", PEER_SETTINGS_MATCH)
        );
    }

    #[test]
    fn silence_and_a_missing_header_keep_a_peers_standing() {
        let swim = Some(settings("Swim", None));
        let held = after(&PeerSettings::default(), &["east"], &[("east", swim)]);
        assert_eq!(after(&held, &["east"], &[]), held, "no answer");
        assert_eq!(after(&held, &["east"], &[("east", None)]), held, "no header");
        assert!(after(&held, &[], &[]).mismatched().is_empty(), "no longer polled");
    }

    #[test]
    fn own_settings_state_the_grid_id_once_it_is_known() {
        let own = OwnSettings::new(Membership::Local);
        assert_eq!(
            own.header().as_ref().map(http::HeaderValue::as_bytes),
            Some(&b"membership=Local"[..])
        );
        own.set_grid_id(Some("dagobah"));
        assert_eq!(
            own.header().as_ref().map(http::HeaderValue::as_bytes),
            Some(&br#"membership=Local, grid-id="dagobah""#[..])
        );
        own.set_grid_id(Some(""));
        assert_eq!(own.current().grid_id, None, "an empty grid ID states none");
    }
}
