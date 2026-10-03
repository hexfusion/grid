//! Which renewals a site's enrollment record admits, the same for every backend.
//! The caller has already proven possession of an unexpired leaf for the name.

use time::OffsetDateTime;
use uuid::Uuid;

/// An authenticated renewal request.
#[derive(Debug, Clone)]
pub struct Renewal {
    /// The site the presented leaf names.
    pub site_name: String,
    /// Key digest of the leaf presented over mTLS.
    pub presented_key: String,
    /// Key digest of the CSR, the key the new leaf certifies.
    pub requested_key: String,
    /// The presented leaf's `notBefore`.
    pub presented_not_before: OffsetDateTime,
    /// Whether the name is reserved for bootstrap issuance.
    pub reserved: bool,
}

/// A name's enrollment record, as renewal reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    /// The enrollment record's identifier.
    pub id: Uuid,
    /// The key the latest issued leaf certifies.
    pub current_key: String,
    /// The key it replaced, kept so a renewal whose response was lost can retry.
    pub previous_key: Option<String>,
    /// When the record last changed.
    pub recorded_at: OffsetDateTime,
}

/// An admitted, signed renewal.
#[derive(Debug, Clone)]
pub struct Renewed {
    /// The enrollment record's identifier.
    pub id: Uuid,
    /// What the renewal did to the record.
    pub action: RenewAction,
    /// The key the record held as current before, `None` on first registration.
    pub replaced_key: Option<String>,
    /// The signed certificate.
    pub issued: super::Issued,
}

impl Held {
    /// A record written now.
    #[must_use]
    pub fn new(id: Uuid, current_key: String, previous_key: Option<String>) -> Self {
        Self {
            id,
            current_key,
            previous_key,
            recorded_at: OffsetDateTime::now_utc(),
        }
    }
}

/// What an admitted renewal does to the record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenewAction {
    /// The current key renews: the requested key becomes current.
    Rotate,
    /// A retry after a lost response: re-sign the current key, record unchanged.
    Resign,
    /// First renewal of a reserved name, which bootstrap issued with no record.
    Register,
    /// A reserved name bootstrap re-issued after the record was written.
    Supersede,
}

/// Why a renewal was refused. Logged, never told to the caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Refusal {
    /// No record holds the name.
    #[error("no enrollment holds this site name")]
    UnknownSite,
    /// The presented key is not the one the record holds.
    #[error("the presented key is not this site's current key")]
    StaleKey,
    /// The CSR reuses a key the record already holds.
    #[error("the certificate request reuses a held key")]
    KeyReused,
}

impl Refusal {
    /// Metric and log label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownSite => "unknown_site",
            Self::StaleKey => "stale_key",
            Self::KeyReused => "key_reused",
        }
    }
}

/// Decide a renewal against the name's record.
///
/// # Errors
///
/// Returns the [`Refusal`] when the record does not admit the presented key.
pub fn decide(held: Option<&Held>, renewal: &Renewal) -> Result<RenewAction, Refusal> {
    if renewal.requested_key == renewal.presented_key {
        return Err(Refusal::KeyReused);
    }
    let Some(held) = held else {
        return if renewal.reserved {
            Ok(RenewAction::Register)
        } else {
            Err(Refusal::UnknownSite)
        };
    };
    if renewal.presented_key == held.current_key {
        return if held.previous_key.as_ref() == Some(&renewal.requested_key) {
            Err(Refusal::KeyReused)
        } else {
            Ok(RenewAction::Rotate)
        };
    }
    if held.previous_key.as_ref() == Some(&renewal.presented_key) && renewal.requested_key == held.current_key {
        return Ok(RenewAction::Resign);
    }
    // Only the CA mints a leaf newer than the record, and for a reserved name only bootstrap does.
    let issued_at = renewal.presented_not_before.saturating_add(certs::CLOCK_SKEW_ALLOWANCE);
    if renewal.reserved && issued_at > held.recorded_at && renewal.requested_key != held.current_key {
        return Ok(RenewAction::Supersede);
    }
    Err(Refusal::StaleKey)
}

#[cfg(test)]
mod tests {
    use time::Duration;

    use super::*;

    fn renewal(presented: &str, requested: &str, reserved: bool, not_before: OffsetDateTime) -> Renewal {
        Renewal {
            site_name: "site-a".to_owned(),
            presented_key: presented.to_owned(),
            requested_key: requested.to_owned(),
            presented_not_before: not_before,
            reserved,
        }
    }

    fn held(current: &str, previous: Option<&str>, recorded_at: OffsetDateTime) -> Held {
        Held {
            id: Uuid::nil(),
            current_key: current.to_owned(),
            previous_key: previous.map(str::to_owned),
            recorded_at,
        }
    }

    /// One decision: a name, the record, the renewal, and the outcome.
    struct Case {
        name: &'static str,
        held: Option<Held>,
        renewal: Renewal,
        expected: Result<RenewAction, Refusal>,
    }

    fn case(name: &'static str, held: Option<Held>, renewal: Renewal, expected: Result<RenewAction, Refusal>) -> Case {
        Case {
            name,
            held,
            renewal,
            expected,
        }
    }

    #[test]
    #[expect(clippy::too_many_lines, reason = "one row per decision")]
    fn decisions() {
        let now = OffsetDateTime::now_utc();
        let earlier = now.saturating_sub(Duration::days(20));
        let record = held("k1", Some("k0"), now);
        let cases = [
            case(
                "the current key renews",
                Some(record.clone()),
                renewal("k1", "k2", false, earlier),
                Ok(RenewAction::Rotate),
            ),
            case(
                "a lost response retries",
                Some(record.clone()),
                renewal("k0", "k1", false, earlier),
                Ok(RenewAction::Resign),
            ),
            case(
                "the previous key cannot pick a new key",
                Some(record.clone()),
                renewal("k0", "k9", false, earlier),
                Err(Refusal::StaleKey),
            ),
            case(
                "an unknown key is refused",
                Some(record.clone()),
                renewal("kx", "k9", false, earlier),
                Err(Refusal::StaleKey),
            ),
            case(
                "a CSR for the presented key is refused",
                Some(record.clone()),
                renewal("k1", "k1", false, earlier),
                Err(Refusal::KeyReused),
            ),
            case(
                "a CSR for the previous key is refused",
                Some(record.clone()),
                renewal("k1", "k0", false, earlier),
                Err(Refusal::KeyReused),
            ),
            case(
                "a spoke with no record is refused",
                None,
                renewal("k1", "k2", false, earlier),
                Err(Refusal::UnknownSite),
            ),
            case(
                "a reserved name registers on first renewal",
                None,
                renewal("k1", "k2", true, earlier),
                Ok(RenewAction::Register),
            ),
            case(
                "a newer bootstrap leaf supersedes a reserved record",
                Some(record.clone()),
                renewal("kb", "k2", true, now),
                Ok(RenewAction::Supersede),
            ),
            case(
                "an older reserved leaf is refused",
                Some(record.clone()),
                renewal("kb", "k2", true, earlier),
                Err(Refusal::StaleKey),
            ),
            case(
                "a newer leaf supersedes only a reserved name",
                Some(record),
                renewal("kb", "k2", false, now),
                Err(Refusal::StaleKey),
            ),
        ];
        for Case {
            name,
            held,
            renewal,
            expected,
        } in cases
        {
            assert_eq!(decide(held.as_ref(), &renewal), expected, "{name}");
        }
    }
}
