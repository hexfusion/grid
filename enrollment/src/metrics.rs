//! Prometheus metrics for the enrollment service.
//!
//! One counter today: submissions the service refused, by reason. Auto-issue
//! removed the human who would have noticed a refused request, so the gate has
//! to be observable, and an integration test asserts a positive increment
//! rather than the absence of a certificate, which is racy.

use std::sync::LazyLock;

use prometheus::{Encoder as _, IntCounterVec, Opts, Registry, TextEncoder};

/// Submissions refused at `/v1/requests`, labelled by reason.
static ENROLLMENT_REJECTED: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "grid_enrollment_rejected_total",
            "Enrollment submissions refused, by reason",
        ),
        &["reason"],
    )
    .unwrap_or_else(|_invalid| std::process::abort())
});

/// The registry the exposition renders from.
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let registry = Registry::new();
    registry
        .register(Box::new(ENROLLMENT_REJECTED.clone()))
        .unwrap_or_else(|_duplicate| std::process::abort());
    registry
});

/// Why a submission was refused, as a stable metric label.
///
/// Closed set so the label cardinality is bounded and the taxonomy is the same
/// at every call site.
#[derive(Debug, Clone, Copy)]
pub enum RejectReason {
    /// No site token in the request.
    NoToken,
    /// A token, but no invite matches it.
    InvalidInvite,
    /// The invite was already redeemed.
    AlreadyRedeemed,
    /// The invite is past its expiry.
    Expired,
    /// The certificate signing request could not be used.
    InvalidCsr,
    /// A valid invite, but its pinned name is already held by an issued member.
    NameTaken,
}

impl RejectReason {
    /// The label value for this reason.
    const fn label(self) -> &'static str {
        match self {
            Self::NoToken => "no_token",
            Self::InvalidInvite => "invalid_invite",
            Self::AlreadyRedeemed => "already_redeemed",
            Self::Expired => "expired",
            Self::InvalidCsr => "invalid_csr",
            Self::NameTaken => "name_taken",
        }
    }
}

/// Count one refused submission.
pub fn record_rejection(reason: RejectReason) {
    ENROLLMENT_REJECTED.with_label_values(&[reason.label()]).inc();
}

/// Encode the registry as Prometheus text.
#[must_use]
pub fn encode() -> Vec<u8> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    // Ignore an encode error rather than fail: a metrics scrape must never take
    // the service down, and the buffer holds whatever encoded.
    let _ignored = encoder.encode(&REGISTRY.gather(), &mut buffer);
    buffer
}
