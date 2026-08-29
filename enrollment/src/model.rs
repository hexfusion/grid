//! Types on the wire, matching `api/enrollment-v1.yaml`.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

/// Where a request has got to. Nothing is trusted before [`Self::Issued`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnrollmentPhase {
    /// Submitted and awaiting a decision.
    Pending,
    /// Approved, and a certificate was issued.
    Issued,
    /// Refused by an operator.
    Denied,
    /// Approved, but issuing the certificate failed.
    Failed,
}

/// Where peers reach a provider once it is a member.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Egress {
    /// Host and port of the provider's gateway.
    pub address: String,

    /// Name peers present in SNI, when it differs from the address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_name: Option<String>,
}

/// One model a provider serves, and how to call it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapability {
    /// Model identifier as the provider knows it.
    pub name: String,

    /// Absolute request path for this model.
    pub path: String,

    /// Request and response shape the provider speaks.
    pub api_format: String,
}

/// Inference a provider offers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InferenceCapability {
    /// The models served.
    #[serde(default)]
    pub models: Vec<ModelCapability>,
}

/// What a provider claims to serve.
///
/// An advertisement grants nothing. A site decides for itself whether to
/// believe it and whether to route there, so a false claim costs the believer
/// one failed request.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    /// Inference the provider offers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inference: Option<InferenceCapability>,
}

/// What a provider submits to ask for membership.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentRequestInput {
    /// The name being asked for.
    pub site_name: String,

    /// The grid being joined.
    pub grid_network_ref: String,

    /// PKCS#10 certificate signing request, PEM encoded.
    pub csr: String,

    /// Where peers reach this provider.
    #[serde(default)]
    pub egress: Option<Egress>,

    /// What the provider claims to serve.
    #[serde(default)]
    pub capabilities: Option<Capabilities>,
}

/// A request and whatever has been decided about it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnrollmentRequest {
    /// Identifier for polling and for deciding.
    pub request_id: Uuid,

    /// The name asked for.
    pub site_name: String,

    /// The grid being joined.
    pub grid_network_ref: String,

    /// Where the request has got to.
    pub phase: EnrollmentPhase,

    /// When the request arrived.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,

    /// When it was approved or denied.
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    pub decided_at: Option<OffsetDateTime>,

    /// Who decided.
    ///
    /// Recorded so a grid can answer why a provider is trusted, which a
    /// hand-edited allow list cannot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decided_by: Option<String>,

    /// Why it was refused, when it was.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// The issued certificate, PEM encoded. Present once issued.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub certificate: Option<String>,

    /// The name the grid assigned, carried as a URI SAN on the certificate.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spiffe_id: Option<String>,

    /// Lowercase hex SHA-256 over the request's public key.
    ///
    /// Names the key rather than the certificate, so it survives reissue.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key_sha256: Option<String>,

    /// Where peers reach this provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub egress: Option<Egress>,

    /// What the provider claims to serve.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Capabilities>,
}

/// Everything a newly admitted provider needs to start participating.
///
/// The certificate on its own is not enough to join. A member has to verify its
/// peers, which needs the grid CA, and has to reach the gossip mesh, which needs
/// the transport key and somewhere to announce itself. Handing these over
/// together is what turns an issued certificate into membership.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JoiningKit {
    /// The certificate the grid issued, PEM encoded.
    pub certificate: String,

    /// The name bound into that certificate.
    pub spiffe_id: String,

    /// The grid CA, for verifying peers.
    ///
    /// Without this a member can prove who it is and still not know who anyone
    /// else is.
    pub ca_bundle: String,

    /// Shared key for the gossip transport, base64.
    ///
    /// A secret, and the reason collecting this is not open the way reading a
    /// certificate is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gossip_key: Option<String>,

    /// Peers to announce to when joining the mesh.
    #[serde(default)]
    pub seeds: Vec<String>,
}

/// Proof that the caller is the provider that made the request.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinProof {
    /// Signature over the request identifier, base64.
    ///
    /// Made with the private key whose public half the request carried, which
    /// only the provider that made it holds.
    pub signature: String,
}

/// Narrowing applied when listing.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListQuery {
    /// Return only requests in this phase.
    #[serde(default)]
    pub phase: Option<EnrollmentPhase>,
}

/// Why a request was refused.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DenyInput {
    /// What to record as the reason.
    #[serde(default)]
    pub reason: Option<String>,
}

/// A failure, described so a caller can act on it.
#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    /// Machine-readable code.
    pub error: String,

    /// What went wrong, and what would fix it.
    pub message: String,
}
