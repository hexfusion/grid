//! The HTTP interface, implementing `api/enrollment-v1.yaml`.
//!
//! An enrollee has no credentials on the grid's cluster, so it speaks HTTP
//! rather than the Kubernetes API. Submitting is separated from deciding: a
//! provider can ask, and only an operator can grant.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, MatchedPath, Path, Query, State},
    http::{HeaderMap, Method, StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine as _;
use certs::{CaCert, EnrollError, MAX_CSR_PEM_BYTES, Validity, csr_public_key, sign_csr};
use ring::rand::SecureRandom as _;
use uuid::Uuid;

use crate::{
    auth::token_digest,
    authz::{Authorizer, AuthzError, Operation},
    metrics::{self, RejectReason},
    model::{
        DenyInput, EnrollmentPhase, EnrollmentRequest, EnrollmentRequestInput, ErrorBody, InviteInput, IssuedInvite,
        JoinProof, JoiningKit, ListQuery,
    },
    store::{Invite, Issued, NewInvite, NewRequest, Store, StoreError},
};

/// Header the enrollee presents its site token in on submit.
const INVITE_HEADER: &str = "x-grid-invite";

/// How long a minted invite is redeemable when the operator names no expiry.
const DEFAULT_INVITE_LIFETIME: time::Duration = time::Duration::days(7);

/// What the handlers need.
#[derive(Debug)]
pub struct AppState {
    /// Where requests are kept.
    pub store: Store,

    /// The CA that signs approved requests.
    pub ca: CaCert,

    /// How decisions are authorized (operator token table, or Kubernetes RBAC).
    pub authorizer: Authorizer,

    /// How long an issued certificate lasts.
    ///
    /// Held here rather than taken per call, so every certificate this grid
    /// issues has the same bound and no route can quietly issue a longer one.
    pub cert_lifetime: time::Duration,

    /// What a newly admitted provider is handed besides its certificate.
    pub joining: JoiningConfig,
}

/// The parts of a joining kit that are the same for every member.
#[derive(Debug, Clone, Default)]
pub struct JoiningConfig {
    /// Shared gossip transport key, base64. `None` leaves a member unable to
    /// join the mesh, so the service says so at startup.
    pub gossip_key: Option<String>,

    /// Peers a joining member announces to.
    pub seeds: Vec<String>,
}

/// Failures the interface can report.
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// The submission was not usable.
    #[error("{message}")]
    BadRequest {
        /// Machine-readable code.
        code: &'static str,
        /// What went wrong.
        message: String,
    },

    /// No request with that identifier.
    #[error("no such enrollment request")]
    NotFound,

    /// The request was already decided.
    #[error("enrollment request was already decided")]
    Conflict {
        /// Machine-readable code.
        code: &'static str,
        /// What went wrong.
        message: String,
    },

    /// The caller presented no operator credential, or one that is not known.
    #[error("an operator credential is required")]
    Unauthorized,

    /// The submission carried no usable site token.
    ///
    /// Missing, unknown, already redeemed, or expired all land here: submit is
    /// closed without a valid invite, and the distinction is not the enrollee's
    /// to act on beyond getting a fresh token.
    #[error("{message}")]
    InviteRejected {
        /// Machine-readable code.
        code: &'static str,
        /// What went wrong.
        message: String,
    },

    /// The caller authenticated but is not permitted the action.
    #[error("not permitted")]
    Forbidden,

    /// The service itself failed.
    #[error("{0}")]
    Internal(String),
}

impl From<AuthzError> for ApiError {
    fn from(error: AuthzError) -> Self {
        match error {
            AuthzError::Unauthenticated => Self::Unauthorized,
            AuthzError::Forbidden(_) => Self::Forbidden,
            AuthzError::Backend(message) => Self::Internal(message),
        }
    }
}

/// An authenticated operator.
///
/// Extracting this is what gates a decision, so a handler that takes it cannot
/// be reached without a credential.
#[derive(Debug, Clone)]
pub struct Operator(pub String);

impl FromRequestParts<Arc<AppState>> for Operator {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &Arc<AppState>) -> Result<Self, Self::Rejection> {
        let presented = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .ok_or(ApiError::Unauthorized)?;

        // The action being authorized, derived from the matched route. The local
        // token backend ignores it; the Kubernetes-RBAC backend maps it to a
        // SubjectAccessReview.
        let operation = route_operation(parts);

        // An unknown token and a missing one are reported the same way, so the
        // interface cannot be used to test whether a token exists.
        state
            .authorizer
            .decide(presented, operation)
            .await
            .map(Self)
            .map_err(ApiError::from)
    }
}

/// The authorization operation for the matched route.
///
/// Deciding (approve or deny) is `update` on the `approval` subresource, the way
/// Kubernetes models CSR approval; reads are `list`/`get` on the resource.
fn route_operation(parts: &Parts) -> Operation {
    let path = parts.extensions.get::<MatchedPath>().map_or("", MatchedPath::as_str);
    // Issuing an invite admits a site as surely as approving does, so it takes the
    // same decide permission rather than a weaker read.
    if path.ends_with("/approve") || path.ends_with("/deny") || path.ends_with("/invites") {
        Operation {
            verb: "update",
            subresource: Some("approval"),
        }
    } else if parts.method == Method::GET {
        Operation {
            verb: "list",
            subresource: None,
        }
    } else {
        Operation {
            verb: "get",
            subresource: None,
        }
    }
}

impl ApiError {
    /// The HTTP status, machine-readable code, and human message for the wire.
    fn rendered(self) -> (StatusCode, &'static str, String) {
        match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "no enrollment request has that identifier".to_owned(),
            ),
            Self::Conflict { code, message } => (StatusCode::CONFLICT, code, message),
            Self::Unauthorized => (
                StatusCode::UNAUTHORIZED,
                "unauthorized",
                "deciding on enrollment requests requires an operator credential".to_owned(),
            ),
            Self::InviteRejected { code, message } => (StatusCode::UNAUTHORIZED, code, message),
            Self::Forbidden => (
                StatusCode::FORBIDDEN,
                "forbidden",
                "not permitted to decide on enrollment requests".to_owned(),
            ),
            Self::Internal(message) => {
                tracing::error!(error = %message, "enrollment request could not be served");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "the enrollment service could not complete the request".to_owned(),
                )
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = self.rendered();
        (
            status,
            Json(ErrorBody {
                error: code.to_owned(),
                message,
            }),
        )
            .into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::NotFound => Self::NotFound,
            StoreError::AlreadyDecided => Self::Conflict {
                code: "already_decided",
                message: "this request was already approved or denied".to_owned(),
            },
            StoreError::NameTaken => Self::Conflict {
                code: "name_taken",
                message: "another member already holds this site name".to_owned(),
            },
            StoreError::InviteNotFound => Self::InviteRejected {
                code: "invalid_invite",
                message: "a valid site token is required to enroll".to_owned(),
            },
            StoreError::InviteUnavailable => Self::InviteRejected {
                code: "invite_unavailable",
                message: "this site token has already been redeemed or has expired".to_owned(),
            },
            StoreError::Backend(detail) => Self::Internal(detail),
        }
    }
}

/// Turn a signing refusal into something the caller can act on.
///
/// A refused request is the caller's to fix, except for a signing fault, which
/// is the grid's.
fn signing_error(err: EnrollError) -> ApiError {
    match err {
        EnrollError::Signing(detail) => ApiError::Internal(detail),
        EnrollError::TooLarge
        | EnrollError::Malformed
        | EnrollError::BadSignature
        | EnrollError::UnsupportedExtension
        | EnrollError::InvalidSiteName => ApiError::BadRequest {
            code: "invalid_csr",
            message: err.to_string(),
        },
    }
}

/// The routes, with a body limit sized for a certificate request.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/v1/requests", post(create).get(list))
        .route("/v1/invites", post(issue_invite))
        .route("/v1/requests/{request_id}", get(fetch).delete(remove))
        // Retained legacy of the manual decision path that auto-issue supersedes.
        // Deleting these (and their store/model support) is the one remaining
        // step and needs Sam's direct authorization; see the branch note.
        .route("/v1/requests/{request_id}/approve", post(approve))
        .route("/v1/requests/{request_id}/deny", post(deny))
        .route("/v1/requests/{request_id}/join", post(join))
        .route("/metrics", get(metrics_handler))
        .layer(DefaultBodyLimit::max(MAX_CSR_PEM_BYTES.saturating_mul(2)))
        .with_state(state)
}

/// Submit a request, and issue on the spot.
///
/// Closed by the site token: an operator mints an invite that pins the name and
/// region, and submit needs it. Redeeming that one-shot invite issues the
/// certificate directly, with no separate approve step. The name and region come
/// from the invite, not the enrollee, so the CSR contributes only its key.
///
/// Restart-safe: a redeemed invite reads its issued record back rather than
/// minting again, so an operator re-submitting after a restart is a read, not a
/// replay. The redeem lock is the anti-replay for a genuinely new issue.
async fn create(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(input): Json<EnrollmentRequestInput>,
) -> Result<(StatusCode, Json<EnrollmentRequest>), ApiError> {
    let token = presented_invite(&headers).inspect_err(|_no_token| {
        metrics::record_rejection(RejectReason::NoToken);
    })?;
    let digest = token_digest(&token);

    let invite = match state.store.find_invite(&digest).await {
        Ok(invite) => invite,
        Err(err @ StoreError::InviteNotFound) => {
            metrics::record_rejection(RejectReason::InvalidInvite);
            return Err(err.into());
        },
        Err(err) => return Err(err.into()),
    };

    // Restart-safe: a spent invite returns the certificate it already
    // bootstrapped rather than being refused as a replay.
    if let Some(existing) = redeemed_outcome(&state, &invite).await? {
        return Ok((StatusCode::OK, Json(existing)));
    }

    if invite.expires_at <= time::OffsetDateTime::now_utc() {
        metrics::record_rejection(RejectReason::Expired);
        return Err(ApiError::InviteRejected {
            code: "invite_unavailable",
            message: "this site token has expired".to_owned(),
        });
    }

    // Box the mint future to keep this handler's stack frame small.
    let issued = Box::pin(mint(&state, &invite, &digest, input)).await?;
    Ok((StatusCode::CREATED, Json(issued)))
}

/// Sign, store, redeem, and record an issue for an unredeemed invite.
///
/// Redeem lands before the mark, so the one-shot lock is the anti-replay: a
/// racing second submit of the same token is undone here and reads the issued
/// record back on retry. The cost of that order: if the mark fails after the
/// redeem succeeds, the invite is spent but the row stays Pending with no cert,
/// and `redeemed_outcome` then refuses the retry. Recovery today is the retained
/// `approve` on that Pending row; resuming it automatically is a tracked
/// follow-up.
///
/// `sign_csr` rebuilds every SAN from the assigned name and drops the CSR's
/// requested names, so the grant is exactly name, region, and grid; egress and
/// capabilities are enrollee-asserted, bounded by the mTLS identity this issues,
/// not part of the grant.
#[expect(
    clippy::too_many_lines,
    reason = "one linear issue sequence: sign, store, redeem, mark"
)]
async fn mint(
    state: &AppState,
    invite: &Invite,
    digest: &str,
    input: EnrollmentRequestInput,
) -> Result<EnrollmentRequest, ApiError> {
    let site_name = invite.site_name.clone().unwrap_or_else(|| input.site_name.clone());
    let issued = issue_for(state, &site_name, &input.csr).inspect_err(|err| {
        if matches!(
            err,
            ApiError::BadRequest {
                code: "invalid_csr",
                ..
            }
        ) {
            metrics::record_rejection(RejectReason::InvalidCsr);
        }
    })?;

    let created = state
        .store
        .create(NewRequest {
            site_name,
            grid_network_ref: invite.grid_network_ref.clone(),
            region: invite.region.clone(),
            csr_pem: input.csr,
            public_key_sha256: issued.public_key_sha256.clone(),
            egress: input.egress,
            capabilities: input.capabilities,
        })
        .await
        .inspect_err(record_name_taken)?;

    if let Err(err) = state.store.redeem_invite(digest, created.request_id).await {
        // The real lost-race is an already-redeemed invite; a backend blip is not,
        // so only the race counts as a rejection. If the undo delete itself fails
        // the pending row is a rare orphan, logged but not otherwise cleaned up.
        let _undone = state.store.delete(created.request_id).await;
        if matches!(err, StoreError::InviteUnavailable) {
            metrics::record_rejection(RejectReason::AlreadyRedeemed);
        }
        return Err(err.into());
    }

    let issue = Issued {
        certificate: issued.cert_pem,
        spiffe_id: issued.spiffe_id,
        decided_by: invite.issued_by.clone(),
        invite_id: Some(invite.id),
    };
    let updated = match state.store.mark_issued(created.request_id, issue).await {
        Ok(updated) => updated,
        // Defensive no-op: this request id is freshly minted and unseen by any
        // other caller, so nothing else can have decided it. Kept for safety.
        Err(StoreError::AlreadyDecided) => state.store.get(created.request_id).await?.public,
        // The invite is already spent, so a mark failure here (a backend error, or
        // a name-collision surfacing late at the unique index) strands the row
        // Pending with no cert; `redeemed_outcome` then refuses the retry, and the
        // retained `approve` on that Pending row is the only recovery until the
        // resume-on-retry follow-up lands. See the branch note.
        Err(err) => {
            record_name_taken(&err);
            return Err(err.into());
        },
    };

    tracing::info!(site = %updated.site_name, invite = %invite.id, operator = %invite.issued_by, "enrollment auto-issued");
    Ok(updated)
}

/// Count a name-collision refusal.
///
/// A valid invite whose pinned name is already held is the silent refusal
/// auto-issue could otherwise leave uncounted, so it joins the rejection metric.
fn record_name_taken(err: &StoreError) {
    if matches!(err, StoreError::NameTaken) {
        metrics::record_rejection(RejectReason::NameTaken);
    }
}

/// The certificate a spent invite already bootstrapped, if any.
///
/// A redeemed invite is not a fresh grant. When its redemption produced an
/// Issued record, that record comes back, so a retry (an operator re-submitting
/// after a restart) reads the certificate rather than being refused. Anything
/// else, a spent invite with no Issued record (including the rare redeemed-but-
/// Pending row from a mid-mint failure), is a refusal by design: recovery of
/// that stranded case is the retained `approve` path, not this read.
async fn redeemed_outcome(state: &AppState, invite: &Invite) -> Result<Option<EnrollmentRequest>, ApiError> {
    if invite.redeemed_at.is_none() {
        return Ok(None);
    }
    if let Some(request_id) = invite.redeemed_by
        && let Ok(stored) = state.store.get(request_id).await
        && stored.public.phase == EnrollmentPhase::Issued
    {
        return Ok(Some(stored.public));
    }
    metrics::record_rejection(RejectReason::AlreadyRedeemed);
    Err(ApiError::InviteRejected {
        code: "invite_unavailable",
        message: "this site token has already been redeemed".to_owned(),
    })
}

/// The site token the submission carried, or a refusal when it carried none.
fn presented_invite(headers: &HeaderMap) -> Result<String, ApiError> {
    headers
        .get(INVITE_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| ApiError::InviteRejected {
            code: "invite_required",
            message: "enrolling requires a site token in the X-Grid-Invite header".to_owned(),
        })
}

/// Mint a site token pinning a name and region, returning it once.
///
/// The federation-plane counterpart to a consumer API key: an operator issues
/// it, hands it to the site, and the site presents it once to start enrolling.
async fn issue_invite(
    State(state): State<Arc<AppState>>,
    Operator(operator): Operator,
    Json(input): Json<InviteInput>,
) -> Result<(StatusCode, Json<IssuedInvite>), ApiError> {
    if input.grid_network_ref.trim().is_empty() {
        return Err(ApiError::BadRequest {
            code: "missing_grid_network",
            message: "gridNetworkRef must name the grid being joined".to_owned(),
        });
    }

    let token = new_invite_token()?;
    let lifetime = input
        .expires_in_secs
        .filter(|secs| *secs > 0)
        .map_or(DEFAULT_INVITE_LIFETIME, time::Duration::seconds);
    let expires_at = time::OffsetDateTime::now_utc().saturating_add(lifetime);

    let stored = state
        .store
        .create_invite(NewInvite {
            token_sha256: token_digest(&token),
            site_name: Some(input.site_name),
            region: Some(input.region),
            grid_network_ref: input.grid_network_ref,
            issued_by: operator.clone(),
            expires_at,
        })
        .await?;

    tracing::info!(invite = %stored.id, site = ?stored.site_name, region = ?stored.region, %operator, "site token issued");
    Ok((StatusCode::CREATED, Json(issued_invite(stored, token))))
}

/// Render a stored invite plus its one-time token for the response.
fn issued_invite(stored: Invite, token: String) -> IssuedInvite {
    IssuedInvite {
        invite_id: stored.id,
        token,
        site_name: stored.site_name.unwrap_or_default(),
        region: stored.region.unwrap_or_default(),
        grid_network_ref: stored.grid_network_ref,
        expires_at: stored.expires_at,
    }
}

/// A random bearer token for a site invite.
///
/// 32 bytes from the system CSPRNG, URL-safe so it drops straight into a header.
fn new_invite_token() -> Result<String, ApiError> {
    let mut bytes = [0_u8; 32];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_unavailable| ApiError::Internal("could not generate a site token".to_owned()))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

/// Every request, newest first.
///
/// Operator-only: a pending request names a provider that asked to join, which
/// is not public.
async fn list(
    State(state): State<Arc<AppState>>,
    _operator: Operator,
    Query(query): Query<ListQuery>,
) -> Result<Json<Vec<EnrollmentRequest>>, ApiError> {
    let mut rows = state.store.list().await?;
    if let Some(phase) = query.phase {
        rows.retain(|row| row.phase == phase);
    }
    Ok(Json(rows))
}

/// One request, including the certificate once it has been issued.
async fn fetch(
    State(state): State<Arc<AppState>>,
    Path(request_id): Path<Uuid>,
) -> Result<Json<EnrollmentRequest>, ApiError> {
    Ok(Json(state.store.get(request_id).await?.public))
}

/// Approve a request and issue the certificate.
///
/// The name granted is the name that was asked for. An operator approves or
/// denies rather than renaming, so what they saw is what gets signed.
async fn approve(
    State(state): State<Arc<AppState>>,
    Operator(operator): Operator,
    Path(request_id): Path<Uuid>,
) -> Result<Json<EnrollmentRequest>, ApiError> {
    let stored = state.store.get(request_id).await?;
    if let Some(settled) = settled_outcome(&stored.public)? {
        return Ok(Json(settled));
    }

    let issued = issue_for(&state, &stored.public.site_name, &stored.csr_pem)?;

    let updated = match state
        .store
        .mark_issued(
            request_id,
            Issued {
                certificate: issued.cert_pem,
                spiffe_id: issued.spiffe_id,
                decided_by: operator.clone(),
                invite_id: None,
            },
        )
        .await
    {
        Ok(updated) => updated,
        // Another approval won the race. Its certificate is the one that counts.
        Err(StoreError::AlreadyDecided) => state.store.get(request_id).await?.public,
        Err(err) => return Err(err.into()),
    };

    tracing::info!(site = %updated.site_name, spiffe_id = ?updated.spiffe_id, %operator, "enrollment approved");
    Ok(Json(updated))
}

/// Collect what is needed to start participating.
///
/// Not open the way reading a certificate is: the kit carries the gossip key,
/// which is a secret. The caller proves it is the provider that made the request
/// by signing the request identifier with the key half it kept, which the grid
/// checks against the public half the request published.
async fn join(
    State(state): State<Arc<AppState>>,
    Path(request_id): Path<Uuid>,
    Json(proof): Json<JoinProof>,
) -> Result<Json<JoiningKit>, ApiError> {
    let stored = state.store.get(request_id).await?;

    let (Some(certificate), Some(spiffe_id)) = (stored.public.certificate, stored.public.spiffe_id) else {
        return Err(ApiError::Conflict {
            code: "not_issued",
            message: "this request has no certificate yet, so there is nothing to join with".to_owned(),
        });
    };

    proves_possession(&stored.csr_pem, request_id, &proof.signature)?;

    tracing::info!(site = %stored.public.site_name, "joining kit collected");
    Ok(Json(JoiningKit {
        certificate,
        spiffe_id,
        ca_bundle: state.ca.cert_pem.clone(),
        gossip_key: state.joining.gossip_key.clone(),
        seeds: state.joining.seeds.clone(),
    }))
}

/// Check that the caller holds the key the request was made with.
///
/// The request identifier is what gets signed. It is unguessable and specific to
/// one request, so a signature over it cannot be replayed onto another.
fn proves_possession(csr_pem: &str, request_id: Uuid, signature: &str) -> Result<(), ApiError> {
    let refused = || ApiError::Unauthorized;

    let signature = base64::engine::general_purpose::STANDARD
        .decode(signature)
        .map_err(|_bad| refused())?;
    let public_key = csr_public_key(csr_pem).map_err(|_bad| refused())?;

    ring::signature::UnparsedPublicKey::new(&ring::signature::ECDSA_P256_SHA256_ASN1, &public_key)
        .verify(request_id.as_bytes(), &signature)
        .map_err(|_bad| refused())
}

/// Sign a request under this grid's CA and lifetime.
///
/// One place decides how long an issued certificate lasts, so no route can
/// quietly issue a longer one than the grid was configured for.
fn issue_for(state: &AppState, site_name: &str, csr_pem: &str) -> Result<certs::EnrolledCert, ApiError> {
    sign_csr(
        &state.ca,
        site_name,
        csr_pem,
        Validity::starting_now(state.cert_lifetime),
    )
    .map_err(signing_error)
}

/// The record to return for a request that has already been decided.
///
/// Retrying an approval must not sign a second certificate over the same key, so
/// an already issued request comes back as it stands. A denied one is a
/// contradiction rather than a retry.
fn settled_outcome(record: &EnrollmentRequest) -> Result<Option<EnrollmentRequest>, ApiError> {
    match record.phase {
        EnrollmentPhase::Issued => Ok(Some(record.clone())),
        EnrollmentPhase::Denied | EnrollmentPhase::Failed => Err(ApiError::Conflict {
            code: "already_decided",
            message: "this request was already denied".to_owned(),
        }),
        EnrollmentPhase::Pending => Ok(None),
    }
}

/// Deny a request.
async fn deny(
    State(state): State<Arc<AppState>>,
    Operator(operator): Operator,
    Path(request_id): Path<Uuid>,
    body: Option<Json<DenyInput>>,
) -> Result<Json<EnrollmentRequest>, ApiError> {
    let reason = body.and_then(|Json(input)| input.reason);
    let updated = state.store.mark_denied(request_id, operator.clone(), reason).await?;
    tracing::info!(site = %updated.site_name, %operator, "enrollment denied");
    Ok(Json(updated))
}

/// Delete an enrollment request. Requires an operator token.
async fn remove(
    State(state): State<Arc<AppState>>,
    Operator(operator): Operator,
    Path(request_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.store.delete(request_id).await?;
    tracing::info!(%request_id, %operator, "enrollment deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Prometheus exposition.
///
/// Open, so a scrape needs no operator credential: it carries counts, never a
/// token or a certificate.
async fn metrics_handler() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics::encode(),
    )
}
