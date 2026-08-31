//! The HTTP interface, implementing `api/enrollment-v1.yaml`.
//!
//! An enrollee has no credentials on the grid's cluster, so it speaks HTTP
//! rather than the Kubernetes API. Submitting is separated from deciding: a
//! provider can ask, and only an operator can grant.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, FromRequestParts, MatchedPath, Path, Query, State},
    http::{Method, StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine as _;
use certs::{CaCert, EnrollError, MAX_CSR_PEM_BYTES, Validity, csr_public_key, sign_csr};
use uuid::Uuid;

use crate::{
    authz::{Authorizer, AuthzError, Operation},
    model::{
        DenyInput, EnrollmentPhase, EnrollmentRequest, EnrollmentRequestInput, ErrorBody, JoinProof, JoiningKit,
        ListQuery,
    },
    store::{Issued, NewRequest, Store, StoreError},
};

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
    if path.ends_with("/approve") || path.ends_with("/deny") {
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
        .route("/v1/requests/{request_id}", get(fetch))
        .route("/v1/requests/{request_id}/approve", post(approve))
        .route("/v1/requests/{request_id}/deny", post(deny))
        .route("/v1/requests/{request_id}/join", post(join))
        .layer(DefaultBodyLimit::max(MAX_CSR_PEM_BYTES.saturating_mul(2)))
        .with_state(state)
}

/// Submit a request.
///
/// The request is verified here rather than at approval, so an operator is never
/// shown something that cannot be signed, and unusable submissions are not
/// stored.
async fn create(
    State(state): State<Arc<AppState>>,
    Json(input): Json<EnrollmentRequestInput>,
) -> Result<(StatusCode, Json<EnrollmentRequest>), ApiError> {
    if input.grid_network_ref.trim().is_empty() {
        return Err(ApiError::BadRequest {
            code: "missing_grid_network",
            message: "gridNetworkRef must name the grid being joined".to_owned(),
        });
    }

    // Signing here proves the submission is usable and yields the key
    // fingerprint. The certificate is thrown away; only approval issues one.
    let checked = issue_for(&state, &input.site_name, &input.csr)?;

    let created = state
        .store
        .create(NewRequest {
            site_name: input.site_name,
            grid_network_ref: input.grid_network_ref,
            csr_pem: input.csr,
            public_key_sha256: checked.public_key_sha256,
            egress: input.egress,
            capabilities: input.capabilities,
        })
        .await?;

    Ok((StatusCode::CREATED, Json(created)))
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
            },
        )
        .await
    {
        Ok(updated) => updated,
        // Another approval won the race. Its certificate is the one that counts.
        Err(StoreError::AlreadyDecided) => state.store.get(request_id).await?.public,
        Err(err) => return Err(err.into()),
    };

    tracing::info!(
        site = %updated.site_name,
        spiffe_id = ?updated.spiffe_id,
        %operator,
        "enrollment approved"
    );
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
