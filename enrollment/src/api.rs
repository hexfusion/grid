//! The HTTP interface, implementing `api/enrollment-v1.yaml`.
//!
//! An enrollee has no credentials on the grid's cluster, so it speaks HTTP
//! rather than the Kubernetes API. Submitting is separated from deciding: a
//! provider can ask, and only an operator can grant.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use certs::{CaCert, EnrollError, MAX_CSR_PEM_BYTES, sign_csr};
use uuid::Uuid;

use crate::{
    model::{DenyInput, EnrollmentRequest, EnrollmentRequestInput, ErrorBody},
    store::{Issued, NewRequest, Store, StoreError},
};

/// What the handlers need.
#[derive(Debug)]
pub struct AppState {
    /// Where requests are kept.
    pub store: Store,

    /// The CA that signs approved requests.
    pub ca: CaCert,
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

    /// The service itself failed.
    #[error("{0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::BadRequest { code, message } => (StatusCode::BAD_REQUEST, code, message),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "no enrollment request has that identifier".to_owned(),
            ),
            Self::Conflict { code, message } => (StatusCode::CONFLICT, code, message),
            Self::Internal(message) => {
                tracing::error!(error = %message, "enrollment request could not be served");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal",
                    "the enrollment service could not complete the request".to_owned(),
                )
            },
        };

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
    let checked = sign_csr(&state.ca, &input.site_name, &input.csr).map_err(signing_error)?;

    let created = state.store.create(NewRequest {
        site_name: input.site_name,
        grid_network_ref: input.grid_network_ref,
        csr_pem: input.csr,
        public_key_sha256: checked.public_key_sha256,
        egress: input.egress,
        capabilities: input.capabilities,
    })?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// Every request, newest first.
async fn list(State(state): State<Arc<AppState>>) -> Result<Json<Vec<EnrollmentRequest>>, ApiError> {
    Ok(Json(state.store.list()?))
}

/// One request, including the certificate once it has been issued.
async fn fetch(
    State(state): State<Arc<AppState>>,
    Path(request_id): Path<Uuid>,
) -> Result<Json<EnrollmentRequest>, ApiError> {
    Ok(Json(state.store.get(request_id)?.public))
}

/// Approve a request and issue the certificate.
///
/// The name granted is the name that was asked for. An operator approves or
/// denies rather than renaming, so what they saw is what gets signed.
async fn approve(
    State(state): State<Arc<AppState>>,
    Path(request_id): Path<Uuid>,
) -> Result<Json<EnrollmentRequest>, ApiError> {
    let stored = state.store.get(request_id)?;
    let issued = sign_csr(&state.ca, &stored.public.site_name, &stored.csr_pem).map_err(signing_error)?;

    let updated = state.store.mark_issued(
        request_id,
        Issued {
            certificate: issued.cert_pem,
            spiffe_id: issued.spiffe_id,
            decided_by: OPERATOR.to_owned(),
        },
    )?;

    tracing::info!(
        site = %updated.site_name,
        spiffe_id = ?updated.spiffe_id,
        "enrollment approved"
    );
    Ok(Json(updated))
}

/// Deny a request.
async fn deny(
    State(state): State<Arc<AppState>>,
    Path(request_id): Path<Uuid>,
    body: Option<Json<DenyInput>>,
) -> Result<Json<EnrollmentRequest>, ApiError> {
    let reason = body.and_then(|Json(input)| input.reason);
    let updated = state.store.mark_denied(request_id, OPERATOR.to_owned(), reason)?;
    tracing::info!(site = %updated.site_name, "enrollment denied");
    Ok(Json(updated))
}

/// Recorded as the approver until operator authentication is wired up.
///
/// The interface records who decided so a grid can answer why a provider is
/// trusted. Until the deciding routes sit behind authentication, that answer is
/// only as good as whoever can reach them.
const OPERATOR: &str = "unauthenticated-operator";
