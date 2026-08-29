//! Where requests are kept.
//!
//! A backend enum rather than a trait object, so the Postgres backend can be
//! added without every caller becoming generic. A MaaS deployment points this
//! at the `Postgres` it already runs; a standalone grid brings its own.

use std::{collections::HashMap, sync::Mutex};

use time::OffsetDateTime;
use uuid::Uuid;

pub mod postgres;

pub use postgres::PgStore;

use crate::model::{Capabilities, Egress, EnrollmentPhase, EnrollmentRequest};

/// Reasons a store operation could not be carried out.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// No request with that identifier.
    #[error("no such enrollment request")]
    NotFound,

    /// The request was already decided.
    ///
    /// Approval mints a certificate, so it has to happen at most once. A second
    /// approval of the same request is refused rather than issuing again.
    #[error("enrollment request was already decided")]
    AlreadyDecided,

    /// Another member already holds this name.
    #[error("site name is already taken")]
    NameTaken,

    /// The backend itself failed.
    #[error("store backend failed: {0}")]
    Backend(String),
}

/// A new request, before it has been decided.
#[derive(Debug, Clone)]
pub struct NewRequest {
    /// The name being asked for.
    pub site_name: String,

    /// The grid being joined.
    pub grid_network_ref: String,

    /// The request as submitted, kept so approval can sign it.
    pub csr_pem: String,

    /// Lowercase hex SHA-256 over the request's public key.
    pub public_key_sha256: String,

    /// Where peers reach this provider.
    pub egress: Option<Egress>,

    /// What the provider claims to serve.
    pub capabilities: Option<Capabilities>,
}

/// What approval recorded.
#[derive(Debug, Clone)]
pub struct Issued {
    /// The issued certificate, PEM encoded.
    pub certificate: String,

    /// The name bound into the certificate.
    pub spiffe_id: String,

    /// Who approved.
    pub decided_by: String,
}

/// A stored request, including material not put on the wire.
#[derive(Debug, Clone)]
pub struct StoredRequest {
    /// The public view.
    pub public: EnrollmentRequest,

    /// The request as submitted. Never serialized.
    pub csr_pem: String,
}

/// Where requests are kept.
#[derive(Debug)]
pub enum Store {
    /// Held in this process. Suits a standalone grid and the tests.
    ///
    /// Everything is lost on restart, and two replicas share nothing, so this
    /// is not a deployment a grid should depend on.
    Memory(MemoryStore),

    /// Held in Postgres. A MaaS deployment already runs one.
    Postgres(PgStore),
}

impl Store {
    /// A store that keeps requests in this process.
    #[must_use]
    pub fn memory() -> Self {
        Self::Memory(MemoryStore::default())
    }

    /// A store backed by Postgres, with the schema applied.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the database is unreachable or the
    /// schema cannot be applied.
    pub async fn postgres(url: &str) -> Result<Self, StoreError> {
        Ok(Self::Postgres(PgStore::connect(url).await?))
    }

    /// Record a new request in [`EnrollmentPhase::Pending`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NameTaken`] if an issued member already holds the
    /// name, so two providers cannot end up with the same identity.
    pub async fn create(&self, new: NewRequest) -> Result<EnrollmentRequest, StoreError> {
        match self {
            Self::Memory(store) => store.create(new),
            Self::Postgres(store) => store.create(new).await,
        }
    }

    /// Every request, newest first.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn list(&self) -> Result<Vec<EnrollmentRequest>, StoreError> {
        match self {
            Self::Memory(store) => store.list(),
            Self::Postgres(store) => store.list().await,
        }
    }

    /// One request by identifier.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] if no request has that identifier.
    pub async fn get(&self, request_id: Uuid) -> Result<StoredRequest, StoreError> {
        match self {
            Self::Memory(store) => store.get(request_id),
            Self::Postgres(store) => store.get(request_id).await,
        }
    }

    /// Move a pending request to [`EnrollmentPhase::Issued`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AlreadyDecided`] if the request was already
    /// decided, and [`StoreError::NameTaken`] if the name was granted to
    /// someone else while this request waited.
    pub async fn mark_issued(&self, request_id: Uuid, issued: Issued) -> Result<EnrollmentRequest, StoreError> {
        match self {
            Self::Memory(store) => store.mark_issued(request_id, issued),
            Self::Postgres(store) => store.mark_issued(request_id, issued).await,
        }
    }

    /// Move a pending request to [`EnrollmentPhase::Denied`].
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::AlreadyDecided`] if the request was already decided.
    pub async fn mark_denied(
        &self,
        request_id: Uuid,
        decided_by: String,
        reason: Option<String>,
    ) -> Result<EnrollmentRequest, StoreError> {
        match self {
            Self::Memory(store) => store.mark_denied(request_id, decided_by, reason),
            Self::Postgres(store) => store.mark_denied(request_id, decided_by, reason).await,
        }
    }
}

/// Requests held in this process.
#[derive(Debug, Default)]
pub struct MemoryStore {
    /// One lock over both fields, so listing cannot observe a half-written
    /// request and there is no lock order to get wrong.
    inner: Mutex<Inner>,
}

/// The requests, and the order they arrived in.
#[derive(Debug, Default)]
struct Inner {
    /// Insertion-ordered so listing can be newest first.
    order: Vec<Uuid>,

    /// The requests themselves.
    by_id: HashMap<Uuid, StoredRequest>,
}

impl MemoryStore {
    /// Whether an issued member other than `except` already holds this name.
    ///
    /// A request being approved is skipped, since its own issued row would
    /// otherwise read as somebody else holding the name.
    fn name_is_taken(by_id: &HashMap<Uuid, StoredRequest>, site_name: &str, except: Option<Uuid>) -> bool {
        by_id.iter().any(|(id, row)| {
            Some(*id) != except && row.public.phase == EnrollmentPhase::Issued && row.public.site_name == site_name
        })
    }

    /// Record a new request.
    fn create(&self, new: NewRequest) -> Result<EnrollmentRequest, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        if Self::name_is_taken(&inner.by_id, &new.site_name, None) {
            return Err(StoreError::NameTaken);
        }

        let request_id = Uuid::new_v4();
        let public = EnrollmentRequest {
            request_id,
            site_name: new.site_name,
            grid_network_ref: new.grid_network_ref,
            phase: EnrollmentPhase::Pending,
            created_at: OffsetDateTime::now_utc(),
            decided_at: None,
            decided_by: None,
            reason: None,
            certificate: None,
            spiffe_id: None,
            public_key_sha256: Some(new.public_key_sha256),
            egress: new.egress,
            capabilities: new.capabilities,
        };

        inner.by_id.insert(
            request_id,
            StoredRequest {
                public: public.clone(),
                csr_pem: new.csr_pem,
            },
        );
        inner.order.push(request_id);
        drop(inner);

        Ok(public)
    }

    /// Every request, newest first.
    fn list(&self) -> Result<Vec<EnrollmentRequest>, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        Ok(inner
            .order
            .iter()
            .rev()
            .filter_map(|id| inner.by_id.get(id).map(|row| row.public.clone()))
            .collect())
    }

    /// One request by identifier.
    fn get(&self, request_id: Uuid) -> Result<StoredRequest, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        inner.by_id.get(&request_id).cloned().ok_or(StoreError::NotFound)
    }

    /// Move a pending request to issued.
    fn mark_issued(&self, request_id: Uuid, issued: Issued) -> Result<EnrollmentRequest, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;

        let existing = inner.by_id.get(&request_id).ok_or(StoreError::NotFound)?;
        // Phase first. A request already issued is a repeat approval, not a
        // name collision, and has to be reported as such.
        if existing.public.phase != EnrollmentPhase::Pending {
            return Err(StoreError::AlreadyDecided);
        }

        let site_name = existing.public.site_name.clone();
        if Self::name_is_taken(&inner.by_id, &site_name, Some(request_id)) {
            return Err(StoreError::NameTaken);
        }

        let row = inner.by_id.get_mut(&request_id).ok_or(StoreError::NotFound)?;

        row.public.phase = EnrollmentPhase::Issued;
        row.public.decided_at = Some(OffsetDateTime::now_utc());
        row.public.decided_by = Some(issued.decided_by);
        row.public.certificate = Some(issued.certificate);
        row.public.spiffe_id = Some(issued.spiffe_id);
        let updated = row.public.clone();
        drop(inner);
        Ok(updated)
    }

    /// Move a pending request to denied.
    fn mark_denied(
        &self,
        request_id: Uuid,
        decided_by: String,
        reason: Option<String>,
    ) -> Result<EnrollmentRequest, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let row = inner.by_id.get_mut(&request_id).ok_or(StoreError::NotFound)?;
        if row.public.phase != EnrollmentPhase::Pending {
            return Err(StoreError::AlreadyDecided);
        }

        row.public.phase = EnrollmentPhase::Denied;
        row.public.decided_at = Some(OffsetDateTime::now_utc());
        row.public.decided_by = Some(decided_by);
        row.public.reason = reason;
        let updated = row.public.clone();
        drop(inner);
        Ok(updated)
    }
}

/// A poisoned lock means another thread panicked holding it.
fn poisoned() -> StoreError {
    StoreError::Backend("in-memory store lock was poisoned".to_owned())
}
