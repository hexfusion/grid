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

    /// No invite matched the presented token.
    #[error("no such enrollment invite")]
    InviteNotFound,

    /// The invite was already redeemed or has expired.
    ///
    /// An invite buys one enrollment, not a standing right to ask, so a spent or
    /// lapsed one is refused rather than honored again.
    #[error("enrollment invite is already redeemed or expired")]
    InviteUnavailable,

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

    /// The geo-fence region, stamped from the redeemed invite.
    pub region: Option<String>,

    /// The request as submitted, kept so approval can sign it.
    pub csr_pem: String,

    /// Lowercase hex SHA-256 over the request's public key.
    pub public_key_sha256: String,

    /// Where peers reach this provider.
    pub egress: Option<Egress>,

    /// What the provider claims to serve.
    pub capabilities: Option<Capabilities>,
}

impl NewRequest {
    /// The pending record this submission becomes, under `request_id`.
    fn into_pending(self, request_id: Uuid) -> StoredRequest {
        StoredRequest {
            public: EnrollmentRequest {
                request_id,
                site_name: self.site_name,
                grid_network_ref: self.grid_network_ref,
                region: self.region,
                phase: EnrollmentPhase::Pending,
                created_at: OffsetDateTime::now_utc(),
                decided_at: None,
                decided_by: None,
                reason: None,
                certificate: None,
                spiffe_id: None,
                public_key_sha256: Some(self.public_key_sha256),
                egress: self.egress,
                capabilities: self.capabilities,
            },
            csr_pem: self.csr_pem,
        }
    }
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

/// An invite to store, before it has been redeemed.
///
/// Carries the token's digest, never the token: the raw token is handed to the
/// operator once and only its digest is kept, like the operator tokens.
#[derive(Debug, Clone)]
pub struct NewInvite {
    /// Lowercase hex SHA-256 of the bearer token.
    pub token_sha256: String,

    /// The name the enrollee is pinned to, when the invite pins one.
    pub site_name: Option<String>,

    /// The geo-fence region pinned onto the enrollee.
    pub region: Option<String>,

    /// The grid this invite admits into.
    pub grid_network_ref: String,

    /// The operator that minted it.
    pub issued_by: String,

    /// When the token stops being redeemable.
    pub expires_at: OffsetDateTime,
}

/// A stored invite. The token digest is never surfaced.
#[derive(Debug, Clone)]
pub struct Invite {
    /// The invite's identifier.
    pub id: Uuid,

    /// The pinned name, when one was pinned.
    pub site_name: Option<String>,

    /// The pinned geo-fence region.
    pub region: Option<String>,

    /// The grid this invite admits into.
    pub grid_network_ref: String,

    /// The operator that minted it.
    pub issued_by: String,

    /// When it was minted.
    pub created_at: OffsetDateTime,

    /// When it stops being redeemable.
    pub expires_at: OffsetDateTime,

    /// When it was redeemed, if it has been.
    pub redeemed_at: Option<OffsetDateTime>,

    /// The request the redemption bootstrapped, if any.
    pub redeemed_by: Option<Uuid>,
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

    /// Delete a request, whatever phase it is in.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::NotFound`] if no request has that identifier.
    pub async fn delete(&self, request_id: Uuid) -> Result<(), StoreError> {
        match self {
            Self::Memory(store) => store.delete(request_id),
            Self::Postgres(store) => store.delete(request_id).await,
        }
    }

    /// Record a new invite, keyed by its token digest.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the backend failed.
    pub async fn create_invite(&self, new: NewInvite) -> Result<Invite, StoreError> {
        match self {
            Self::Memory(store) => store.create_invite(new),
            Self::Postgres(store) => store.create_invite(new).await,
        }
    }

    /// Look an invite up by its token digest, redeemed or not.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InviteNotFound`] if no invite has that digest.
    pub async fn find_invite(&self, token_sha256: &str) -> Result<Invite, StoreError> {
        match self {
            Self::Memory(store) => store.find_invite(token_sha256),
            Self::Postgres(store) => store.find_invite(token_sha256).await,
        }
    }

    /// Redeem an invite for `redeemed_by`, one-shot and guarded on expiry.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InviteNotFound`] if the digest is unknown, and
    /// [`StoreError::InviteUnavailable`] if it was already redeemed or has
    /// expired, so a token cannot bootstrap two sites.
    pub async fn redeem_invite(&self, token_sha256: &str, redeemed_by: Uuid) -> Result<Invite, StoreError> {
        match self {
            Self::Memory(store) => store.redeem_invite(token_sha256, redeemed_by),
            Self::Postgres(store) => store.redeem_invite(token_sha256, redeemed_by).await,
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

    /// Invites, keyed by token digest. Under the same lock as the requests, so
    /// redeeming one and creating the request it authorizes cannot interleave.
    invites: HashMap<String, Invite>,
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
        let stored = new.into_pending(request_id);
        let public = stored.public.clone();
        inner.by_id.insert(request_id, stored);
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

    /// Remove a request from both the index and the arrival order.
    fn delete(&self, request_id: Uuid) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let existed = inner.by_id.remove(&request_id).is_some();
        inner.order.retain(|id| *id != request_id);
        drop(inner);
        existed.then_some(()).ok_or(StoreError::NotFound)
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

    /// Record a new invite.
    fn create_invite(&self, new: NewInvite) -> Result<Invite, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let invite = Invite {
            id: Uuid::new_v4(),
            site_name: new.site_name,
            region: new.region,
            grid_network_ref: new.grid_network_ref,
            issued_by: new.issued_by,
            created_at: OffsetDateTime::now_utc(),
            expires_at: new.expires_at,
            redeemed_at: None,
            redeemed_by: None,
        };
        inner.invites.insert(new.token_sha256, invite.clone());
        drop(inner);
        Ok(invite)
    }

    /// Look an invite up by token digest.
    fn find_invite(&self, token_sha256: &str) -> Result<Invite, StoreError> {
        let inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        inner.invites.get(token_sha256).cloned().ok_or(StoreError::InviteNotFound)
    }

    /// Redeem an invite, one-shot and guarded on expiry.
    ///
    /// Holding the lock across the check and the mark is what makes it one-shot:
    /// a second redemption sees `redeemed_at` already set.
    fn redeem_invite(&self, token_sha256: &str, redeemed_by: Uuid) -> Result<Invite, StoreError> {
        let mut inner = self.inner.lock().map_err(|_poisoned| poisoned())?;
        let invite = inner.invites.get_mut(token_sha256).ok_or(StoreError::InviteNotFound)?;
        if invite.redeemed_at.is_some() || invite.expires_at <= OffsetDateTime::now_utc() {
            return Err(StoreError::InviteUnavailable);
        }
        invite.redeemed_at = Some(OffsetDateTime::now_utc());
        invite.redeemed_by = Some(redeemed_by);
        let redeemed = invite.clone();
        drop(inner);
        Ok(redeemed)
    }
}

/// A poisoned lock means another thread panicked holding it.
fn poisoned() -> StoreError {
    StoreError::Backend("in-memory store lock was poisoned".to_owned())
}
