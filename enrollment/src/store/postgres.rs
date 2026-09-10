//! Requests kept in Postgres.
//!
//! A MaaS deployment already runs one, so this points at that. The guarantees
//! the in-process backend enforces by holding a lock are enforced here by the
//! schema, because two services sharing a database cannot hold each other's
//! locks: the name is a partial unique index, and approval is an update guarded
//! on the row still being pending.

use sqlx::{PgPool, Row as _, postgres::PgRow};
use time::OffsetDateTime;
use uuid::Uuid;

use super::{Invite, Issued, NewInvite, NewRequest, StoreError, StoredRequest};
use crate::model::{EnrollmentPhase, EnrollmentRequest};

/// The request schema this backend expects.
static SCHEMA: &str = include_str!("../../db/schema/0001_create_enrollment_requests.up.sql");
/// The invite schema, applied alongside it so submit can be gated on a token.
static SCHEMA_INVITES: &str = include_str!("../../db/schema/0002_create_enrollment_invites.up.sql");

/// Requests held in Postgres.
#[derive(Debug, Clone)]
pub struct PgStore {
    /// Connection pool.
    pool: PgPool,
}

impl PgStore {
    /// Connect and make sure the schema is present.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Backend`] if the database is unreachable or the
    /// schema cannot be applied.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPool::connect(url).await.map_err(backend)?;
        sqlx::raw_sql(SCHEMA).execute(&pool).await.map_err(backend)?;
        sqlx::raw_sql(SCHEMA_INVITES).execute(&pool).await.map_err(backend)?;
        Ok(Self { pool })
    }

    /// Record a new request.
    pub(super) async fn create(&self, new: NewRequest) -> Result<EnrollmentRequest, StoreError> {
        if self.name_is_taken(&new.site_name).await? {
            return Err(StoreError::NameTaken);
        }

        let row = sqlx::query(
            "INSERT INTO enrollment_requests
                 (id, site_name, grid_network_ref, region, csr_pem, public_key_sha256, phase, egress, capabilities)
             VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8)
             RETURNING *",
        )
        .bind(Uuid::new_v4())
        .bind(&new.site_name)
        .bind(&new.grid_network_ref)
        .bind(new.region.as_deref())
        .bind(&new.csr_pem)
        .bind(&new.public_key_sha256)
        .bind(to_json(new.egress.as_ref())?)
        .bind(to_json(new.capabilities.as_ref())?)
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;

        public_from(&row)
    }

    /// Whether an issued member already holds this name.
    ///
    /// The partial unique index is what actually guarantees it, at approval.
    /// Checking on submission means a provider learns straight away rather than
    /// waiting for an operator to find out on its behalf.
    async fn name_is_taken(&self, site_name: &str) -> Result<bool, StoreError> {
        let row = sqlx::query("SELECT 1 AS taken FROM enrollment_requests WHERE site_name = $1 AND phase = 'issued'")
            .bind(site_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?;
        Ok(row.is_some())
    }

    /// Every request, newest first.
    pub(super) async fn list(&self) -> Result<Vec<EnrollmentRequest>, StoreError> {
        let rows = sqlx::query("SELECT * FROM enrollment_requests ORDER BY created_at DESC")
            .fetch_all(&self.pool)
            .await
            .map_err(backend)?;
        rows.iter().map(public_from).collect()
    }

    /// One request by identifier.
    pub(super) async fn get(&self, request_id: Uuid) -> Result<StoredRequest, StoreError> {
        let row = sqlx::query("SELECT * FROM enrollment_requests WHERE id = $1")
            .bind(request_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?
            .ok_or(StoreError::NotFound)?;

        Ok(StoredRequest {
            public: public_from(&row)?,
            csr_pem: row.try_get("csr_pem").map_err(backend)?,
        })
    }

    /// Delete one request, whatever phase it is in.
    pub(super) async fn delete(&self, request_id: Uuid) -> Result<(), StoreError> {
        let result = sqlx::query("DELETE FROM enrollment_requests WHERE id = $1")
            .bind(request_id)
            .execute(&self.pool)
            .await
            .map_err(backend)?;
        if result.rows_affected() == 0 {
            return Err(StoreError::NotFound);
        }
        Ok(())
    }

    /// Move a pending request to issued.
    ///
    /// Guarded on `phase = 'pending'`, so two approvals racing from separate
    /// processes cannot both mint a certificate: the second updates no rows.
    pub(super) async fn mark_issued(&self, request_id: Uuid, issued: Issued) -> Result<EnrollmentRequest, StoreError> {
        let updated = sqlx::query(
            "UPDATE enrollment_requests
                SET phase = 'issued', certificate = $2, spiffe_id = $3,
                    decided_by = $4, decided_at = NOW()
              WHERE id = $1 AND phase = 'pending'
              RETURNING *",
        )
        .bind(request_id)
        .bind(&issued.certificate)
        .bind(&issued.spiffe_id)
        .bind(&issued.decided_by)
        .fetch_optional(&self.pool)
        .await;

        match updated {
            Ok(Some(row)) => public_from(&row),
            Ok(None) => Err(self.why_not_updated(request_id).await),
            // The partial unique index refusing a second holder of one name.
            Err(err) if is_unique_violation(&err) => Err(StoreError::NameTaken),
            Err(err) => Err(backend(err)),
        }
    }

    /// Move a pending request to denied.
    pub(super) async fn mark_denied(
        &self,
        request_id: Uuid,
        decided_by: String,
        reason: Option<String>,
    ) -> Result<EnrollmentRequest, StoreError> {
        let updated = sqlx::query(
            "UPDATE enrollment_requests
                SET phase = 'denied', reason = $3, decided_by = $2, decided_at = NOW()
              WHERE id = $1 AND phase = 'pending'
              RETURNING *",
        )
        .bind(request_id)
        .bind(&decided_by)
        .bind(reason.as_deref())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;

        match updated {
            Some(row) => public_from(&row),
            None => Err(self.why_not_updated(request_id).await),
        }
    }

    /// Tell a missing request apart from one already decided.
    ///
    /// A guarded update that changes nothing does not say which happened, and
    /// the caller owes the operator a different answer for each.
    async fn why_not_updated(&self, request_id: Uuid) -> StoreError {
        match sqlx::query("SELECT 1 AS present FROM enrollment_requests WHERE id = $1")
            .bind(request_id)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(_present)) => StoreError::AlreadyDecided,
            Ok(None) => StoreError::NotFound,
            Err(err) => backend(err),
        }
    }

    /// Record a new invite.
    pub(super) async fn create_invite(&self, new: NewInvite) -> Result<Invite, StoreError> {
        let row = sqlx::query(
            "INSERT INTO enrollment_invites
                 (id, token_sha256, site_name, region, grid_network_ref, issued_by, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             RETURNING *",
        )
        .bind(Uuid::new_v4())
        .bind(&new.token_sha256)
        .bind(new.site_name.as_deref())
        .bind(new.region.as_deref())
        .bind(&new.grid_network_ref)
        .bind(&new.issued_by)
        .bind(new.expires_at)
        .fetch_one(&self.pool)
        .await
        .map_err(backend)?;

        invite_from(&row)
    }

    /// Look an invite up by token digest.
    pub(super) async fn find_invite(&self, token_sha256: &str) -> Result<Invite, StoreError> {
        let row = sqlx::query("SELECT * FROM enrollment_invites WHERE token_sha256 = $1")
            .bind(token_sha256)
            .fetch_optional(&self.pool)
            .await
            .map_err(backend)?
            .ok_or(StoreError::InviteNotFound)?;
        invite_from(&row)
    }

    /// Redeem an invite, one-shot and guarded on expiry.
    ///
    /// Guarded on `redeemed_at IS NULL AND expires_at > NOW()`, so two submits
    /// racing with one token cannot both bootstrap: the second updates no rows.
    pub(super) async fn redeem_invite(&self, token_sha256: &str, redeemed_by: Uuid) -> Result<Invite, StoreError> {
        let updated = sqlx::query(
            "UPDATE enrollment_invites
                SET redeemed_at = NOW(), redeemed_by = $2
              WHERE token_sha256 = $1 AND redeemed_at IS NULL AND expires_at > NOW()
              RETURNING *",
        )
        .bind(token_sha256)
        .bind(redeemed_by)
        .fetch_optional(&self.pool)
        .await
        .map_err(backend)?;

        match updated {
            Some(row) => invite_from(&row),
            None => Err(self.why_not_redeemed(token_sha256).await),
        }
    }

    /// Tell an unknown token apart from a spent or expired one.
    ///
    /// A guarded update that changes nothing does not say which happened, and the
    /// caller owes the enrollee a different answer for each.
    async fn why_not_redeemed(&self, token_sha256: &str) -> StoreError {
        match sqlx::query("SELECT 1 AS present FROM enrollment_invites WHERE token_sha256 = $1")
            .bind(token_sha256)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(Some(_present)) => StoreError::InviteUnavailable,
            Ok(None) => StoreError::InviteNotFound,
            Err(err) => backend(err),
        }
    }
}

/// Build the public view of a stored row.
fn public_from(row: &PgRow) -> Result<EnrollmentRequest, StoreError> {
    let phase: String = row.try_get("phase").map_err(backend)?;
    Ok(EnrollmentRequest {
        request_id: row.try_get("id").map_err(backend)?,
        site_name: row.try_get("site_name").map_err(backend)?,
        grid_network_ref: row.try_get("grid_network_ref").map_err(backend)?,
        region: row.try_get("region").map_err(backend)?,
        phase: phase_from(&phase)?,
        created_at: row.try_get::<OffsetDateTime, _>("created_at").map_err(backend)?,
        decided_at: row.try_get("decided_at").map_err(backend)?,
        decided_by: row.try_get("decided_by").map_err(backend)?,
        reason: row.try_get("reason").map_err(backend)?,
        certificate: row.try_get("certificate").map_err(backend)?,
        spiffe_id: row.try_get("spiffe_id").map_err(backend)?,
        public_key_sha256: row.try_get("public_key_sha256").map_err(backend)?,
        egress: from_json(row, "egress")?,
        capabilities: from_json(row, "capabilities")?,
    })
}

/// Build an invite from a stored row. The token digest stays in the database.
fn invite_from(row: &PgRow) -> Result<Invite, StoreError> {
    Ok(Invite {
        id: row.try_get("id").map_err(backend)?,
        site_name: row.try_get("site_name").map_err(backend)?,
        region: row.try_get("region").map_err(backend)?,
        grid_network_ref: row.try_get("grid_network_ref").map_err(backend)?,
        issued_by: row.try_get("issued_by").map_err(backend)?,
        created_at: row.try_get::<OffsetDateTime, _>("created_at").map_err(backend)?,
        expires_at: row.try_get::<OffsetDateTime, _>("expires_at").map_err(backend)?,
        redeemed_at: row.try_get("redeemed_at").map_err(backend)?,
        redeemed_by: row.try_get("redeemed_by").map_err(backend)?,
    })
}

/// Read a phase back from the column.
fn phase_from(raw: &str) -> Result<EnrollmentPhase, StoreError> {
    match raw {
        "pending" => Ok(EnrollmentPhase::Pending),
        "issued" => Ok(EnrollmentPhase::Issued),
        "denied" => Ok(EnrollmentPhase::Denied),
        "failed" => Ok(EnrollmentPhase::Failed),
        other => Err(StoreError::Backend(format!("unknown phase {other} in the database"))),
    }
}

/// Encode an optional value as JSON for storage.
fn to_json<T: serde::Serialize>(value: Option<&T>) -> Result<Option<serde_json::Value>, StoreError> {
    value
        .map(|value| serde_json::to_value(value).map_err(|err| StoreError::Backend(err.to_string())))
        .transpose()
}

/// Decode an optional JSON column.
fn from_json<T: serde::de::DeserializeOwned>(row: &PgRow, column: &str) -> Result<Option<T>, StoreError> {
    let raw: Option<serde_json::Value> = row.try_get(column).map_err(backend)?;
    raw.map(|value| serde_json::from_value(value).map_err(|err| StoreError::Backend(err.to_string())))
        .transpose()
}

/// Whether an error is the unique index refusing a duplicate.
fn is_unique_violation(err: &sqlx::Error) -> bool {
    matches!(err.as_database_error().and_then(sqlx::error::DatabaseError::code), Some(code) if code == "23505")
}

/// Wrap any backend failure.
fn backend<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Backend(err.to_string())
}
