//! The Postgres backend and the full mint-to-issue lifecycle, against a real
//! database.
//!
//! Skipped unless `ENROLLMENT_TEST_DATABASE_URL` points at one, so the suite
//! stays runnable without a database. Start one with:
//!
//! ```text
//! podman run -d --name grid-enroll-pg -e POSTGRES_PASSWORD=test \
//!   -e POSTGRES_DB=enrollment -p 55432:5432 docker.io/library/postgres:16-alpine
//! export ENROLLMENT_TEST_DATABASE_URL=postgres://postgres:test@127.0.0.1:55432/enrollment
//! ```

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(clippy::expect_used, reason = "tests")]

use std::sync::{Arc, LazyLock};

use enrollment::{Issued, NewSiteToken, Pin, Store, StoreError};
use time::{Duration, OffsetDateTime};
use tokio::sync::{Mutex, MutexGuard};

/// Serializes these tests against the one shared database. They exercise a single
/// service's lifecycle, not many services racing one database, so running them
/// concurrently only produces cross-test lock contention.
static DB_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

/// A store against the test database and the exclusive lock over it, or `None`
/// when no database is configured.
async fn store() -> Option<(Store, MutexGuard<'static, ()>)> {
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").ok()?;
    let guard = DB_LOCK.lock().await;
    let store = Store::postgres(&url).await.expect("connect to the test database");
    Some((store, guard))
}

/// Give each test its own names and tokens, so they can share one database.
fn unique(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", id.get(..8).unwrap_or("x"))
}

/// A token pinning `site`, keyed by a digest unique to this test. The store
/// compares digests as opaque strings, so the API-layer hashing is not needed
/// here.
fn new_token(site: &str, digest: &str) -> NewSiteToken {
    NewSiteToken {
        token_sha256: digest.to_owned(),
        site_name: site.to_owned(),
        grid_network_ref: "demo-grid".to_owned(),
        issued_by: "sam".to_owned(),
        expires_at: OffsetDateTime::now_utc().saturating_add(Duration::hours(1)),
    }
}

/// A signer that issues a stub certificate for the pinned name, standing in for
/// the certs primitive the handler uses.
#[expect(clippy::unnecessary_wraps, reason = "matches the redeem_and_issue signer signature")]
fn sign_for(pin: &Pin) -> Result<Issued, StoreError> {
    Ok(Issued {
        certificate: format!(
            "-----BEGIN CERTIFICATE-----\n{}\n-----END CERTIFICATE-----",
            pin.site_name
        ),
        spiffe_id: format!("spiffe://grid.internal/site/{}", pin.site_name),
        public_key_sha256: "a".repeat(64),
    })
}

/// The full lifecycle: a grid-admin mints a token, a site redeems it under the
/// pinned name, and the certificate is signed and returned.
#[tokio::test]
async fn the_full_lifecycle_runs_mint_to_issued() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("site");
    let digest = unique("digest");

    store.mint_site_token(new_token(&site, &digest)).await.expect("mint");
    assert!(
        store.token_valid(&digest).await.expect("peek"),
        "a fresh token is usable"
    );

    let (enrollment_id, issued) = store
        .redeem_and_issue(&digest, sign_for)
        .await
        .expect("redeem and issue");
    assert!(!enrollment_id.is_nil(), "redeem returns the issued-enrollment row id");
    assert_eq!(
        issued.spiffe_id,
        format!("spiffe://grid.internal/site/{site}"),
        "the certificate is signed under the pinned name"
    );

    assert!(
        !store.token_valid(&digest).await.expect("peek"),
        "the token is spent once redeemed"
    );
}

#[tokio::test]
async fn a_token_redeems_at_most_once() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("once"), &digest))
        .await
        .expect("mint");

    store.redeem_and_issue(&digest, sign_for).await.expect("first redeem");
    let second = store.redeem_and_issue(&digest, sign_for).await;
    assert!(
        matches!(second, Err(StoreError::TokenInvalid)),
        "a token cannot enroll a second site, got {second:?}"
    );
}

#[tokio::test]
async fn an_unknown_or_expired_token_is_invalid() {
    let Some((store, _guard)) = store().await else {
        return;
    };

    let unknown = store.redeem_and_issue(&unique("nope"), sign_for).await;
    assert!(
        matches!(unknown, Err(StoreError::TokenInvalid)),
        "a token nobody minted is invalid, got {unknown:?}"
    );

    let digest = unique("digest");
    let mut expired = new_token(&unique("stale"), &digest);
    expired.expires_at = OffsetDateTime::now_utc().saturating_sub(Duration::hours(1));
    store.mint_site_token(expired).await.expect("mint expired token");
    assert!(
        !store.token_valid(&digest).await.expect("peek"),
        "an expired token is unusable"
    );
    let redeemed = store.redeem_and_issue(&digest, sign_for).await;
    assert!(
        matches!(redeemed, Err(StoreError::TokenInvalid)),
        "an expired token cannot be redeemed, got {redeemed:?}"
    );
}

#[tokio::test]
async fn a_revoked_token_cannot_be_redeemed() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    let token_id = store
        .mint_site_token(new_token(&unique("revoked"), &digest))
        .await
        .expect("mint");

    store.revoke_site_token(token_id).await.expect("revoke");
    let redeemed = store.redeem_and_issue(&digest, sign_for).await;
    assert!(
        matches!(redeemed, Err(StoreError::TokenInvalid)),
        "a revoked token is unusable, got {redeemed:?}"
    );
}

/// Revoke is a pre-redemption kill switch: a redeemed token's row stays as
/// issuance provenance, so revoking it reports not found rather than deleting.
#[tokio::test]
async fn a_redeemed_token_cannot_be_revoked() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    let token_id = store
        .mint_site_token(new_token(&unique("redeemed"), &digest))
        .await
        .expect("mint");
    store.redeem_and_issue(&digest, sign_for).await.expect("redeem");

    let revoked = store.revoke_site_token(token_id).await;
    assert!(
        matches!(revoked, Err(StoreError::NotFound)),
        "a redeemed token is no longer revocable, got {revoked:?}"
    );
}

/// The partial unique index, not the process, is what guarantees this.
#[tokio::test]
async fn two_sites_cannot_hold_one_name() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("dup");

    let first = unique("digest");
    store
        .mint_site_token(new_token(&site, &first))
        .await
        .expect("mint first");
    store.redeem_and_issue(&first, sign_for).await.expect("first enroll");

    let second = unique("digest");
    store
        .mint_site_token(new_token(&site, &second))
        .await
        .expect("mint second");
    let again = store.redeem_and_issue(&second, sign_for).await;
    assert!(
        matches!(again, Err(StoreError::NameTaken)),
        "a name an issued member holds must be refused, got {again:?}"
    );
    assert!(
        store.token_valid(&second).await.expect("peek"),
        "the refused redeem rolls back, so the second token is not spent"
    );
}

/// Redeem `digest` on its own task, so two can race the same store.
fn spawn_redeem(
    store: &Arc<Store>,
    digest: String,
) -> tokio::task::JoinHandle<Result<(uuid::Uuid, Issued), StoreError>> {
    let store = Arc::clone(store);
    tokio::spawn(async move { store.redeem_and_issue(&digest, sign_for).await })
}

/// Two racers redeeming one token: the guarded UPDATE, not the process, makes it
/// single-use, so exactly one wins and the other is refused as invalid.
#[tokio::test]
async fn concurrent_redeems_spend_a_token_once() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("race"), &digest))
        .await
        .expect("mint");

    let store = Arc::new(store);
    let first = spawn_redeem(&store, digest.clone());
    let second = spawn_redeem(&store, digest);
    let first = first.await.expect("join first");
    let second = second.await.expect("join second");

    let succeeded = usize::from(first.is_ok()) + usize::from(second.is_ok());
    let invalid = usize::from(matches!(first, Err(StoreError::TokenInvalid)))
        + usize::from(matches!(second, Err(StoreError::TokenInvalid)));
    assert_eq!(succeeded, 1, "exactly one redeem wins, got {first:?} and {second:?}");
    assert_eq!(
        invalid, 1,
        "the loser is refused as invalid, got {first:?} and {second:?}"
    );
}

/// Two racers redeeming different tokens that pin the same name: the unique index,
/// not the process, holds the name, so exactly one wins and the other is refused.
#[tokio::test]
async fn concurrent_redeems_of_one_name_leave_one_holder() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let site = unique("dupe");
    let first_digest = unique("digest");
    let second_digest = unique("digest");
    store
        .mint_site_token(new_token(&site, &first_digest))
        .await
        .expect("mint first");
    store
        .mint_site_token(new_token(&site, &second_digest))
        .await
        .expect("mint second");

    let store = Arc::new(store);
    let first = spawn_redeem(&store, first_digest);
    let second = spawn_redeem(&store, second_digest);
    let first = first.await.expect("join first");
    let second = second.await.expect("join second");

    let succeeded = usize::from(first.is_ok()) + usize::from(second.is_ok());
    let name_taken = usize::from(matches!(first, Err(StoreError::NameTaken)))
        + usize::from(matches!(second, Err(StoreError::NameTaken)));
    assert_eq!(succeeded, 1, "exactly one holder wins, got {first:?} and {second:?}");
    assert_eq!(
        name_taken, 1,
        "the loser is refused as name taken, got {first:?} and {second:?}"
    );
}

/// The invariant: the token is spent only when the certificate is issued.
#[tokio::test]
async fn a_signing_failure_leaves_the_token_unspent() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let digest = unique("digest");
    store
        .mint_site_token(new_token(&unique("unsigned"), &digest))
        .await
        .expect("mint");

    let failed = store
        .redeem_and_issue(&digest, |_pin| Err(StoreError::Backend("signing fault".to_owned())))
        .await;
    assert!(
        matches!(failed, Err(StoreError::Backend(_))),
        "a signing failure surfaces, got {failed:?}"
    );
    assert!(
        store.token_valid(&digest).await.expect("peek"),
        "a signing failure rolls back, so the token stays unspent"
    );
}

/// A renewal of `site` presenting `presented` and asking for `requested`.
fn renewal(site: &str, presented: &str, requested: &str, reserved: bool) -> enrollment::Renewal {
    enrollment::Renewal {
        site_name: site.to_owned(),
        presented_key: presented.to_owned(),
        requested_key: requested.to_owned(),
        presented_not_before: OffsetDateTime::now_utc().saturating_sub(Duration::days(1)),
        reserved,
    }
}

/// A signer standing in for the CA on renewal.
fn resign(site: &str) -> Result<Issued, StoreError> {
    sign_for(&Pin {
        site_name: site.to_owned(),
    })
}

/// Rotate, refuse the replaced key, re-sign a lost response, and rotate again.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "one renewal sequence")]
async fn a_renewal_rotates_the_recorded_key() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let (site, digest) = (unique("site"), unique("digest"));
    let (key_a, key_b, key_c) = ("a".repeat(64), "b".repeat(64), "c".repeat(64));
    store.mint_site_token(new_token(&site, &digest)).await.expect("mint");
    store.redeem_and_issue(&digest, sign_for).await.expect("redeem");

    let renewed = store
        .renew_and_issue(&renewal(&site, &key_a, &key_b, false), || resign(&site))
        .await
        .expect("the enrolled key renews");
    assert_eq!(renewed.action, enrollment::RenewAction::Rotate);
    assert_eq!(renewed.replaced_key.as_deref(), Some(key_a.as_str()));

    let stale = store
        .renew_and_issue(&renewal(&site, &key_a, &key_c, false), || resign(&site))
        .await;
    assert!(
        matches!(stale, Err(StoreError::Refused(enrollment::Refusal::StaleKey))),
        "the replaced key cannot pick a new key: {stale:?}"
    );
    let retried = store
        .renew_and_issue(&renewal(&site, &key_a, &key_b, false), || resign(&site))
        .await
        .expect("a lost response retries");
    assert_eq!(retried.action, enrollment::RenewAction::Resign);
    assert_eq!(retried.id, renewed.id, "the same record");

    let again = store
        .renew_and_issue(&renewal(&site, &key_b, &key_c, false), || resign(&site))
        .await
        .expect("the current key renews");
    assert_eq!(again.action, enrollment::RenewAction::Rotate);
}

/// A reserved name gets a token-less record; nothing else may.
#[tokio::test]
#[expect(clippy::too_many_lines, reason = "the store path, then the schema check")]
async fn only_a_reserved_name_records_without_a_token() {
    let Some((store, _guard)) = store().await else {
        return;
    };
    let (hub, spoke) = (unique("hub"), unique("spoke"));
    let (x, y) = ("d".repeat(64), "e".repeat(64));

    let refused = store
        .renew_and_issue(&renewal(&spoke, &x, &y, false), || resign(&spoke))
        .await;
    assert!(
        matches!(refused, Err(StoreError::Refused(enrollment::Refusal::UnknownSite))),
        "an unrecorded spoke is refused: {refused:?}"
    );
    let registered = store
        .renew_and_issue(&renewal(&hub, &x, &y, true), || resign(&hub))
        .await
        .expect("a reserved name registers");
    assert_eq!(registered.action, enrollment::RenewAction::Register);
    assert_eq!(registered.replaced_key, None);

    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").expect("url");
    let pool = Box::pin(sqlx::PgPool::connect(&url)).await.expect("pool");
    let row: (Option<uuid::Uuid>, bool, String, Option<String>) = sqlx::query_as(
        "SELECT site_token_id, reserved, public_key_sha256, previous_public_key_sha256
           FROM site_enrollments WHERE site_name = $1",
    )
    .bind(&hub)
    .fetch_one(&pool)
    .await
    .expect("row");
    assert_eq!(row, (None, true, y, Some(x)), "token-less, reserved, keys recorded");

    let forged = sqlx::query(
        "INSERT INTO site_enrollments (id, site_token_id, site_name, public_key_sha256, spiffe_id)
         VALUES ($1, NULL, $2, $3, $4)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(&spoke)
    .bind("f".repeat(64))
    .bind("spiffe://grid.internal/site/x")
    .execute(&pool)
    .await;
    assert!(forged.is_err(), "the schema refuses a token-less spoke record");
}
