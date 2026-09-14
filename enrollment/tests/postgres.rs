//! The Postgres backend, against a real database.
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

use enrollment::{
    model::{Capabilities, Egress, EnrollmentPhase, InferenceCapability, ModelCapability},
    store::{Issued, NewRequest, Store, StoreError},
};

/// A store against the test database, or `None` when none is configured.
async fn store() -> Option<Store> {
    let url = std::env::var("ENROLLMENT_TEST_DATABASE_URL").ok()?;
    let store = Store::postgres(&url).await.expect("connect to the test database");
    Some(store)
}

/// Give each test its own site names, so they can share one database.
fn unique(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple().to_string();
    format!("{prefix}-{}", id.get(..8).unwrap_or("x"))
}

fn request_for(site: &str) -> NewRequest {
    NewRequest {
        site_name: site.to_owned(),
        grid_network_ref: "demo-grid".to_owned(),
        csr_pem: "-----BEGIN CERTIFICATE REQUEST-----\nstub\n-----END CERTIFICATE REQUEST-----".to_owned(),
        public_key_sha256: "a".repeat(64),
        egress: Some(Egress {
            address: format!("{site}.example:8443"),
            server_name: None,
        }),
        capabilities: Some(Capabilities {
            inference: Some(InferenceCapability {
                models: vec![ModelCapability {
                    name: "Qwen/Qwen3-0.6B".to_owned(),
                    path: "/v1/chat/completions".to_owned(),
                    api_format: "openai-chat".to_owned(),
                }],
            }),
        }),
    }
}

fn issued_for(site: &str) -> Issued {
    Issued {
        certificate: format!("-----BEGIN CERTIFICATE-----\n{site}\n-----END CERTIFICATE-----"),
        spiffe_id: format!("spiffe://grid.internal/site/{site}"),
        decided_by: "sam".to_owned(),
    }
}

#[tokio::test]
async fn a_request_survives_being_written_and_read_back() {
    let Some(store) = store().await else { return };
    let site = unique("site");

    let created = store.create(request_for(&site)).await.expect("create");
    assert_eq!(created.phase, EnrollmentPhase::Pending, "a new request waits");

    let read = store.get(created.request_id).await.expect("get");
    assert_eq!(read.public.site_name, site, "the name round trips");
    assert_eq!(
        read.public.egress.as_ref().map(|egress| egress.address.clone()),
        Some(format!("{site}.example:8443")),
        "the egress round trips through JSONB"
    );
    assert_eq!(
        read.public
            .capabilities
            .as_ref()
            .and_then(|caps| caps.inference.as_ref())
            .map(|inf| inf.models.len()),
        Some(1),
        "advertised models round trip through JSONB"
    );
    assert!(!read.csr_pem.is_empty(), "the request is kept so approval can sign it");
}

/// The guarantee that matters once two replicas share one database.
#[tokio::test]
async fn only_one_of_two_racing_approvals_issues() {
    let Some(store) = store().await else { return };
    let site = unique("race");
    let created = store.create(request_for(&site)).await.expect("create");

    let first = store.mark_issued(created.request_id, issued_for(&site));
    let second = store.mark_issued(created.request_id, issued_for(&site));
    let (first, second) = tokio::join!(first, second);

    let issued = [&first, &second].iter().filter(|result| result.is_ok()).count();
    assert_eq!(issued, 1, "exactly one approval may issue: {first:?} {second:?}");

    let loser = if first.is_err() { first } else { second };
    assert!(
        matches!(loser, Err(StoreError::AlreadyDecided)),
        "the losing approval must report the request as already decided, got {loser:?}"
    );
}

/// The partial unique index, not the process, is what guarantees this.
#[tokio::test]
async fn two_members_cannot_hold_one_name() {
    let Some(store) = store().await else { return };
    let site = unique("dup");

    let first = store.create(request_for(&site)).await.expect("create");
    store
        .mark_issued(first.request_id, issued_for(&site))
        .await
        .expect("issue");

    let again = store.create(request_for(&site)).await;
    assert!(
        matches!(again, Err(StoreError::NameTaken)),
        "a name an issued member holds must be refused, got {again:?}"
    );
}

#[tokio::test]
async fn a_denied_request_cannot_then_be_approved() {
    let Some(store) = store().await else { return };
    let site = unique("denied");
    let created = store.create(request_for(&site)).await.expect("create");

    let denied = store
        .mark_denied(created.request_id, "sam".to_owned(), Some("no".to_owned()))
        .await
        .expect("deny");
    assert_eq!(denied.phase, EnrollmentPhase::Denied);
    assert_eq!(denied.reason.as_deref(), Some("no"));
    assert!(denied.decided_at.is_some(), "a decision records when it was made");

    let approved = store.mark_issued(created.request_id, issued_for(&site)).await;
    assert!(
        matches!(approved, Err(StoreError::AlreadyDecided)),
        "a denied request must not then issue, got {approved:?}"
    );
}

#[tokio::test]
async fn an_unknown_request_is_told_apart_from_a_decided_one() {
    let Some(store) = store().await else { return };
    let missing = uuid::Uuid::new_v4();

    assert!(
        matches!(store.get(missing).await, Err(StoreError::NotFound)),
        "an unknown identifier is not found"
    );
    assert!(
        matches!(
            store.mark_issued(missing, issued_for("nobody")).await,
            Err(StoreError::NotFound)
        ),
        "approving something that does not exist is not found, not already decided"
    );
}

#[tokio::test]
async fn listing_is_newest_first() {
    let Some(store) = store().await else { return };
    let names: Vec<String> = std::iter::repeat_with(|| unique("order")).take(3).collect();
    for name in &names {
        store.create(request_for(name)).await.expect("create");
    }

    let listed = store.list().await.expect("list");
    let mine: Vec<&str> = listed
        .iter()
        .map(|row| row.site_name.as_str())
        .filter(|name| names.iter().any(|mine| mine == name))
        .collect();

    let expected: Vec<&str> = names.iter().rev().map(String::as_str).collect();
    assert_eq!(mine, expected, "newest first");
}
