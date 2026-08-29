//! The enrollment flow, end to end over the HTTP interface.

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "tests, and serde_json::Value indexing yields Null rather than panicking"
)]

use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use enrollment::{AppState, Operators, Store, router};
use http_body_util::BodyExt as _;
use rcgen::{CertificateParams, KeyPair, SanType};
use serde_json::{Value, json};
use tower::ServiceExt as _;

/// The operator credential the tests decide with.
const TOKEN: &str = "t0ken";

/// A service with a fresh CA, an empty store, and one operator.
fn service() -> axum::Router {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    router(Arc::new(AppState {
        store: Store::memory(),
        ca,
        operators: Operators::from_table("tester: t0ken\n"),
    }))
}

/// A request the way an enrollee would make one, asking for `requested`.
fn csr_asking_for(requested: &[SanType]) -> String {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::default();
    params.distinguished_name.push(rcgen::DnType::CommonName, "whatever");
    params.subject_alt_names = requested.to_vec();
    params.serialize_request(&key).expect("csr").pem().expect("pem")
}

fn plain_csr() -> String {
    csr_asking_for(&[])
}

async fn call(app: &axum::Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    send(app, method, path, body, None).await
}

/// A call carrying an operator credential.
async fn call_as_operator(app: &axum::Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    send(app, method, path, body, Some(TOKEN)).await
}

async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let request = builder
        .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
        .expect("request");

    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("body").to_bytes();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, value)
}

fn submit(site: &str, csr: &str) -> Value {
    json!({
        "siteName": site,
        "gridNetworkRef": "demo-grid",
        "csr": csr,
        "egress": { "address": "site-d.example:8443" },
        "capabilities": {
            "inference": {
                "models": [{ "name": "Qwen/Qwen3-0.6B", "path": "/v1/chat/completions", "apiFormat": "openai-chat" }]
            }
        }
    })
}

#[tokio::test]
async fn a_provider_enrolls_and_collects_a_certificate() {
    let app = service();

    let (create_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    assert_eq!(create_status, StatusCode::CREATED, "submitting should be accepted");
    assert_eq!(created["phase"], "pending", "a new request waits for a decision");
    assert!(created["certificate"].is_null(), "nothing is issued before approval");
    let id = created["requestId"].as_str().expect("request id").to_owned();

    // The capabilities and egress it advertised come back on the record.
    assert_eq!(created["egress"]["address"], "site-d.example:8443");
    assert_eq!(
        created["capabilities"]["inference"]["models"][0]["name"],
        "Qwen/Qwen3-0.6B"
    );

    let (approve_status, approved) = call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;
    assert_eq!(approve_status, StatusCode::OK, "approval should succeed");
    assert_eq!(approved["phase"], "issued");
    assert_eq!(approved["spiffeId"], "spiffe://grid.internal/site/site-d");
    assert!(
        approved["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "approval issues a certificate"
    );
    assert!(approved["decidedBy"].is_string(), "who decided is recorded");

    // Collecting it again returns the same certificate.
    let (fetch_status, fetched) = call(&app, "GET", &format!("/v1/requests/{id}"), None).await;
    assert_eq!(fetch_status, StatusCode::OK, "collecting should succeed");
    assert_eq!(fetched["certificate"], approved["certificate"], "the record is durable");
}

#[tokio::test]
async fn the_certificate_never_carries_a_name_the_request_asked_for() {
    let app = service();
    let csr = csr_asking_for(&[SanType::URI(
        "spiffe://grid.internal/site/site-a".to_owned().try_into().expect("ia5"),
    )]);

    let (_create_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &csr))).await;
    let id = created["requestId"].as_str().expect("id").to_owned();
    let (_approve_status, approved) = call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;

    assert_eq!(
        approved["spiffeId"], "spiffe://grid.internal/site/site-d",
        "a request asking to be site-a must not be granted it"
    );
}

/// Approval mints a certificate, so a retry must not mint a second one.
#[tokio::test]
async fn approving_twice_issues_one_certificate() {
    let app = service();
    let (_create_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = created["requestId"].as_str().expect("id").to_owned();

    let (first_status, first) = call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;
    assert_eq!(first_status, StatusCode::OK, "the first approval issues");

    let (second_status, second) = call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;
    assert_eq!(second_status, StatusCode::OK, "a retried approval is not an error");
    assert_eq!(
        second["certificate"], first["certificate"],
        "a retry must return the certificate already issued, not a new one"
    );
    assert_eq!(
        second["decidedAt"], first["decidedAt"],
        "the decision is not re-recorded"
    );
}

/// Deciding is not self-service.
#[tokio::test]
async fn deciding_requires_an_operator_credential() {
    let app = service();
    let (_create_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = created["requestId"].as_str().expect("id").to_owned();

    let (approve_status, approve_body) = call(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;
    assert_eq!(
        approve_status,
        StatusCode::UNAUTHORIZED,
        "approving without a credential must be refused"
    );
    assert_eq!(approve_body["error"], "unauthorized");

    let (deny_status, _deny_body) = call(&app, "POST", &format!("/v1/requests/{id}/deny"), None).await;
    assert_eq!(deny_status, StatusCode::UNAUTHORIZED, "denying must be refused too");

    let (list_status, _list_body) = call(&app, "GET", "/v1/requests", None).await;
    assert_eq!(
        list_status,
        StatusCode::UNAUTHORIZED,
        "who has asked to join is not public"
    );

    // The request is untouched by the refused attempts.
    let (_fetch_status, fetched) = call(&app, "GET", &format!("/v1/requests/{id}"), None).await;
    assert_eq!(fetched["phase"], "pending", "a refused decision changes nothing");
    assert!(fetched["certificate"].is_null(), "nothing was issued");
}

#[tokio::test]
async fn an_unknown_token_decides_nothing() {
    let app = service();
    let (_create_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = created["requestId"].as_str().expect("id").to_owned();

    let request = Request::builder()
        .method("POST")
        .uri(format!("/v1/requests/{id}/approve"))
        .header("authorization", "Bearer not-the-token")
        .body(Body::empty())
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "an unknown token must not approve"
    );
}

/// An enrollee polls by identifier without a credential, since it has none yet.
#[tokio::test]
async fn an_enrollee_collects_its_certificate_without_a_credential() {
    let app = service();
    let (_create_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = created["requestId"].as_str().expect("id").to_owned();
    call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;

    let (fetch_status, fetched) = call(&app, "GET", &format!("/v1/requests/{id}"), None).await;
    assert_eq!(fetch_status, StatusCode::OK, "collecting needs no credential");
    assert!(
        fetched["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the certificate is there to collect"
    );
}

/// Listing can be narrowed to what still needs a decision.
#[tokio::test]
async fn listing_can_be_filtered_by_phase() {
    let app = service();
    let (_first_status, first) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = first["requestId"].as_str().expect("id").to_owned();
    call(&app, "POST", "/v1/requests", Some(submit("site-e", &plain_csr()))).await;
    call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;

    let (_pending_status, pending) = call_as_operator(&app, "GET", "/v1/requests?phase=pending", None).await;
    let pending_names: Vec<&str> = pending
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|row| row["siteName"].as_str())
        .collect();
    assert_eq!(pending_names, vec!["site-e"], "only site-e is still pending");

    let (_issued_status, issued) = call_as_operator(&app, "GET", "/v1/requests?phase=issued", None).await;
    assert_eq!(
        issued.as_array().map(Vec::len),
        Some(1),
        "site-d is the only issued member"
    );
}

#[tokio::test]
async fn two_providers_cannot_hold_the_same_name() {
    let app = service();
    let (_first_status, first) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = first["requestId"].as_str().expect("id").to_owned();
    call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;

    let (second_status, second_body) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    assert_eq!(second_status, StatusCode::CONFLICT, "the name is already held");
    assert_eq!(second_body["error"], "name_taken");
}

#[tokio::test]
async fn a_denied_request_issues_nothing() {
    let app = service();
    let (_status, created) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    let id = created["requestId"].as_str().expect("id").to_owned();

    let (deny_status, denied) = call_as_operator(
        &app,
        "POST",
        &format!("/v1/requests/{id}/deny"),
        Some(json!({"reason": "not a known operator"})),
    )
    .await;
    assert_eq!(deny_status, StatusCode::OK, "denial should succeed");
    assert_eq!(denied["phase"], "denied");
    assert_eq!(denied["reason"], "not a known operator");
    assert!(denied["certificate"].is_null(), "denial issues nothing");

    let (approve_status, approve_body) =
        call_as_operator(&app, "POST", &format!("/v1/requests/{id}/approve"), None).await;
    assert_eq!(
        approve_status,
        StatusCode::CONFLICT,
        "a denied request cannot then be approved"
    );
    assert_eq!(approve_body["error"], "already_decided");
}

/// An unusable submission is refused at the door, so an operator is never shown
/// something that cannot be signed.
#[tokio::test]
async fn an_unusable_request_is_refused_on_submission() {
    let app = service();

    let (malformed_status, malformed_body) =
        call(&app, "POST", "/v1/requests", Some(submit("site-d", "not a csr"))).await;
    assert_eq!(
        malformed_status,
        StatusCode::BAD_REQUEST,
        "a malformed request is refused"
    );
    assert_eq!(malformed_body["error"], "invalid_csr");

    let (bad_name_status, bad_name_body) =
        call(&app, "POST", "/v1/requests", Some(submit("Site-D", &plain_csr()))).await;
    assert_eq!(
        bad_name_status,
        StatusCode::BAD_REQUEST,
        "a name outside the grammar is refused"
    );
    assert_eq!(bad_name_body["error"], "invalid_csr");

    let (list_status, listed) = call_as_operator(&app, "GET", "/v1/requests", None).await;
    assert_eq!(list_status, StatusCode::OK, "listing should succeed");
    assert_eq!(
        listed.as_array().map(Vec::len),
        Some(0),
        "refused submissions are not stored"
    );
}

#[tokio::test]
async fn an_unknown_request_is_not_found() {
    let app = service();
    let missing = uuid::Uuid::new_v4();
    let (status, body) = call(&app, "GET", &format!("/v1/requests/{missing}"), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "not_found");
}

#[tokio::test]
async fn requests_are_listed_newest_first() {
    let app = service();
    for site in ["site-d", "site-e", "site-f"] {
        call(&app, "POST", "/v1/requests", Some(submit(site, &plain_csr()))).await;
    }

    let (_list_status, listed) = call_as_operator(&app, "GET", "/v1/requests", None).await;
    let names: Vec<&str> = listed
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|row| row["siteName"].as_str())
        .collect();
    assert_eq!(names, vec!["site-f", "site-e", "site-d"], "newest first");
}
