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
use enrollment::{AppState, JoiningConfig, Operators, Store, authz::Authorizer, router};
use http_body_util::BodyExt as _;
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
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
        authorizer: Authorizer::Local(Operators::from_table("tester: t0ken\n")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        joining: JoiningConfig {
            gossip_key: Some("dGVzdC1nb3NzaXAta2V5LTMyLWJ5dGVzLWxvbmchIQ==".to_owned()),
            seeds: vec!["site-a.grid.internal:7946".to_owned()],
        },
    }))
}

/// A request the way an enrollee would make one, asking for `requested`.
fn csr_asking_for(requested: &[SanType]) -> String {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, "whatever");
    params.subject_alt_names = requested.to_vec();
    params.serialize_request(&key).expect("csr").pem().expect("pem")
}

fn plain_csr() -> String {
    csr_asking_for(&[])
}

async fn call(app: &axum::Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    send(app, method, path, body, &[]).await
}

/// A call carrying an operator credential.
async fn call_as_operator(app: &axum::Router, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
    let bearer = format!("Bearer {TOKEN}");
    send(app, method, path, body, &[("authorization", &bearer)]).await
}

/// Mint a site token for `site`, pinned to region `us`.
async fn invite_for(app: &axum::Router, site: &str) -> String {
    let (status, body) = call_as_operator(
        app,
        "POST",
        "/v1/invites",
        Some(json!({ "siteName": site, "region": "us", "gridNetworkRef": "demo-grid" })),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "issuing a site token should succeed");
    body["token"].as_str().expect("token").to_owned()
}

/// Submit for `site` carrying a freshly issued invite, the way a site now must.
async fn submit_invited(app: &axum::Router, site: &str, csr: &str) -> (StatusCode, Value) {
    let token = invite_for(app, site).await;
    submit_with_token(app, site, csr, &token).await
}

/// Submit for `site` presenting `token` in the invite header.
async fn submit_with_token(app: &axum::Router, site: &str, csr: &str, token: &str) -> (StatusCode, Value) {
    send(
        app,
        "POST",
        "/v1/requests",
        Some(submit(site, csr)),
        &[("x-grid-invite", token)],
    )
    .await
}

async fn send(
    app: &axum::Router,
    method: &str,
    path: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
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
async fn a_valid_invite_issues_a_certificate_on_submit() {
    let app = service();

    let (create_status, created) = submit_invited(&app, "site-d", &plain_csr()).await;
    assert_eq!(create_status, StatusCode::CREATED, "submitting should be accepted");
    assert_eq!(
        created["phase"], "issued",
        "a valid invite issues on submit, with no separate approve"
    );
    assert!(
        created["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the certificate is issued at submit"
    );
    assert_eq!(created["spiffeId"], "spiffe://grid.internal/site/site-d");
    let id = created["requestId"].as_str().expect("request id").to_owned();

    // The capabilities and egress it advertised come back on the record.
    assert_eq!(created["egress"]["address"], "site-d.example:8443");
    assert_eq!(
        created["capabilities"]["inference"]["models"][0]["name"],
        "Qwen/Qwen3-0.6B"
    );

    // The region is the operator's pin from the invite, not anything the site sent.
    assert_eq!(created["region"], "us", "the redeemed invite stamps its region");

    // Provenance replaces the human approver: the minting operator and the spent
    // invite are recorded so an auto-issued certificate still traces to a decision.
    assert_eq!(created["decidedBy"], "tester", "the minting operator is recorded");
    assert!(
        created["issuedVia"].is_string(),
        "the invite that authorized the issue is recorded"
    );

    // Collecting it again returns the same certificate.
    let (fetch_status, fetched) = call(&app, "GET", &format!("/v1/requests/{id}"), None).await;
    assert_eq!(fetch_status, StatusCode::OK, "collecting should succeed");
    assert_eq!(fetched["certificate"], created["certificate"], "the record is durable");
}

#[tokio::test]
async fn the_certificate_never_carries_a_name_the_request_asked_for() {
    let app = service();
    let csr = csr_asking_for(&[SanType::URI(
        "spiffe://grid.internal/site/site-a".to_owned().try_into().expect("ia5"),
    )]);

    let (_create_status, created) = submit_invited(&app, "site-d", &csr).await;

    assert_eq!(
        created["spiffeId"], "spiffe://grid.internal/site/site-d",
        "a request asking to be site-a must not be granted it"
    );
}

/// Approval mints a certificate, so a retry must not mint a second one.
#[tokio::test]
async fn approving_twice_issues_one_certificate() {
    let app = service();
    let (_create_status, created) = submit_invited(&app, "site-d", &plain_csr()).await;
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
    let (_create_status, created) = submit_invited(&app, "site-d", &plain_csr()).await;
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

    // The auto-issued request is untouched by the refused decision attempts.
    let (_fetch_status, fetched) = call(&app, "GET", &format!("/v1/requests/{id}"), None).await;
    assert_eq!(fetched["phase"], "issued", "a refused decision changes nothing");
    assert!(
        fetched["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the certificate issued at submit is still there"
    );
}

#[tokio::test]
async fn an_unknown_token_decides_nothing() {
    let app = service();
    let (_create_status, created) = submit_invited(&app, "site-d", &plain_csr()).await;
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
    let (_create_status, created) = submit_invited(&app, "site-d", &plain_csr()).await;
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
    submit_invited(&app, "site-d", &plain_csr()).await;
    submit_invited(&app, "site-e", &plain_csr()).await;

    // Auto-issue leaves nothing pending: both requests are issued on submit.
    let (_pending_status, pending) = call_as_operator(&app, "GET", "/v1/requests?phase=pending", None).await;
    assert_eq!(
        pending.as_array().map(Vec::len),
        Some(0),
        "auto-issue leaves nothing pending"
    );

    let (_issued_status, issued) = call_as_operator(&app, "GET", "/v1/requests?phase=issued", None).await;
    let issued_names: Vec<&str> = issued
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|row| row["siteName"].as_str())
        .collect();
    assert_eq!(issued_names, vec!["site-e", "site-d"], "both are issued, newest first");
}

#[tokio::test]
async fn two_providers_cannot_hold_the_same_name() {
    let app = service();
    submit_invited(&app, "site-d", &plain_csr()).await;

    let (second_status, second_body) = submit_invited(&app, "site-d", &plain_csr()).await;
    assert_eq!(second_status, StatusCode::CONFLICT, "the name is already held");
    assert_eq!(second_body["error"], "name_taken");
}

/// Auto-issue leaves no pending request to deny, so the retained deny path
/// cannot take back a certificate the submit already issued. Revocation on the
/// routing plane is deleting the `GridSite`, not deny.
#[tokio::test]
async fn the_retained_deny_cannot_revoke_an_auto_issued_certificate() {
    let app = service();
    let (_status, created) = submit_invited(&app, "site-d", &plain_csr()).await;
    assert_eq!(created["phase"], "issued", "submit auto-issues");
    let id = created["requestId"].as_str().expect("id").to_owned();

    let (deny_status, deny_body) = call_as_operator(
        &app,
        "POST",
        &format!("/v1/requests/{id}/deny"),
        Some(json!({"reason": "too late"})),
    )
    .await;
    assert_eq!(
        deny_status,
        StatusCode::CONFLICT,
        "an already-issued request cannot be denied"
    );
    assert_eq!(deny_body["error"], "already_decided");

    let (_fetch_status, fetched) = call(&app, "GET", &format!("/v1/requests/{id}"), None).await;
    assert_eq!(fetched["phase"], "issued", "the certificate still stands");
}

/// An unusable submission is refused at the door, so an operator is never shown
/// something that cannot be signed.
#[tokio::test]
async fn an_unusable_request_is_refused_on_submission() {
    let app = service();

    let (malformed_status, malformed_body) = submit_invited(&app, "site-d", "not a csr").await;
    assert_eq!(
        malformed_status,
        StatusCode::BAD_REQUEST,
        "a malformed request is refused"
    );
    assert_eq!(malformed_body["error"], "invalid_csr");

    let (bad_name_status, bad_name_body) = submit_invited(&app, "Site-D", &plain_csr()).await;
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
        submit_invited(&app, site, &plain_csr()).await;
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


/// A certificate alone does not let a provider join: it also has to verify peers
/// and reach the mesh.
#[tokio::test]
async fn an_approved_provider_collects_what_it_needs_to_join() {
    let app = service();
    let (id, key) = enrolled_site(&app, "site-join").await;

    let (status, kit) = call(
        &app,
        "POST",
        &format!("/v1/requests/{id}/join"),
        Some(proof_for(&id, &key)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "an approved provider can collect its kit");
    assert!(
        kit["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the kit carries the certificate"
    );
    assert!(
        kit["caBundle"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the kit carries the CA, without which a member cannot verify anyone else"
    );
    assert!(kit["gossipKey"].is_string(), "the kit carries the gossip key");
    assert_eq!(
        kit["seeds"][0], "site-a.grid.internal:7946",
        "the kit says who to announce to"
    );
}

/// The kit carries a secret, so it is not handed to whoever asks.
#[tokio::test]
async fn the_joining_kit_is_refused_without_proof_of_the_key() {
    let app = service();
    let (id, _key) = enrolled_site(&app, "site-proof").await;

    // Somebody else's key, which is what an interloper would have.
    let other = KeyPair::generate().expect("other key");
    let (wrong_status, _body) = call(
        &app,
        "POST",
        &format!("/v1/requests/{id}/join"),
        Some(proof_for(&id, &other)),
    )
    .await;
    assert_eq!(
        wrong_status,
        StatusCode::UNAUTHORIZED,
        "a signature from another key must not collect the kit"
    );

    let (garbage_status, _garbage_body) = call(
        &app,
        "POST",
        &format!("/v1/requests/{id}/join"),
        Some(json!({"signature": "bm90LWEtc2lnbmF0dXJl"})),
    )
    .await;
    assert_eq!(garbage_status, StatusCode::UNAUTHORIZED, "rubbish must not collect it");
}

/// Auto-issue makes a submitted request joinable at once: no approve step, the
/// joining kit is available as soon as submit returns.
#[tokio::test]
async fn a_submitted_request_is_immediately_joinable() {
    let app = service();
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, "site-early");
    let csr = params.serialize_request(&key).expect("csr").pem().expect("pem");

    let (_status, created) = submit_invited(&app, "site-early", &csr).await;
    let id = created["requestId"].as_str().expect("id").to_owned();

    let (status, kit) = call(
        &app,
        "POST",
        &format!("/v1/requests/{id}/join"),
        Some(proof_for(&id, &key)),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "an auto-issued request is joinable at once");
    assert!(
        kit["certificate"]
            .as_str()
            .is_some_and(|pem| pem.contains("BEGIN CERTIFICATE")),
        "the kit carries the certificate"
    );
}

/// Submit for `site`, which auto-issues, and hand back the request id and the key.
async fn enrolled_site(app: &axum::Router, site: &str) -> (String, KeyPair) {
    let key = KeyPair::generate().expect("key");
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, site);
    let csr = params.serialize_request(&key).expect("csr").pem().expect("pem");

    let (_status, created) = submit_invited(app, site, &csr).await;
    let id = created["requestId"].as_str().expect("id").to_owned();
    (id, key)
}

/// Sign the request identifier the way the provider does.
fn proof_for(request_id: &str, key: &KeyPair) -> Value {
    use base64::Engine as _;

    let id = uuid::Uuid::parse_str(request_id).expect("uuid");
    let der = pem::parse(key.serialize_pem()).expect("key pem").contents().to_vec();
    let signing = ring::signature::EcdsaKeyPair::from_pkcs8(
        &ring::signature::ECDSA_P256_SHA256_ASN1_SIGNING,
        &der,
        &ring::rand::SystemRandom::new(),
    )
    .expect("signing key");
    let signature = signing
        .sign(&ring::rand::SystemRandom::new(), id.as_bytes())
        .expect("sign");

    json!({ "signature": base64::engine::general_purpose::STANDARD.encode(signature.as_ref()) })
}

/// Submit is closed without a site token: no header, or an unknown one, is refused.
#[tokio::test]
async fn submitting_requires_a_valid_invite() {
    let app = service();

    // No invite header at all.
    let (missing_status, missing_body) = call(&app, "POST", "/v1/requests", Some(submit("site-d", &plain_csr()))).await;
    assert_eq!(
        missing_status,
        StatusCode::UNAUTHORIZED,
        "submitting without a site token is refused"
    );
    assert_eq!(missing_body["error"], "invite_required");

    // A token that matches no invite digest.
    let (bad_status, bad_body) = submit_with_token(&app, "site-d", &plain_csr(), "not-a-real-token").await;
    assert_eq!(bad_status, StatusCode::UNAUTHORIZED, "an unknown token is refused");
    assert_eq!(bad_body["error"], "invalid_invite");

    // Neither attempt stored anything.
    let (_list_status, listed) = call_as_operator(&app, "GET", "/v1/requests", None).await;
    assert_eq!(
        listed.as_array().map(Vec::len),
        Some(0),
        "a refused submission is not stored"
    );
}

/// The invite's pin is identity: the body cannot ask for a different name.
#[tokio::test]
async fn the_invite_pins_the_name_the_body_cannot_override() {
    let app = service();
    let token = invite_for(&app, "site-pinned").await;

    // The body asks for a different name; the pin must win.
    let (status, created) = submit_with_token(&app, "attacker-choice", &plain_csr(), &token).await;
    assert_eq!(status, StatusCode::CREATED, "a pinned invite admits the submission");
    assert_eq!(
        created["siteName"], "site-pinned",
        "the name is the operator's pin, not the body's ask"
    );
    assert_eq!(created["region"], "us", "the region is pinned too");
}

/// An invite buys one enrollment. Re-presenting a spent token does not mint a
/// second certificate: it reads the first back, which is the restart-safe path
/// for an operator re-submitting after a restart.
#[tokio::test]
async fn a_spent_invite_returns_the_first_certificate_not_a_second() {
    let app = service();
    let token = invite_for(&app, "site-d").await;

    let (first_status, first) = submit_with_token(&app, "site-d", &plain_csr(), &token).await;
    assert_eq!(first_status, StatusCode::CREATED, "the first submit issues");
    assert_eq!(first["phase"], "issued");

    let (second_status, second) = submit_with_token(&app, "site-d", &plain_csr(), &token).await;
    assert_eq!(
        second_status,
        StatusCode::OK,
        "re-presenting the spent token reads the issued record back, not a replay"
    );
    assert_eq!(
        second["requestId"], first["requestId"],
        "no second enrollment: the same request comes back"
    );
    assert_eq!(
        second["certificate"], first["certificate"],
        "and the same certificate, not a freshly minted one"
    );
}

/// An expired invite cannot be redeemed, checked at the store where expiry is guarded.
#[tokio::test]
async fn an_expired_invite_cannot_be_redeemed() {
    use enrollment::{NewInvite, Store, StoreError};
    use time::{Duration, OffsetDateTime};

    let store = Store::memory();
    let created = store
        .create_invite(NewInvite {
            token_sha256: "digest-of-an-expired-token".to_owned(),
            site_name: Some("site-d".to_owned()),
            region: Some("us".to_owned()),
            grid_network_ref: "demo-grid".to_owned(),
            issued_by: "tester".to_owned(),
            expires_at: OffsetDateTime::now_utc() - Duration::minutes(1),
        })
        .await
        .expect("create invite");

    let outcome = store
        .redeem_invite("digest-of-an-expired-token", uuid::Uuid::new_v4())
        .await;
    assert!(
        matches!(outcome, Err(StoreError::InviteUnavailable)),
        "an expired invite is refused, not honored: {outcome:?}"
    );
    assert!(created.redeemed_at.is_none(), "and it was never marked redeemed");
}
