//! Renewing a site identity with its current certificate.

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
use enrollment::{AppState, GridAdmins, SharedCa, Store, api::PeerLeaf, authz::Authorizer, router};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use time::{Duration, OffsetDateTime};
use tower::ServiceExt as _;

/// A grid: the service, and a copy of its CA to issue test leaves outside enrollment.
struct Grid {
    app: axum::Router,
    ca: certs::CaCert,
}

/// A site's identity: its leaf and the key behind it.
struct Identity {
    cert_pem: String,
    key_pem: String,
}

impl Identity {
    fn der(&self) -> Vec<u8> {
        x509_parser::pem::parse_x509_pem(self.cert_pem.as_bytes())
            .expect("pem")
            .1
            .contents
    }

    fn key_sha256(&self) -> String {
        certs::cert_public_key_sha256(&self.cert_pem).expect("key digest")
    }
}

fn grid(reserved: &[&str]) -> Grid {
    let ca = certs::generate_ca("test-grid-ca").expect("ca");
    let copy = certs::load_ca("test-grid-ca", &ca.key_pem, &ca.cert_pem).expect("copy");
    let app = router(Arc::new(AppState {
        store: Store::memory(),
        ca: SharedCa::new(ca),
        authorizer: Authorizer::Local(GridAdmins::from_table("tester: t0ken\n")),
        cert_lifetime: certs::DEFAULT_SITE_CERT_LIFETIME,
        reserved_sites: reserved.iter().map(|site| (*site).to_owned()).collect(),
    }));
    Grid { app, ca: copy }
}

/// A leaf for `site` from `ca`, outside enrollment, as bootstrap issues the hub's.
fn issue(ca: &certs::CaCert, site: &str, validity: certs::Validity) -> Identity {
    let csr = certs::generate_csr(site).expect("csr");
    let cert = certs::sign_csr(ca, site, &csr.csr_pem, validity).expect("sign");
    Identity {
        cert_pem: cert.cert_pem,
        key_pem: csr.key_pem.to_string(),
    }
}

async fn send(
    app: &axum::Router,
    path: &str,
    body: &Value,
    headers: &[(&str, String)],
    peer: Option<&Identity>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/json");
    for (name, value) in headers {
        builder = builder.header(*name, value);
    }
    let mut request = builder.body(Body::from(body.to_string())).expect("request");
    if let Some(peer) = peer {
        request.extensions_mut().insert(PeerLeaf(Some(Arc::from(peer.der()))));
    }
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response.into_body().collect().await.expect("body").to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

/// Enroll `site` under a fresh token, returning its identity.
async fn enroll(grid: &Grid, site: &str) -> Identity {
    let admin = [("authorization", "Bearer t0ken".to_owned())];
    let mint = json!({ "siteName": site, "gridNetworkRef": "demo-grid" });
    let (minted, token) = send(&grid.app, "/v1alpha1/enrollmenttokens", &mint, &admin, None).await;
    assert_eq!(minted, StatusCode::CREATED, "mint");
    let csr = certs::generate_csr(site).expect("csr");
    let bearer = [(
        "authorization",
        format!("Bearer {}", token["token"].as_str().expect("token")),
    )];
    let (enrolled, issued) = send(
        &grid.app,
        "/v1alpha1/enrollments",
        &json!({ "csr": csr.csr_pem }),
        &bearer,
        None,
    )
    .await;
    assert_eq!(enrolled, StatusCode::CREATED, "enroll");
    Identity {
        cert_pem: issued["certificate"].as_str().expect("certificate").to_owned(),
        key_pem: csr.key_pem.to_string(),
    }
}

/// A renewal's response and the key its request asked for.
struct Reply {
    status: StatusCode,
    body: Value,
    key_pem: String,
}

impl Reply {
    /// The renewed identity.
    fn identity(&self) -> Identity {
        Identity {
            cert_pem: self.body["certificate"].as_str().expect("certificate").to_owned(),
            key_pem: self.key_pem.clone(),
        }
    }
}

/// Renew presenting `peer` with a CSR for a fresh key.
async fn renew(grid: &Grid, site: &str, peer: Option<&Identity>) -> Reply {
    let csr = certs::generate_csr(site).expect("csr");
    let (status, body) = renew_with(grid, peer, &csr.csr_pem).await;
    Reply {
        status,
        body,
        key_pem: csr.key_pem.to_string(),
    }
}

async fn renew_with(grid: &Grid, peer: Option<&Identity>, csr_pem: &str) -> (StatusCode, Value) {
    send(&grid.app, "/v1alpha1/renewals", &json!({ "csr": csr_pem }), &[], peer).await
}

#[tokio::test]
async fn a_site_renews_with_its_current_certificate() {
    let grid = grid(&[]);
    let first = enroll(&grid, "site-a").await;

    let renewal = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(
        renewal.status,
        StatusCode::OK,
        "the current certificate renews: {}",
        renewal.body
    );
    assert_eq!(renewal.body["spiffeId"], certs::spiffe_id("site-a"));
    assert_eq!(
        renewal.body["caCertificate"],
        grid.ca.cert_pem.as_str(),
        "the CA rides back"
    );
    let second = renewal.identity();
    certs::verify_site_cert(&grid.ca.cert_pem, &second.cert_pem, "site-a").expect("same site, same CA");
    assert_ne!(second.key_sha256(), first.key_sha256(), "a new key");

    let again = renew(&grid, "site-a", Some(&second)).await;
    assert_eq!(again.status, StatusCode::OK, "the renewed certificate renews in turn");
}

#[tokio::test]
async fn a_replaced_certificate_cannot_renew_again() {
    let grid = grid(&[]);
    let first = enroll(&grid, "site-a").await;
    let renewal = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(renewal.status, StatusCode::OK);
    let current = renewal.identity();

    let refused = renew(&grid, "site-a", Some(&first)).await;
    assert_eq!(
        refused.status,
        StatusCode::FORBIDDEN,
        "the old key cannot pick a new key"
    );
    assert_eq!(refused.body["error"], "identity_refused");

    // A retry whose response was lost: the old leaf, asking again for the current key.
    let (retried, body) = renew_with(&grid, Some(&first), &rcgen_csr_for(&current.key_pem)).await;
    assert_eq!(retried, StatusCode::OK, "a lost response retries: {body}");
    assert_eq!(
        certs::cert_public_key_sha256(body["certificate"].as_str().expect("certificate")),
        Ok(current.key_sha256()),
        "re-signed for the key already recorded"
    );
}

#[tokio::test]
async fn renewal_without_a_usable_certificate_is_refused() {
    let grid = grid(&[]);
    let enrolled = enroll(&grid, "site-a").await;
    let now = OffsetDateTime::now_utc();

    let anonymous = renew(&grid, "site-a", None).await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED, "no certificate");
    assert_eq!(anonymous.body["error"], "identity_required");

    let expired = issue(
        &grid.ca,
        "site-a",
        certs::Validity {
            not_before: now.saturating_sub(Duration::days(40)),
            not_after: now.saturating_sub(Duration::days(10)),
        },
    );
    let lapsed = renew(&grid, "site-a", Some(&expired)).await;
    assert_eq!(lapsed.status, StatusCode::UNAUTHORIZED, "an expired certificate");

    let other_ca = certs::generate_ca("test-grid-ca").expect("other ca");
    let foreign = issue(&other_ca, "site-a", certs::Validity::default());
    let outsider = renew(&grid, "site-a", Some(&foreign)).await;
    assert_eq!(outsider.status, StatusCode::UNAUTHORIZED, "another grid's certificate");

    let (reused, body) = renew_with(&grid, Some(&enrolled), &rcgen_csr_for(&enrolled.key_pem)).await;
    assert_eq!(reused, StatusCode::FORBIDDEN, "a request for the presented key: {body}");
}

#[tokio::test]
async fn a_valid_leaf_without_a_record_is_refused() {
    let grid = grid(&[]);
    let unrecorded = issue(&grid.ca, "ghost", certs::Validity::default());
    let refused = renew(&grid, "ghost", Some(&unrecorded)).await;
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "only an enrolled name renews");
    assert_eq!(refused.body["error"], "identity_refused");
}

#[tokio::test]
async fn a_reserved_name_registers_on_first_renewal() {
    let grid = grid(&["hub"]);
    let bootstrap = issue(&grid.ca, "hub", certs::Validity::default());

    let registered = renew(&grid, "hub", Some(&bootstrap)).await;
    assert_eq!(
        registered.status,
        StatusCode::OK,
        "the bootstrap identity registers: {}",
        registered.body
    );
    let current = registered.identity();

    let replaced = renew(&grid, "hub", Some(&bootstrap)).await;
    assert_eq!(
        replaced.status,
        StatusCode::FORBIDDEN,
        "the replaced bootstrap leaf cannot renew again"
    );
    let renewed = renew(&grid, "hub", Some(&current)).await;
    assert_eq!(
        renewed.status,
        StatusCode::OK,
        "the registered hub renews like any site"
    );
}

#[tokio::test]
async fn a_newer_bootstrap_identity_supersedes_a_reserved_record() {
    let grid = grid(&["hub"]);
    let now = OffsetDateTime::now_utc();
    let issued_since = |days: i64, minutes: i64| certs::Validity {
        not_before: now
            .saturating_sub(Duration::days(days))
            .saturating_sub(Duration::minutes(minutes)),
        not_after: now.saturating_add(Duration::days(28)),
    };
    let older = issue(&grid.ca, "hub", issued_since(2, 0));
    assert_eq!(
        renew(&grid, "hub", Some(&older)).await.status,
        StatusCode::OK,
        "registered"
    );

    let forced = issue(&grid.ca, "hub", issued_since(0, 1));
    let superseded = renew(&grid, "hub", Some(&forced)).await;
    assert_eq!(
        superseded.status,
        StatusCode::OK,
        "bootstrap re-issued after the record: {}",
        superseded.body
    );

    let stale = issue(&grid.ca, "hub", issued_since(1, 0));
    assert_eq!(
        renew(&grid, "hub", Some(&stale)).await.status,
        StatusCode::FORBIDDEN,
        "a leaf older than the record is not newer issuance"
    );
}

#[tokio::test]
async fn a_newer_leaf_supersedes_only_a_reserved_name() {
    let grid = grid(&[]);
    let _enrolled = enroll(&grid, "site-a").await;
    let now = OffsetDateTime::now_utc();
    let minted = issue(
        &grid.ca,
        "site-a",
        certs::Validity {
            not_before: now.saturating_sub(Duration::minutes(1)),
            not_after: now.saturating_add(Duration::days(30)),
        },
    );
    assert_eq!(
        renew(&grid, "site-a", Some(&minted)).await.status,
        StatusCode::FORBIDDEN,
        "a spoke renews only with its recorded key"
    );
}

/// A CSR for the key in `key_pem`.
fn rcgen_csr_for(key_pem: &str) -> String {
    let key = rcgen::KeyPair::from_pem(key_pem).expect("key");
    rcgen::CertificateParams::default()
        .serialize_request(&key)
        .expect("csr")
        .pem()
        .expect("pem")
}
