//! Signing binds a broadcast to the site that sent it.
//!
//! The gossip transport key is shared by every member, so a valid packet proves
//! only that some member sent it. These tests cover the part that says which.

#![allow(clippy::tests_outside_test_module, reason = "integration tests live in tests/")]
#![expect(clippy::expect_used, reason = "tests")]

use crdt::GridStateSnapshot;
use swim::state_broadcast::StateBroadcast;

/// A site with an enrolled certificate and the key behind it.
struct Site {
    /// The site's private key, PKCS#8 DER.
    key_der: Vec<u8>,
    /// The certificate the grid issued.
    cert_pem: String,
}

/// Enroll a site with the grid CA, as the enrollment service would.
fn enrolled(ca: &certs::CaCert, site: &str) -> Site {
    let key = rcgen::KeyPair::generate().expect("key");
    let params = rcgen::CertificateParams::default();
    let csr = params.serialize_request(&key).expect("csr").pem().expect("pem");
    let issued = certs::sign_csr(ca, site, &csr).expect("sign");

    let key_pem = key.serialize_pem();
    let key_der = pem::parse(&key_pem).expect("key pem").contents().to_vec();
    Site {
        key_der,
        cert_pem: issued.cert_pem,
    }
}

fn broadcast_from(site: &Site, origin: &str, gateway: &str) -> StateBroadcast {
    let mut broadcast = StateBroadcast::new(
        origin.to_owned(),
        1,
        GridStateSnapshot::new("site-a".to_owned()),
        Some(gateway.to_owned()),
    )
    .with_cert(Some(site.cert_pem.clone()));
    broadcast.sign(&site.key_der).expect("sign");
    broadcast
}

/// What a receiver does: check the certificate names the origin, then the signature.
fn accept(ca_pem: &str, broadcast: &StateBroadcast) -> bool {
    let Some(cert) = broadcast.site_cert_pem.as_deref() else {
        return false;
    };
    let Ok(public_key) = certs::verify_site_cert(ca_pem, cert, &broadcast.origin_site) else {
        return false;
    };
    broadcast.verify(&public_key)
}

#[test]
fn a_site_signs_its_own_broadcast_and_it_is_accepted() {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let site_a = enrolled(&ca, "site-a");

    let broadcast = broadcast_from(&site_a, "site-a", "site-a.example:8443");
    assert!(accept(&ca.cert_pem, &broadcast), "a site's own broadcast is accepted");
}

/// The attack this exists to stop.
///
/// The transport key is shared, so site-b can put site-a's name on a packet and
/// the AEAD tag still verifies. Only the signature separates them.
#[test]
fn a_member_cannot_broadcast_as_another_site() {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let site_b = enrolled(&ca, "site-b");

    // site-b claims to be site-a, and points site-a's traffic at itself.
    let mut hijack = broadcast_from(&site_b, "site-a", "site-b.example:8443");
    assert!(
        !accept(&ca.cert_pem, &hijack),
        "site-b must not be able to broadcast as site-a"
    );

    // Nor by presenting its own certificate under site-a's name.
    hijack.site_cert_pem = Some(site_b.cert_pem.clone());
    hijack.sign(&site_b.key_der).expect("re-sign");
    assert!(
        !accept(&ca.cert_pem, &hijack),
        "a certificate naming site-b must not establish site-a"
    );
}

#[test]
fn an_unsigned_broadcast_is_not_accepted() {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let site_a = enrolled(&ca, "site-a");

    let unsigned = StateBroadcast::new(
        "site-a".to_owned(),
        1,
        GridStateSnapshot::new("site-a".to_owned()),
        Some("site-a.example:8443".to_owned()),
    )
    .with_cert(Some(site_a.cert_pem));

    assert!(unsigned.signature.is_none(), "this broadcast carries no signature");
    assert!(
        !accept(&ca.cert_pem, &unsigned),
        "an unsigned broadcast must not be accepted"
    );
}

/// Every field a receiver acts on has to be covered, or it can be rewritten in flight.
#[test]
fn tampering_with_any_acted_on_field_breaks_the_signature() {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let site_a = enrolled(&ca, "site-a");
    let original = broadcast_from(&site_a, "site-a", "site-a.example:8443");

    let mut redirected = original.clone();
    redirected.gateway_address = Some("attacker.example:8443".to_owned());
    assert!(
        !accept(&ca.cert_pem, &redirected),
        "rewriting the gateway address must break the signature"
    );

    let mut rewound = original.clone();
    rewound.revision = 99;
    assert!(
        !accept(&ca.cert_pem, &rewound),
        "rewriting the revision must break the signature"
    );

    let mut relabelled = original;
    relabelled.site_labels = Some([("region".to_owned(), "elsewhere".to_owned())].into_iter().collect());
    assert!(
        !accept(&ca.cert_pem, &relabelled),
        "adding labels must break the signature"
    );
}

/// A site outside the grid holds no certificate this grid will accept.
#[test]
fn a_site_from_another_grid_is_not_accepted() {
    let ours = certs::generate_ca("grid-ca").expect("ca");
    let theirs = certs::generate_ca("another-grid-ca").expect("other ca");
    let outsider = enrolled(&theirs, "site-a");

    let broadcast = broadcast_from(&outsider, "site-a", "outsider.example:8443");
    assert!(
        !accept(&ours.cert_pem, &broadcast),
        "a certificate from another grid must not be accepted"
    );
}

/// Signing must survive the wire, or receivers verify different bytes than were signed.
#[test]
fn a_signature_survives_encoding_and_decoding() {
    let ca = certs::generate_ca("grid-ca").expect("ca");
    let site_a = enrolled(&ca, "site-a");
    let broadcast = broadcast_from(&site_a, "site-a", "site-a.example:8443");

    let bytes = broadcast.encode().expect("encode");
    let decoded = StateBroadcast::decode(&bytes).expect("decode");

    assert_eq!(decoded.signature, broadcast.signature, "the signature rides the wire");
    assert!(
        accept(&ca.cert_pem, &decoded),
        "a decoded broadcast still verifies against the same bytes"
    );
}
