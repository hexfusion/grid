//! Crypto backend seam: rcgen (default) or OpenSSL EVP (`fips`).
//!
//! The keygen/sign/verify primitive is chosen at compile time. `fips` routes
//! through OpenSSL EVP so a FIPS host enforces the validated module.
//!
//! The two backends are equivalent under this crate's verifier (signature,
//! issuer match, SPIFFE SAN, validity), not byte-for-byte: the extension sets
//! differ, so a persisted CA should be loaded by the backend that minted it.

use time::OffsetDateTime;

#[cfg(feature = "rcgen")]
mod rcgen_backend;
#[cfg(feature = "rcgen")]
pub(crate) use rcgen_backend::{
    CaMaterial, csr_spki_der, generate_ca, generate_csr, hmac_sha256, issue_leaf, key_spki_der, load_ca, sha256,
    sign_csr, sign_message, verify_leaf_signature, verify_message,
};

#[cfg(feature = "fips")]
mod openssl_backend;
#[cfg(feature = "fips")]
pub(crate) use openssl_backend::{
    CaMaterial, csr_spki_der, generate_ca, generate_csr, hmac_sha256, issue_leaf, key_spki_der, load_ca, sha256,
    sign_csr, sign_message, verify_leaf_signature, verify_message,
};

/// Refuse a request key other than an uncompressed EC point on a named P-256 or
/// P-384 curve: the keys this grid issues and its peers verify.
pub(crate) fn check_request_key(spki_der: &[u8]) -> Result<(), BackendError> {
    use x509_parser::{
        oid_registry::{OID_EC_P256, OID_KEY_TYPE_EC_PUBLIC_KEY, OID_NIST_EC_P384},
        prelude::FromDer as _,
        x509::SubjectPublicKeyInfo,
    };

    let (rest, spki) = SubjectPublicKeyInfo::from_der(spki_der).map_err(|_bad| BackendError::ParseCsr)?;
    if !rest.is_empty() || spki.algorithm.algorithm != OID_KEY_TYPE_EC_PUBLIC_KEY {
        return Err(BackendError::UnsupportedKey);
    }
    // Explicit curve parameters are not an OID, so they fall through and are refused.
    let point_len = match spki
        .algorithm
        .parameters
        .as_ref()
        .and_then(|params| params.as_oid().ok())
    {
        Some(curve) if curve == OID_EC_P256 => 65,
        Some(curve) if curve == OID_NIST_EC_P384 => 97,
        _ => return Err(BackendError::UnsupportedKey),
    };
    let point = &spki.subject_public_key.data;
    if point.len() != point_len || point.first() != Some(&0x04) {
        return Err(BackendError::UnsupportedKey);
    }
    Ok(())
}

/// What a certificate should say, independent of the backend that mints it.
///
/// `is_ca` selects the extension set: a CA gets basic-constraints and cert-sign
/// usage, a leaf gets the server/client EKU, its DNS/SPIFFE SANs, and an org.
pub(crate) struct CertSpec<'spec> {
    /// Subject common name.
    pub common_name: &'spec str,
    /// Subject organization: `None` for a CA, `Some` for a leaf.
    pub organization: Option<&'spec str>,
    /// DNS SANs (leaf only).
    pub dns_sans: &'spec [String],
    /// SPIFFE URI SANs (leaf only).
    pub uri_sans: &'spec [String],
    /// Whether to mint a CA certificate.
    pub is_ca: bool,
    /// Not valid before.
    pub not_before: OffsetDateTime,
    /// Not valid after.
    pub not_after: OffsetDateTime,
}

/// A generated CA: its PEM pair plus the material needed to sign leaves.
pub(crate) struct GeneratedCa {
    /// PEM-encoded CA certificate.
    pub cert_pem: String,
    /// PEM-encoded CA private key.
    pub key_pem: String,
    /// Backend material for signing site certificates.
    pub material: CaMaterial,
}

/// A freshly minted certificate and the key generated with it.
pub(crate) struct GeneratedCert {
    /// PEM-encoded certificate.
    pub cert_pem: String,
    /// PEM-encoded private key.
    pub key_pem: String,
}

/// A CSR and its private key.
pub(crate) struct GeneratedCsr {
    /// PEM-encoded PKCS#10 request.
    pub csr_pem: String,
    /// PEM-encoded private key.
    pub key_pem: String,
}

/// A certificate signed from a request, plus the request's public key.
#[derive(Debug)]
pub(crate) struct SignedCsr {
    /// PEM-encoded certificate.
    pub cert_pem: String,
    /// The request's `SubjectPublicKeyInfo` DER, for fingerprinting the key.
    pub public_key_der: Vec<u8>,
}

/// Reasons a backend operation fails, mapped by callers onto their own errors.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BackendError {
    /// Key generation failed.
    KeyGen(String),
    /// Certificate signing failed.
    Sign(String),
    /// A certificate request could not be parsed.
    ParseCsr,
    /// A certificate request's self-signature does not verify.
    CsrBadSignature,
    /// A certificate request's key is not an EC key on P-256 or P-384.
    UnsupportedKey,
    /// A CA certificate could not be parsed.
    InvalidCaCert,
    /// A CA private key could not be parsed.
    InvalidCaKey(String),
    /// A CA certificate and private key do not correspond.
    CaCertKeyMismatch,
    /// A leaf signature does not verify against the CA.
    BadSignature,
}
