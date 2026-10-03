//! Renew the site identity before it expires, in place, with the current certificate.

use std::{collections::BTreeMap, time::Duration};

use k8s_openapi::{ByteString, api::core::v1::Secret};
use kube::{
    Api, Client,
    api::{Patch, PatchParams},
};
use reqwest::StatusCode;
use time::OffsetDateTime;
use zeroize::Zeroizing;

use super::{Enrollment, EnrollmentRequest, ErrorBody, Target, error_chain, pem_roots, read_capped, read_pem};
use crate::metrics;

/// The renewal route on the enrollment service.
const RENEW_PATH: &str = "/v1alpha1/renewals";

/// Longest wait between checks, so an identity replaced out of band is still watched.
const CHECK_EVERY: Duration = Duration::from_secs(60 * 60);

/// First retry after a failed renewal, doubling to [`RETRY_MAX`].
const RETRY_INITIAL: Duration = Duration::from_secs(60);

/// Longest wait between retries.
const RETRY_MAX: Duration = Duration::from_secs(30 * 60);

/// Secret keys holding a renewal in flight, so a restart retries with the same key.
const PENDING_KEY: &str = "renew.key";
/// The CSR for [`PENDING_KEY`].
const PENDING_CSR: &str = "renew.csr";

/// The leaf the current one replaced, kept so the co-located gateway stays recognised until it reloads.
pub const PREVIOUS_CERT: &str = "previous.crt";

/// When the identity needs renewing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Due {
    /// Not before this instant.
    At(OffsetDateTime),
    /// Now: less than a third of its lifetime remains.
    Now,
    /// It has expired and can no longer authenticate a renewal.
    Expired,
}

/// When an identity valid from `not_before` to `not_after` is due at `now`.
#[must_use]
pub fn due(not_before: OffsetDateTime, not_after: OffsetDateTime, now: OffsetDateTime) -> Due {
    if now >= not_after {
        return Due::Expired;
    }
    let renew_at = renew_after(not_before, not_after);
    if now >= renew_at { Due::Now } else { Due::At(renew_at) }
}

/// The instant a third of the lifetime remains.
#[must_use]
pub fn renew_after(not_before: OffsetDateTime, not_after: OffsetDateTime) -> OffsetDateTime {
    not_after.saturating_sub((not_after - not_before) / 3)
}

/// Why a renewal did not complete.
#[derive(Debug, thiserror::Error)]
pub(super) enum RenewError {
    /// The identity Secret is absent or incomplete.
    #[error("site identity unreadable: {0}")]
    Material(String),
    /// A Kubernetes call failed.
    #[error("kubernetes: {0}")]
    Kube(String),
    /// The service could not be reached or did not answer usably. Retried.
    #[error("enrollment service unavailable: {0}")]
    Transport(String),
    /// The service did not accept the presented certificate.
    #[error("renewal unauthenticated ({0}); re-enroll this site with a new site token")]
    Unauthenticated(String),
    /// The service refused this identity.
    #[error("renewal refused ({0}); re-enroll this site with a new site token")]
    Refused(String),
    /// The service answered with a certificate this site cannot use.
    #[error("renewal response invalid: {0}")]
    Invalid(String),
}

impl RenewError {
    /// Metric label.
    const fn result(&self) -> &'static str {
        match self {
            Self::Unauthenticated(_) | Self::Refused(_) => "refused",
            Self::Material(_) | Self::Kube(_) | Self::Transport(_) | Self::Invalid(_) => "failed",
        }
    }
}

/// What a check found or did.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Checked {
    /// Not due until this instant.
    Waiting(OffsetDateTime),
    /// Renewed, valid until this instant.
    Renewed(OffsetDateTime),
    /// Expired at this instant.
    Expired(OffsetDateTime),
}

/// The identity as stored.
pub(super) struct Held {
    /// Current leaf, PEM.
    pub(super) cert: String,
    /// Its key, PEM.
    pub(super) key: Zeroizing<String>,
    /// The grid CA, PEM.
    pub(super) ca: String,
    /// A renewal in flight: its CSR and key.
    pub(super) pending: Option<(String, Zeroizing<String>)>,
    /// The site Secret's resource version, a write precondition.
    pub(super) version: String,
}

/// Where the identity is read and written.
pub(super) trait IdentityStore {
    /// Read the identity in `target`.
    fn read(&self, target: &Target) -> impl Future<Output = Result<Held, RenewError>> + Send;
    /// Set or remove (`None`) keys of the site Secret at `version`, returning the new version.
    fn write(
        &self,
        target: &Target,
        version: &str,
        data: BTreeMap<&'static str, Option<Zeroizing<String>>>,
    ) -> impl Future<Output = Result<String, RenewError>> + Send;
}

/// How a renewal reaches the enrollment service.
pub(super) trait Renewer {
    /// Present `held`'s certificate and ask for a certificate for `csr_pem`.
    fn renew(&self, held: &Held, csr_pem: &str) -> impl Future<Output = Result<Enrollment, RenewError>> + Send;
}

/// Renew the identity in `target` if it is due at `now`.
///
/// # Errors
///
/// Returns [`RenewError`] when a due renewal does not complete.
pub(super) async fn check<S: IdentityStore + Sync, R: Renewer + Sync>(
    store: &S,
    renewer: &R,
    target: &Target,
    now: OffsetDateTime,
) -> Result<Checked, RenewError> {
    let held = store.read(target).await?;
    let (not_before, not_after) =
        certs::cert_validity(&held.cert).map_err(|e| RenewError::Material(format!("certificate: {e}")))?;
    metrics::set_site_identity_expiry(not_after.unix_timestamp());
    match due(not_before, not_after, now) {
        Due::At(at) => Ok(Checked::Waiting(at)),
        Due::Expired => Ok(Checked::Expired(not_after)),
        Due::Now => renew(store, renewer, target, held).await,
    }
}

/// Renew `held` now and store the result in place.
async fn renew<S: IdentityStore + Sync, R: Renewer + Sync>(
    store: &S,
    renewer: &R,
    target: &Target,
    mut held: Held,
) -> Result<Checked, RenewError> {
    let site = site_of(&held.cert)?;
    let (csr, key) = pending(store, target, &mut held, &site).await?;
    let enrollment = renewer.renew(&held, &csr).await?;
    let cert = verified(&enrollment, &held.ca, &site, &csr)?;
    let (_issued, valid_until) = certs::cert_validity(&cert).map_err(|e| RenewError::Invalid(e.to_string()))?;
    let replaced = BTreeMap::from([
        ("tls.crt", Some(Zeroizing::new(cert))),
        ("tls.key", Some(key)),
        (PREVIOUS_CERT, Some(Zeroizing::new(held.cert.clone()))),
        (PENDING_CSR, None),
        (PENDING_KEY, None),
    ]);
    store.write(target, &held.version, replaced).await?;
    metrics::set_site_identity_expiry(valid_until.unix_timestamp());
    Ok(Checked::Renewed(valid_until))
}

/// The renewal in flight, else a new key stored before it is sent, so a lost
/// response is retried with the key the service recorded.
async fn pending<S: IdentityStore + Sync>(
    store: &S,
    target: &Target,
    held: &mut Held,
    site: &str,
) -> Result<(String, Zeroizing<String>), RenewError> {
    if let Some(pending) = held.pending.take() {
        return Ok(pending);
    }
    let certs::GeneratedCsr { csr_pem, key_pem } =
        certs::generate_csr(site).map_err(|e| RenewError::Material(format!("generating a CSR: {e}")))?;
    let stash = BTreeMap::from([
        (PENDING_CSR, Some(Zeroizing::new(csr_pem.clone()))),
        (PENDING_KEY, Some(key_pem.clone())),
    ]);
    held.version = store.write(target, &held.version, stash).await?;
    Ok((csr_pem, key_pem))
}

/// The site a leaf's SPIFFE ID names.
fn site_of(cert_pem: &str) -> Result<String, RenewError> {
    let der = crate::resources::tls_backend::first_cert_der_from_pem(cert_pem)
        .map_err(|e| RenewError::Material(format!("certificate: {e}")))?;
    certs::leaf_spiffe_id(&der)
        .as_deref()
        .and_then(certs::site_of_spiffe_id)
        .map(str::to_owned)
        .ok_or_else(|| RenewError::Material("certificate names no grid site".to_owned()))
}

/// The renewed leaf, if it is this site's, from the CA it already holds, for the CSR's key.
fn verified(enrollment: &Enrollment, held_ca: &str, site: &str, csr_pem: &str) -> Result<String, RenewError> {
    let invalid = RenewError::Invalid;
    let ca = certs::anchored_ca(&enrollment.ca_certificate, held_ca)
        .map_err(|e| invalid(format!("returned CA is not this site's grid CA ({e})")))?;
    let cert = certs::leaf_only(&enrollment.certificate).map_err(|e| invalid(format!("certificate: {e}")))?;
    certs::verify_site_cert(&ca, &cert, site).map_err(|e| invalid(format!("certificate is not {site}'s ({e})")))?;
    let leaf_key = certs::cert_public_key(&cert).map_err(|e| invalid(e.to_string()))?;
    let csr_key = certs::csr_public_key(csr_pem).map_err(|e| invalid(e.to_string()))?;
    if leaf_key != csr_key {
        return Err(invalid("certificate does not carry the requested key".to_owned()));
    }
    Ok(cert)
}

/// The site identity Secret, through the Kubernetes API.
pub(super) struct KubeIdentity(pub(super) Api<Secret>);

/// One Secret value as text.
fn text(data: &BTreeMap<String, ByteString>, key: &str) -> Option<Zeroizing<String>> {
    data.get(key)
        .and_then(|value| String::from_utf8(value.0.clone()).ok())
        .map(Zeroizing::new)
}

impl IdentityStore for KubeIdentity {
    async fn read(&self, target: &Target) -> Result<Held, RenewError> {
        let missing = |what: &str| RenewError::Material(format!("{what} absent"));
        let site = Box::pin(self.0.get_opt(&target.site_secret))
            .await
            .map_err(|e| RenewError::Kube(e.to_string()))?
            .ok_or_else(|| missing(&format!("Secret {}", target.site_secret)))?;
        let version = site.metadata.resource_version.clone().unwrap_or_default();
        let data = site.data.unwrap_or_default();
        let ca = if target.ca_secret == target.site_secret {
            text(&data, "ca.crt")
        } else {
            Box::pin(self.0.get_opt(&target.ca_secret))
                .await
                .map_err(|e| RenewError::Kube(e.to_string()))?
                .and_then(|secret| secret.data)
                .and_then(|ca_data| text(&ca_data, "ca.crt"))
        };
        let pending = text(&data, PENDING_CSR).zip(text(&data, PENDING_KEY));
        Ok(Held {
            cert: text(&data, "tls.crt").ok_or_else(|| missing("tls.crt"))?.to_string(),
            key: text(&data, "tls.key").ok_or_else(|| missing("tls.key"))?,
            ca: ca.ok_or_else(|| missing("ca.crt"))?.to_string(),
            pending: pending.map(|(csr, key)| (csr.to_string(), key)),
            version,
        })
    }

    async fn write(
        &self,
        target: &Target,
        version: &str,
        data: BTreeMap<&'static str, Option<Zeroizing<String>>>,
    ) -> Result<String, RenewError> {
        let encoded: BTreeMap<&str, Option<ByteString>> = data
            .into_iter()
            .map(|(key, value)| (key, value.map(|text| ByteString(text.as_bytes().to_vec()))))
            .collect();
        // The resource version makes the patch fail if another writer changed the Secret.
        let patch = serde_json::json!({ "metadata": { "resourceVersion": version }, "data": encoded });
        let written = Box::pin(
            self.0
                .patch(&target.site_secret, &PatchParams::default(), &Patch::Merge(&patch)),
        )
        .await
        .map_err(|e| RenewError::Kube(e.to_string()))?;
        Ok(written.metadata.resource_version.unwrap_or_default())
    }
}

/// Renewal over HTTPS, presenting the current identity.
pub(super) struct HttpsRenewer {
    /// The renewal URL.
    url: String,
    /// PEM pinning the enrollment server, else the grid CA the site holds.
    pin: Option<String>,
}

impl HttpsRenewer {
    /// A renewer for the service at `base`, pinned to `pin` or the held grid CA.
    #[must_use]
    pub(super) fn new(base: &str, pin: Option<String>) -> Self {
        Self {
            url: format!("{}{RENEW_PATH}", base.trim_end_matches('/')),
            pin,
        }
    }

    /// A client presenting `held` and trusting only the pin.
    fn client(&self, held: &Held) -> Result<reqwest::Client, RenewError> {
        let roots = pem_roots(self.pin.as_deref().unwrap_or(&held.ca), "renewal trust")
            .map_err(|e| RenewError::Material(e.to_string()))?;
        let identity = client_identity(&held.cert, &held.key)
            .map_err(|e| RenewError::Material(format!("client identity: {}", error_chain(&e))))?;
        reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .tls_certs_only(roots)
            .identity(identity)
            .build()
            .map_err(|e| RenewError::Material(format!("renewal client: {e}")))
    }
}

/// The client identity: rustls takes one PEM, native-tls (fips) the certificate and key apart.
#[cfg(not(feature = "fips"))]
fn client_identity(cert: &str, key: &str) -> Result<reqwest::Identity, reqwest::Error> {
    reqwest::Identity::from_pem(Zeroizing::new(format!("{cert}{key}")).as_bytes())
}

/// The client identity: rustls takes one PEM, native-tls (fips) the certificate and key apart.
#[cfg(feature = "fips")]
fn client_identity(cert: &str, key: &str) -> Result<reqwest::Identity, reqwest::Error> {
    reqwest::Identity::from_pkcs8_pem(cert.as_bytes(), key.as_bytes())
}

impl Renewer for HttpsRenewer {
    async fn renew(&self, held: &Held, csr_pem: &str) -> Result<Enrollment, RenewError> {
        let response = self
            .client(held)?
            .post(&self.url)
            .json(&EnrollmentRequest { csr: csr_pem })
            .send()
            .await
            .map_err(|e| RenewError::Transport(error_chain(&e)))?;
        let status = response.status();
        let body = read_capped(response).await.map_err(RenewError::Transport)?;
        if status.is_success() {
            return serde_json::from_slice(&body).map_err(|e| RenewError::Invalid(format!("decoding: {e}")));
        }
        let detail = serde_json::from_slice::<ErrorBody>(&body).map_or_else(
            |_| status.to_string(),
            |err| format!("{status} {}: {}", err.error, err.message),
        );
        Err(match status {
            StatusCode::UNAUTHORIZED => RenewError::Unauthenticated(detail),
            StatusCode::FORBIDDEN => RenewError::Refused(detail),
            _ => RenewError::Transport(detail),
        })
    }
}

/// Settings for the renewal loop.
pub struct Settings {
    /// Enrollment service base URL.
    url: String,
    /// PEM pinning the enrollment server, when configured.
    pin: Option<String>,
    /// Where the identity lives when no `GridNetwork` names it.
    defaults: Target,
}

impl Settings {
    /// Read the renewal settings from the enrollment configuration.
    ///
    /// # Errors
    ///
    /// Returns a message when the URL is missing or not https, or the pin is unreadable.
    pub fn from_config(config: &super::Config) -> Result<Self, String> {
        let url = renewal_url(config.url.as_deref())?;
        let pin = config
            .ca_file
            .as_deref()
            .map(read_pem)
            .transpose()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            url: url.to_owned(),
            pin,
            defaults: Target {
                site_secret: config.identity_secret.clone(),
                ca_secret: config.ca_secret.clone(),
            },
        })
    }
}

/// The enrollment URL, https and outside the grid domain.
fn renewal_url(url: Option<&str>) -> Result<&str, String> {
    let url = url
        .map(str::trim)
        .filter(|url| url.starts_with("https://"))
        .ok_or("GRID_ENROLL_URL must be an https URL to renew")?;
    // Any site leaf names a host under the grid domain, so one could stand in for the service.
    let host = reqwest::Url::parse(url)
        .map_err(|e| format!("GRID_ENROLL_URL: {e}"))?
        .host_str()
        .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
        .unwrap_or_default();
    let grid_domain = certs::SPIFFE_TRUST_DOMAIN;
    if host == grid_domain || host.ends_with(&format!(".{grid_domain}")) {
        return Err(format!(
            "GRID_ENROLL_URL must not name a host under {grid_domain}, where site certificates live"
        ));
    }
    Ok(url)
}

/// Keep the site identity renewed for the life of the process.
#[expect(
    clippy::infinite_loop,
    reason = "a background renewer runs for the life of the process"
)]
pub async fn run(client: Client, settings: Settings) {
    let namespace = client.default_namespace().to_owned();
    let store = KubeIdentity(Api::namespaced(client.clone(), &namespace));
    let renewer = HttpsRenewer::new(&settings.url, settings.pin.clone());
    let mut last: Option<String> = None;
    let mut failures = 0_u32;
    loop {
        let target = Box::pin(super::resolve_target(
            &client,
            &namespace,
            &settings.defaults,
            super::STEP_BACKOFF,
        ))
        .await;
        let checked = match target {
            Ok(target) => check(&store, &renewer, &target, OffsetDateTime::now_utc()).await,
            Err(e) => Err(RenewError::Kube(e.to_string())),
        };
        let wait = settle(&checked, &mut last, &mut failures);
        tokio::time::sleep(jittered(wait)).await;
    }
}

/// Log a check once per change of state, count it, and return the wait before the next.
fn settle(checked: &Result<Checked, RenewError>, last: &mut Option<String>, failures: &mut u32) -> Duration {
    let (state, wait) = match checked {
        Ok(Checked::Waiting(at)) => (
            "waiting".to_owned(),
            until(*at, OffsetDateTime::now_utc()).min(CHECK_EVERY),
        ),
        // Re-read soon, so the next renewal is scheduled from the new leaf.
        Ok(Checked::Renewed(_)) => ("renewed".to_owned(), RETRY_INITIAL),
        Ok(Checked::Expired(_)) => ("expired".to_owned(), CHECK_EVERY),
        Err(error) => (error.to_string(), retry_delay(failures.saturating_add(1))),
    };
    report(checked, last.as_deref() != Some(state.as_str()), wait);
    *failures = if checked.is_err() {
        failures.saturating_add(1)
    } else {
        0
    };
    *last = Some(state);
    wait
}

/// Log and count a check: a renewal always, anything else only when it changed.
#[expect(clippy::cognitive_complexity, reason = "one tracing call per outcome")]
fn report(checked: &Result<Checked, RenewError>, changed: bool, wait: Duration) {
    match checked {
        Ok(Checked::Renewed(until)) => {
            metrics::record_site_identity_renewal("renewed");
            tracing::info!(not_after = %until, "site identity renewed");
        },
        Ok(Checked::Waiting(at)) if changed => tracing::info!(renew_after = %at, "site identity renewal scheduled"),
        Ok(Checked::Expired(at)) if changed => {
            metrics::record_site_identity_renewal("expired");
            tracing::error!(
                not_after = %at,
                "site identity expired and cannot renew; re-enroll this site with a new site token"
            );
        },
        Err(error) if changed => {
            metrics::record_site_identity_renewal(error.result());
            tracing::warn!(%error, retry_in_s = wait.as_secs(), "site identity renewal failed");
        },
        Err(error) => {
            metrics::record_site_identity_renewal(error.result());
            tracing::debug!(%error, retry_in_s = wait.as_secs(), "site identity renewal failed again");
        },
        Ok(_) => {},
    }
}

/// Time from `now` to `at`, zero once passed.
fn until(at: OffsetDateTime, now: OffsetDateTime) -> Duration {
    (at - now).try_into().unwrap_or(Duration::ZERO)
}

/// The wait after `failures` consecutive failures.
fn retry_delay(failures: u32) -> Duration {
    RETRY_INITIAL
        .saturating_mul(2_u32.saturating_pow(failures.saturating_sub(1)))
        .min(RETRY_MAX)
}

/// `wait` plus up to a tenth more, so sites enrolled together do not renew in step.
fn jittered(wait: Duration) -> Duration {
    let spread = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos(),
    );
    let tenth = wait
        .as_millis()
        .checked_div(10)
        .and_then(|ms| u64::try_from(ms).ok())
        .unwrap_or(0);
    wait.saturating_add(Duration::from_millis(spread.checked_rem(tenth).unwrap_or(0)))
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests;
