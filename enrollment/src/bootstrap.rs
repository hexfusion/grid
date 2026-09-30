//! The `enrollment bootstrap` subcommand.
//!
//! Mints or loads the Grid CA and issues the enrollment endpoint's serving
//! certificate, then writes them as Kubernetes Secrets for a pre-install Job.
//! Compiled only with `--features bootstrap`.
//!
//! Key separation is deliberate: the signing key lives only in the CA-key Secret
//! the enrollment service mounts, while peers receive the bundle Secret, which
//! holds the certificate and never the key. Generation is idempotent and never
//! overwrites an existing CA without `--force-regenerate`.

use std::error::Error;

use clap::Parser;

/// Boxed error that is `Send + Sync` so the async command future stays `Send`.
type BoxError = Box<dyn Error + Send + Sync>;

/// Arguments for `bootstrap`.
#[derive(Parser)]
#[command(name = "bootstrap", about = "Mint or load the Grid CA and write it as Secrets")]
struct BootstrapArgs {
    /// Namespace to read and write Secrets in. Defaults to `POD_NAMESPACE`, then
    /// `default`.
    #[arg(long)]
    namespace: Option<String>,
    /// Common name recorded on the CA certificate.
    #[arg(long, default_value = "grid-ca")]
    common_name: String,
    /// Secret holding the CA signing key (`tls.crt`, `tls.key`). Issuer-only.
    #[arg(long, default_value = "grid-ca-key")]
    ca_key_secret: String,
    /// Secret holding the public CA bundle (`ca.crt`). Never the key.
    #[arg(long, default_value = "grid-ca-bundle")]
    ca_bundle_secret: String,
    /// Secret holding the enrollment serving certificate (`tls.crt`, `tls.key`).
    #[arg(long, default_value = "enrollment-serving-tls")]
    serving_secret: String,
    /// Leave the serving certificate alone: a user-provided Secret serves instead.
    #[arg(long)]
    skip_serving: bool,
    /// A DNS name the serving certificate must cover. Repeatable.
    #[arg(long = "serving-dns")]
    serving_dns: Vec<String>,
    /// Secret holding the Postgres serving certificate (`tls.crt`, `tls.key`),
    /// issued from the grid CA so the service can connect with sslmode=verify-full.
    #[arg(long, default_value = "grid-db-serving-tls")]
    db_serving_secret: String,
    /// A DNS name the Postgres serving certificate must cover. Repeatable.
    #[arg(long = "db-dns")]
    db_dns: Vec<String>,
    /// Regenerate every certificate and the CA even if the Secrets exist.
    #[arg(long)]
    force_regenerate: bool,
}

/// Run the `bootstrap` subcommand.
///
/// Parses from the process arguments after the binary name, so `bootstrap`
/// sits in the argv0 slot clap ignores and the flags parse as usual.
pub(crate) async fn run() -> Result<(), BoxError> {
    let args = BootstrapArgs::parse_from(std::env::args_os().skip(1));
    bootstrap(&args).await
}

/// Generate or load the CA, then ensure the bundle and serving Secrets.
#[expect(
    clippy::large_stack_frames,
    reason = "one-shot init command; holds the large CaCert/Secret types, off any hot path"
)]
async fn bootstrap(args: &BootstrapArgs) -> Result<(), BoxError> {
    use k8s_openapi::api::core::v1::Secret;
    use kube::api::Api;

    let namespace = args
        .namespace
        .clone()
        .or_else(|| std::env::var("POD_NAMESPACE").ok())
        .unwrap_or_else(|| "default".to_owned());

    let client = kube::Client::try_default().await?;
    let secrets: Api<Secret> = Api::namespaced(client, &namespace);

    let ca = resolve_ca(&secrets, args).await?;
    write_opaque_secret(&secrets, &args.ca_bundle_secret, "ca.crt", &ca.cert_pem).await?;
    if !args.skip_serving {
        ensure_serving(&secrets, &ca, args).await?;
    }
    ensure_db_serving(&secrets, &ca, args).await?;
    Ok(())
}

/// Load the CA from its Secret, or generate and persist a fresh one.
///
/// Load when the Secret exists and no regenerate is forced, so re-runs keep the
/// same CA. The signing key is written only to the CA-key Secret.
async fn resolve_ca(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    args: &BootstrapArgs,
) -> Result<certs::CaCert, BoxError> {
    match load_tls_material(secrets, &args.ca_key_secret).await? {
        Some((cert_pem, key_pem)) if !args.force_regenerate => {
            Ok(certs::load_ca(&args.common_name, &key_pem, &cert_pem)?)
        },
        _ => {
            let ca = certs::generate_ca(&args.common_name)?;
            write_tls_secret(
                secrets,
                &args.ca_key_secret,
                &ca.cert_pem,
                &ca.key_pem,
                args.force_regenerate,
            )
            .await?;
            Ok(ca)
        },
    }
}

/// The `app.kubernetes.io/managed-by` value on every Secret bootstrap writes.
const MANAGED_BY: &str = "grid-enrollment-bootstrap";

/// Another manager's claim on a Secret: an owner reference (External Secrets,
/// Sealed Secrets, an operator), a different `managed-by` label, or cert-manager
/// annotations. An unlabelled Secret counts as ours, since earlier bootstrap runs
/// wrote it without the label.
fn foreign_manager(meta: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta) -> Option<String> {
    if let Some(owner) = meta.owner_references.as_deref().and_then(<[_]>::first) {
        return Some(format!("its owner {} {}", owner.kind, owner.name));
    }
    if let Some(owner) = meta
        .labels
        .as_ref()
        .and_then(|labels| labels.get("app.kubernetes.io/managed-by"))
        && owner != MANAGED_BY
    {
        return Some(format!("app.kubernetes.io/managed-by={owner}"));
    }
    meta.annotations
        .as_ref()
        .is_some_and(|annotations| annotations.keys().any(|key| key.starts_with("cert-manager.io/")))
        .then(|| "cert-manager".to_owned())
}

/// The serving certificate a Secret holds, as far as re-issue is concerned.
enum ServingCert {
    /// No Secret by that name.
    Absent,
    /// The Secret exists without a UTF-8 `tls.crt`.
    Unusable,
    /// The Secret's `tls.crt` PEM.
    Pem(String),
}

/// Read the serving certificate and any other manager's claim on its Secret. API
/// errors propagate, so a transient failure fails the Job instead of overwriting
/// the Secret.
async fn load_serving_cert(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<(ServingCert, Option<String>), BoxError> {
    let Some(secret) = secrets.get_opt(name).await? else {
        return Ok((ServingCert::Absent, None));
    };
    let foreign = foreign_manager(&secret.metadata);
    let pem = secret
        .data
        .as_ref()
        .and_then(|data| data.get("tls.crt"))
        .and_then(|cert| String::from_utf8(cert.0.clone()).ok());
    Ok((pem.map_or(ServingCert::Unusable, ServingCert::Pem), foreign))
}

/// Issue the serving certificate when needed; see [`serving_needs_issue`]. The CA is
/// preserved: only the leaf is re-signed under it.
async fn ensure_serving(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    let (current, foreign) = load_serving_cert(secrets, &args.serving_secret).await?;
    if serving_needs_issue(args.force_regenerate, &current, &args.serving_dns, &ca.cert_pem) {
        if let Some(owner) = foreign {
            return Err(format!(
                "serving Secret {} is managed by {owner}; refusing to replace it. Set serving.existingSecretRef to use it, \
                 or choose another serving.secretName",
                args.serving_secret
            )
            .into());
        }
        warn_if_unchained(&args.serving_secret, &current, &ca.cert_pem);
        let serving = certs::generate_dns_only_cert(ca, &args.common_name, &args.serving_dns)?;
        write_tls_secret(secrets, &args.serving_secret, &serving.cert_pem, &serving.key_pem, true).await?;
    }
    Ok(())
}

/// Whether to issue the serving certificate: forced, absent, unusable, missing a
/// requested DNS name, or not a currently valid leaf of the current CA (the CA was
/// regenerated, or the leaf expired).
fn serving_needs_issue(force: bool, current: &ServingCert, requested: &[String], ca_cert_pem: &str) -> bool {
    force
        || match current {
            ServingCert::Absent | ServingCert::Unusable => true,
            ServingCert::Pem(cert_pem) => {
                serving_sans_missing(cert_pem, requested) || certs::verify_issued_by(ca_cert_pem, cert_pem).is_err()
            },
        }
}

/// Warn when an existing leaf is replaced because it does not verify against the
/// current CA, so an unexpected CA regeneration is visible. Logs names and dates only.
fn warn_if_unchained(secret: &str, current: &ServingCert, ca_cert_pem: &str) {
    let ServingCert::Pem(cert_pem) = current else {
        return;
    };
    let Err(reason) = certs::verify_issued_by(ca_cert_pem, cert_pem) else {
        return;
    };
    let (issuer, not_after) = certs::cert_issuer_and_expiry(cert_pem)
        .unwrap_or_else(|_unparseable| ("unparseable".to_owned(), "unknown".to_owned()));
    tracing::warn!(
        secret,
        %reason,
        old_issuer = %issuer,
        old_not_after = %not_after,
        "re-issuing a serving certificate that does not verify against the current grid CA"
    );
}

/// Whether the serving cert lacks any requested DNS name, or cannot be parsed. A
/// subset check: extra names on the cert do not trigger a re-issue.
fn serving_sans_missing(cert_pem: &str, requested: &[String]) -> bool {
    // DNS names compare case-insensitively and ignore a trailing dot.
    fn norm(name: &str) -> String {
        name.trim_end_matches('.').to_ascii_lowercase()
    }
    match certs::cert_dns_sans(cert_pem) {
        Ok(current) => {
            let have: std::collections::BTreeSet<String> = current.iter().map(|name| norm(name)).collect();
            requested.iter().any(|name| !have.contains(&norm(name)))
        },
        Err(_unparseable) => true,
    }
}

/// Issue the Postgres serving certificate when needed; see [`serving_needs_issue`].
///
/// The service connects with sslmode=verify-full, so builtin Postgres needs a
/// leaf of the current grid CA whose SANs cover the DB Service names in `--db-dns`.
async fn ensure_db_serving(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    ca: &certs::CaCert,
    args: &BootstrapArgs,
) -> Result<(), BoxError> {
    let (current, foreign) = load_serving_cert(secrets, &args.db_serving_secret).await?;
    if serving_needs_issue(args.force_regenerate, &current, &args.db_dns, &ca.cert_pem) {
        if let Some(owner) = foreign {
            return Err(format!(
                "DB serving Secret {} is managed by {owner}; refusing to replace it. Remove that label or \
                 annotation, or delete the Secret, so bootstrap can issue it",
                args.db_serving_secret
            )
            .into());
        }
        warn_if_unchained(&args.db_serving_secret, &current, &ca.cert_pem);
        let serving = certs::generate_dns_only_cert(ca, "grid-enrollment-db", &args.db_dns)?;
        write_tls_secret(
            secrets,
            &args.db_serving_secret,
            &serving.cert_pem,
            &serving.key_pem,
            true,
        )
        .await?;
    }
    Ok(())
}

/// The `tls.crt`/`tls.key` PEM from a Secret, or `None` if it does not exist.
async fn load_tls_material(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
) -> Result<Option<(String, String)>, BoxError> {
    let Some(secret) = secrets.get_opt(name).await? else {
        return Ok(None);
    };
    let data = secret.data.as_ref().ok_or("secret has no data")?;
    let cert = data.get("tls.crt").ok_or("secret missing tls.crt")?;
    let key = data.get("tls.key").ok_or("secret missing tls.key")?;
    Ok(Some((
        String::from_utf8(cert.0.clone())?,
        String::from_utf8(key.0.clone())?,
    )))
}

/// Create or replace a `kubernetes.io/tls` Secret with a certificate and key.
async fn write_tls_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    cert_pem: &str,
    key_pem: &str,
    force: bool,
) -> Result<(), BoxError> {
    let mut string_data = std::collections::BTreeMap::new();
    string_data.insert("tls.crt".to_owned(), cert_pem.to_owned());
    string_data.insert("tls.key".to_owned(), key_pem.to_owned());
    apply_secret(secrets, name, Some("kubernetes.io/tls"), string_data, force).await
}

/// Create or replace an `Opaque` Secret with a single key.
///
/// The bundle is public, so this always refreshes it to match the current CA.
async fn write_opaque_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    key: &str,
    value: &str,
) -> Result<(), BoxError> {
    let mut string_data = std::collections::BTreeMap::new();
    string_data.insert(key.to_owned(), value.to_owned());
    apply_secret(secrets, name, None, string_data, true).await
}

/// Create the Secret, or replace it when it exists and `replace` is set.
#[expect(
    clippy::large_stack_frames,
    reason = "one-shot init command; holds the large k8s Secret type, off any hot path"
)]
async fn apply_secret(
    secrets: &kube::api::Api<k8s_openapi::api::core::v1::Secret>,
    name: &str,
    type_: Option<&str>,
    string_data: std::collections::BTreeMap<String, String>,
    replace: bool,
) -> Result<(), BoxError> {
    use k8s_openapi::{api::core::v1::Secret, apimachinery::pkg::apis::meta::v1::ObjectMeta};
    use kube::api::PostParams;

    // Boxed: the Secret type is large; keep it off this frame's stack.
    let secret = Box::new(Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            labels: Some(std::collections::BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                MANAGED_BY.to_owned(),
            )])),
            ..ObjectMeta::default()
        },
        type_: type_.map(ToOwned::to_owned),
        string_data: Some(string_data),
        ..Secret::default()
    });

    // Existence by metadata only, so the full object never lands on the stack.
    if secrets.get_metadata_opt(name).await?.is_some() {
        if replace {
            secrets.replace(name, &PostParams::default(), &secret).await?;
        }
    } else {
        secrets.create(&PostParams::default(), &secret).await?;
    }
    Ok(())
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "tests")]
mod tests {
    use std::{collections::BTreeMap, sync::LazyLock};

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};

    use super::{MANAGED_BY, ServingCert, foreign_manager, serving_needs_issue};

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn secrets_bootstrap_wrote_are_ours_to_replace() {
        let ours = ObjectMeta {
            labels: Some(map(&[("app.kubernetes.io/managed-by", MANAGED_BY)])),
            ..ObjectMeta::default()
        };
        assert_eq!(foreign_manager(&ours), None, "stamped by bootstrap");
        assert_eq!(
            foreign_manager(&ObjectMeta::default()),
            None,
            "unlabelled: written before the label existed"
        );
    }

    #[test]
    fn secrets_another_manager_owns_are_refused() {
        let helm = ObjectMeta {
            labels: Some(map(&[("app.kubernetes.io/managed-by", "Helm")])),
            ..ObjectMeta::default()
        };
        assert_eq!(
            foreign_manager(&helm).as_deref(),
            Some("app.kubernetes.io/managed-by=Helm")
        );
        let issued = ObjectMeta {
            annotations: Some(map(&[("cert-manager.io/certificate-name", "enrollment")])),
            ..ObjectMeta::default()
        };
        assert_eq!(foreign_manager(&issued).as_deref(), Some("cert-manager"));
        // An owner reference wins even over our own label.
        let synced = ObjectMeta {
            labels: Some(map(&[("app.kubernetes.io/managed-by", MANAGED_BY)])),
            owner_references: Some(vec![OwnerReference {
                kind: "ExternalSecret".to_owned(),
                name: "enrollment-serving".to_owned(),
                ..OwnerReference::default()
            }]),
            ..ObjectMeta::default()
        };
        assert_eq!(
            foreign_manager(&synced).as_deref(),
            Some("its owner ExternalSecret enrollment-serving")
        );
    }

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| (*name).to_owned()).collect()
    }

    /// The current grid CA the tests issue under.
    static CA: LazyLock<certs::CaCert> = LazyLock::new(|| certs::generate_ca("grid-ca").expect("ca"));

    /// A serving cert for `sans`, signed by [`CA`].
    fn serving_pem(sans: &[&str]) -> ServingCert {
        let leaf = certs::generate_dns_only_cert(&CA, "grid-ca", &names(sans)).expect("leaf");
        ServingCert::Pem(leaf.cert_pem)
    }

    /// Whether `current` needs issuing for `requested` under [`CA`].
    fn needs_issue(force: bool, current: &ServingCert, requested: &[String]) -> bool {
        serving_needs_issue(force, current, requested, &CA.cert_pem)
    }

    #[test]
    fn serving_cert_is_kept_when_every_requested_name_is_present() {
        let requested = names(&["enroll.grid.svc", "enroll.apps.example.com"]);
        let current = serving_pem(&["enroll.grid.svc", "enroll.apps.example.com"]);
        assert!(
            !needs_issue(false, &current, &requested),
            "unchanged values keep the cert"
        );
    }

    #[test]
    fn extra_names_on_the_cert_do_not_reissue() {
        let current = serving_pem(&["enroll.grid.svc", "old.apps.example.com"]);
        assert!(!needs_issue(false, &current, &names(&["enroll.grid.svc"])));
    }

    #[test]
    fn a_missing_name_reissues() {
        let current = serving_pem(&["enroll.grid.svc"]);
        let requested = names(&["enroll.grid.svc", "enroll.apps.example.com"]);
        assert!(needs_issue(false, &current, &requested), "a new route.host is added");
    }

    #[test]
    fn names_compare_without_case_or_trailing_dot() {
        let current = serving_pem(&["enroll.apps.example.com"]);
        assert!(!needs_issue(false, &current, &names(&["Enroll.Apps.Example.COM."])));
    }

    #[test]
    fn force_absent_unusable_and_unparseable_reissue() {
        let requested = names(&["enroll.grid.svc"]);
        assert!(
            needs_issue(true, &serving_pem(&["enroll.grid.svc"]), &requested),
            "forced"
        );
        assert!(needs_issue(false, &ServingCert::Absent, &requested), "absent");
        assert!(needs_issue(false, &ServingCert::Unusable, &requested), "no tls.crt");
        let garbage = ServingCert::Pem("not a certificate".to_owned());
        assert!(needs_issue(false, &garbage, &requested), "unparseable");
    }

    #[test]
    fn a_leaf_of_another_ca_reissues() {
        let old_ca = certs::generate_ca("grid-ca").expect("old ca");
        let leaf = certs::generate_dns_only_cert(&old_ca, "grid-ca", &names(&["enroll.grid.svc"])).expect("leaf");
        assert!(
            needs_issue(false, &ServingCert::Pem(leaf.cert_pem), &names(&["enroll.grid.svc"])),
            "a leaf the regenerated CA did not sign is re-issued"
        );
    }
}
