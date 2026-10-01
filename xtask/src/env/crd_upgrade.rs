//! Prove the `v1alpha1` to `v1beta1` upgrade on a throwaway kind cluster.
//!
//! The cluster is created and deleted here, so the check never touches an existing install.

use std::{
    io::Write as _,
    process::{Command, Output, Stdio},
};

/// Kind cluster owned by this check.
const CLUSTER: &str = "grid-crd-upgrade";

/// `GridNetwork` the check stores at each version.
const NETWORK: &str = "crd-upgrade";

/// Run the upgrade scenario, deleting the cluster afterwards whatever the outcome.
pub(crate) fn run() -> Result<(), Box<dyn std::error::Error>> {
    let crds = super::operator::generate_crd_json()?;
    let legacy = as_v1alpha1(&crds)?;

    eprintln!("verify-crd-upgrade: creating kind cluster {CLUSTER}...");
    run_ok("kind", &["create", "cluster", "--name", CLUSTER, "--wait", "60s"], None)?;
    let result = scenario(&format!("kind-{CLUSTER}"), &crds, &legacy);
    eprintln!("verify-crd-upgrade: deleting kind cluster {CLUSTER}...");
    let deleted = run_ok("kind", &["delete", "cluster", "--name", CLUSTER], None);
    result?;
    deleted.map(drop)
}

/// The upgrade steps against `context`, which must hold no grid CRDs yet.
fn scenario(context: &str, crds: &str, legacy: &str) -> Result<(), Box<dyn std::error::Error>> {
    store_legacy_and_expect_refusal(context, crds, legacy)?;
    upgrade(context, crds, legacy)?;
    verify_upgraded(context)?;
    eprintln!("verify-crd-upgrade: [OK] upgrade path verified");
    Ok(())
}

/// Store a `v1alpha1` object, then show the `v1beta1` CRDs cannot be applied over it.
fn store_legacy_and_expect_refusal(context: &str, crds: &str, legacy: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  [1/5] install v1alpha1 CRDs and store a v1alpha1 GridNetwork");
    kubectl_ok(context, &["apply", "-f", "-"], Some(legacy))?;
    wait_established(context)?;
    kubectl_ok(context, &["apply", "-f", "-"], Some(&network_manifest("v1alpha1")))?;
    kubectl_ok(
        context,
        &["apply", "-f", "-"],
        Some(&consumer_config_map("default", true)),
    )?;
    kubectl_ok(
        context,
        &["apply", "-f", "-"],
        Some(&consumer_config_map("kube-public", false)),
    )?;

    eprintln!("  [2/5] applying v1beta1 CRDs over stored v1alpha1 objects is refused");
    let refused = kubectl(context, &["apply", "-f", "-"], Some(crds))?;
    if refused.status.success() {
        return Err("v1beta1 CRDs applied over stored v1alpha1 objects; expected storedVersions refusal".into());
    }
    let stderr = String::from_utf8_lossy(&refused.stderr);
    if !stderr.contains("storedVersions") {
        return Err(format!("unexpected refusal: {stderr}").into());
    }

    Ok(())
}

/// The upgrade: delete grid objects, delete the CRDs, install `v1beta1`.
fn upgrade(context: &str, crds: &str, legacy: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  [3/5] upgrade: delete grid objects, delete CRDs, install v1beta1 CRDs");
    for kind in ["gridnetworks", "gridsites", "inferenceproviders", "agenttoolproviders"] {
        kubectl_ok(
            context,
            &["delete", &format!("{kind}.grid.praxis-proxy.io"), "--all", "--wait"],
            None,
        )?;
    }
    kubectl_ok(context, &["delete", "-f", "-", "--wait"], Some(legacy))?;
    kubectl_ok(context, &["apply", "-f", "-"], Some(crds))?;
    wait_established(context)?;
    kubectl_ok(
        context,
        &[
            "delete",
            "configmap",
            "-A",
            "-l",
            "app.kubernetes.io/managed-by=grid-operator",
            "--field-selector",
            &format!("metadata.name={LEGACY_CONSUMER_CONFIG_MAP}"),
        ],
        None,
    )
    .map(drop)
}

/// Confirm a `v1beta1` object round-trips and `v1alpha1` is no longer served.
fn verify_upgraded(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  [4/5] a v1beta1 GridNetwork applies and reads back");
    kubectl_ok(context, &["apply", "-f", "-"], Some(&network_manifest("v1beta1")))?;
    let read = kubectl_ok(
        context,
        &[
            "get",
            &format!("gridnetworks.v1beta1.grid.praxis-proxy.io/{NETWORK}"),
            "-o",
            "jsonpath={.spec.gridId}",
        ],
        None,
    )?;
    if String::from_utf8_lossy(&read.stdout) != NETWORK {
        return Err("v1beta1 GridNetwork did not read back".into());
    }

    eprintln!("  [5/5] the v1alpha1 version is no longer served; only the operator's old consumer ConfigMap is gone");
    verify_consumer_config_maps(context)?;
    let legacy_apply = kubectl(context, &["apply", "-f", "-"], Some(&network_manifest("v1alpha1")))?;
    if legacy_apply.status.success() {
        return Err("a v1alpha1 GridNetwork was accepted after the upgrade".into());
    }
    Ok(())
}

/// The generated CRD list with every version renamed to `v1alpha1`, standing in for the previous release.
fn as_v1alpha1(crds: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut list: serde_json::Value = serde_json::from_str(crds)?;
    let items = list
        .get_mut("items")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("generate_crds output has no items")?;
    for version in items
        .iter_mut()
        .filter_map(|crd| crd.pointer_mut("/spec/versions"))
        .filter_map(serde_json::Value::as_array_mut)
        .flatten()
    {
        if let Some(name) = version.get_mut("name") {
            *name = serde_json::json!("v1alpha1");
        }
    }
    Ok(serde_json::to_string(&list)?)
}

/// Only the operator-written `praxis-consumer-config` is gone; another one stays.
fn verify_consumer_config_maps(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let config_maps = kubectl_ok(
        context,
        &[
            "get",
            "configmap",
            "-A",
            "--field-selector",
            &format!("metadata.name={LEGACY_CONSUMER_CONFIG_MAP}"),
            "-o",
            "jsonpath={.items[*].metadata.namespace}",
        ],
        None,
    )?;
    if String::from_utf8_lossy(&config_maps.stdout).trim() != "kube-public" {
        return Err("the upgrade must delete only the operator-written praxis-consumer-config".into());
    }
    Ok(())
}

/// The consumer `ConfigMap` name the previous release wrote.
const LEGACY_CONSUMER_CONFIG_MAP: &str = "praxis-consumer-config";

/// A `praxis-consumer-config` in `namespace`, labelled as the operator's when `operator_owned`.
fn consumer_config_map(namespace: &str, operator_owned: bool) -> String {
    let labels = if operator_owned {
        serde_json::json!({ "app.kubernetes.io/managed-by": "grid-operator" })
    } else {
        serde_json::json!({})
    };
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": { "name": LEGACY_CONSUMER_CONFIG_MAP, "namespace": namespace, "labels": labels },
        "data": { "praxis.yaml": "{}" }
    })
    .to_string()
}

/// A minimal `GridNetwork` at `version`.
fn network_manifest(version: &str) -> String {
    serde_json::json!({
        "apiVersion": format!("grid.praxis-proxy.io/{version}"),
        "kind": "GridNetwork",
        "metadata": { "name": NETWORK },
        "spec": { "gridId": NETWORK }
    })
    .to_string()
}

/// Wait until every grid CRD is established.
fn wait_established(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    kubectl_ok(
        context,
        &[
            "wait",
            "--for=condition=Established",
            "--timeout=60s",
            "crd/gridnetworks.grid.praxis-proxy.io",
            "crd/gridsites.grid.praxis-proxy.io",
            "crd/inferenceproviders.grid.praxis-proxy.io",
            "crd/agenttoolproviders.grid.praxis-proxy.io",
        ],
        None,
    )
    .map(drop)
}

/// Run `kubectl --context context args`, feeding `stdin` when given.
fn kubectl(context: &str, args: &[&str], stdin: Option<&str>) -> Result<Output, Box<dyn std::error::Error>> {
    let mut full = vec!["--context", context];
    full.extend_from_slice(args);
    exec("kubectl", &full, stdin)
}

/// [`kubectl`] that fails on a non-zero exit.
fn kubectl_ok(context: &str, args: &[&str], stdin: Option<&str>) -> Result<Output, Box<dyn std::error::Error>> {
    let mut full = vec!["--context", context];
    full.extend_from_slice(args);
    run_ok("kubectl", &full, stdin)
}

/// Run `program args`, feeding `stdin` when given, and capture its output.
fn exec(program: &str, args: &[&str], stdin: Option<&str>) -> Result<Output, Box<dyn std::error::Error>> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(if stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    if let (Some(input), Some(pipe)) = (stdin, child.stdin.as_mut()) {
        pipe.write_all(input.as_bytes())?;
    }
    drop(child.stdin.take());
    Ok(child.wait_with_output()?)
}

/// [`exec`] that fails on a non-zero exit, carrying stderr.
fn run_ok(program: &str, args: &[&str], stdin: Option<&str>) -> Result<Output, Box<dyn std::error::Error>> {
    let output = exec(program, args, stdin)?;
    if output.status.success() {
        Ok(output)
    } else {
        Err(format!(
            "{program} {}: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_v1alpha1_renames_every_version_and_keeps_the_schema() {
        let crds = serde_json::json!({
            "apiVersion": "v1",
            "kind": "List",
            "items": [{ "spec": { "versions": [{ "name": "v1beta1", "served": true, "storage": true }] } }]
        })
        .to_string();
        let legacy: serde_json::Value =
            serde_json::from_str(&as_v1alpha1(&crds).unwrap_or_else(|_| std::process::abort()))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            legacy.pointer("/items/0/spec/versions/0"),
            Some(&serde_json::json!({ "name": "v1alpha1", "served": true, "storage": true }))
        );
    }

    #[test]
    fn network_manifest_targets_the_requested_version() {
        assert!(network_manifest("v1alpha1").contains("grid.praxis-proxy.io/v1alpha1"));
    }
}
