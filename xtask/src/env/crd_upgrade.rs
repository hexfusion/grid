//! Prove the upgrade from `grid.praxis-proxy.io/v1alpha1` to `grid.praxis.fast/v1beta1` on a throwaway kind
//! cluster.
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

/// API group the previous release served.
const LEGACY_GROUP: &str = "grid.praxis-proxy.io";

/// API group this release serves.
const GROUP: &str = "grid.praxis.fast";

/// Plural of every grid CRD.
const KINDS: [&str; 4] = ["gridnetworks", "gridsites", "inferenceproviders", "agenttoolproviders"];

/// Run the upgrade scenario, deleting the cluster afterwards whatever the outcome.
pub(crate) fn run() -> Result<(), Box<dyn std::error::Error>> {
    let crds = super::operator::generate_crd_json()?;
    let legacy = as_legacy(&crds)?;

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
    store_legacy(context, legacy)?;
    upgrade(context, crds, legacy)?;
    verify_upgraded(context)?;
    eprintln!("verify-crd-upgrade: [OK] upgrade path verified");
    Ok(())
}

/// Install the previous release's CRDs and store a `GridNetwork` under them.
fn store_legacy(context: &str, legacy: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  [1/4] install {LEGACY_GROUP}/v1alpha1 CRDs and store a v1alpha1 GridNetwork");
    kubectl_ok(context, &["apply", "-f", "-"], Some(legacy))?;
    wait_established(context, LEGACY_GROUP)?;
    kubectl_ok(
        context,
        &["apply", "-f", "-"],
        Some(&network_manifest(LEGACY_GROUP, "v1alpha1")),
    )?;
    kubectl_ok(
        context,
        &["apply", "-f", "-"],
        Some(&consumer_config_map("default", true)),
    )?;
    kubectl_ok(
        context,
        &["apply", "-f", "-"],
        Some(&consumer_config_map("kube-public", false)),
    )
    .map(drop)
}

/// The upgrade: delete grid objects, delete the old-group CRDs, install the new ones.
fn upgrade(context: &str, crds: &str, legacy: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  [2/4] upgrade: delete grid objects, delete {LEGACY_GROUP} CRDs, install {GROUP} CRDs");
    for kind in KINDS {
        kubectl_ok(
            context,
            &["delete", &format!("{kind}.{LEGACY_GROUP}"), "--all", "--wait"],
            None,
        )?;
    }
    kubectl_ok(context, &["delete", "-f", "-", "--wait"], Some(legacy))?;
    kubectl_ok(context, &["apply", "-f", "-"], Some(crds))?;
    wait_established(context, GROUP)?;
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

/// Confirm a `v1beta1` object round-trips under the new group and the old group is gone.
fn verify_upgraded(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("  [3/4] a {GROUP}/v1beta1 GridNetwork applies, reads back, and keeps its gridId");
    kubectl_ok(
        context,
        &["apply", "-f", "-"],
        Some(&network_manifest(GROUP, "v1beta1")),
    )?;
    let read = kubectl_ok(
        context,
        &[
            "get",
            &format!("gridnetworks.v1beta1.{GROUP}/{NETWORK}"),
            "-o",
            "jsonpath={.spec.gridId}",
        ],
        None,
    )?;
    if String::from_utf8_lossy(&read.stdout) != NETWORK {
        return Err("v1beta1 GridNetwork did not read back".into());
    }
    verify_grid_id_immutable(context)?;

    eprintln!("  [4/4] {LEGACY_GROUP} is no longer served; only the operator's old consumer ConfigMap is gone");
    verify_consumer_config_maps(context)?;
    verify_legacy_gone(context)
}

/// The old-group CRDs are deleted, the group is not served, and an old-group object is refused.
fn verify_legacy_gone(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    for kind in KINDS {
        let crd = kubectl(context, &["get", "crd", &format!("{kind}.{LEGACY_GROUP}")], None)?;
        if crd.status.success() || !String::from_utf8_lossy(&crd.stderr).contains("NotFound") {
            return Err(format!("CRD {kind}.{LEGACY_GROUP} survived the upgrade").into());
        }
    }
    let served = kubectl_ok(
        context,
        &["api-resources", "--api-group", LEGACY_GROUP, "-o", "name"],
        None,
    )?;
    if !String::from_utf8_lossy(&served.stdout).trim().is_empty() {
        return Err(format!("{LEGACY_GROUP} is still served after the upgrade").into());
    }
    let legacy_apply = kubectl(
        context,
        &["apply", "-f", "-"],
        Some(&network_manifest(LEGACY_GROUP, "v1alpha1")),
    )?;
    if legacy_apply.status.success() {
        return Err(format!("a {LEGACY_GROUP}/v1alpha1 GridNetwork was accepted after the upgrade").into());
    }
    Ok(())
}

/// The generated CRD list moved to the previous release's group, with every version renamed to `v1alpha1`.
fn as_legacy(crds: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut list: serde_json::Value = serde_json::from_str(crds)?;
    let items = list
        .get_mut("items")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("generate_crds output has no items")?;
    for crd in items.iter_mut() {
        if let Some(name) = crd.pointer_mut("/metadata/name")
            && let Some(plural) = name.as_str().and_then(|n| n.strip_suffix(&format!(".{GROUP}")))
        {
            *name = serde_json::json!(format!("{plural}.{LEGACY_GROUP}"));
        }
        if let Some(group) = crd.pointer_mut("/spec/group") {
            *group = serde_json::json!(LEGACY_GROUP);
        }
        for version in crd
            .pointer_mut("/spec/versions")
            .and_then(serde_json::Value::as_array_mut)
            .into_iter()
            .flatten()
        {
            if let Some(name) = version.get_mut("name") {
                *name = serde_json::json!("v1alpha1");
            }
        }
    }
    Ok(serde_json::to_string(&list)?)
}

/// A set `gridId` refuses a change, through the schema's transition rule.
fn verify_grid_id_immutable(context: &str) -> Result<(), Box<dyn std::error::Error>> {
    let changed = kubectl(
        context,
        &[
            "patch",
            &format!("gridnetworks.{GROUP}/{NETWORK}"),
            "--type",
            "merge",
            "-p",
            r#"{"spec":{"gridId":"another"}}"#,
        ],
        None,
    )?;
    if changed.status.success() || !String::from_utf8_lossy(&changed.stderr).contains("gridId is immutable") {
        return Err("a set gridId must be immutable".into());
    }
    Ok(())
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

/// A minimal `GridNetwork` at `group`/`version`.
fn network_manifest(group: &str, version: &str) -> String {
    serde_json::json!({
        "apiVersion": format!("{group}/{version}"),
        "kind": "GridNetwork",
        "metadata": { "name": NETWORK },
        "spec": { "gridId": NETWORK }
    })
    .to_string()
}

/// Wait until every grid CRD in `group` is established.
fn wait_established(context: &str, group: &str) -> Result<(), Box<dyn std::error::Error>> {
    let crds: Vec<String> = KINDS.iter().map(|kind| format!("crd/{kind}.{group}")).collect();
    let mut args = vec!["wait", "--for=condition=Established", "--timeout=60s"];
    args.extend(crds.iter().map(String::as_str));
    kubectl_ok(context, &args, None).map(drop)
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
    fn as_legacy_moves_the_group_renames_every_version_and_keeps_the_schema() {
        let crds = serde_json::json!({
            "apiVersion": "v1",
            "kind": "List",
            "items": [{
                "metadata": { "name": format!("gridnetworks.{GROUP}") },
                "spec": {
                    "group": GROUP,
                    "versions": [{ "name": "v1beta1", "served": true, "storage": true }]
                }
            }]
        })
        .to_string();
        let legacy: serde_json::Value =
            serde_json::from_str(&as_legacy(&crds).unwrap_or_else(|_| std::process::abort()))
                .unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            legacy.pointer("/items/0/metadata/name"),
            Some(&serde_json::json!("gridnetworks.grid.praxis-proxy.io"))
        );
        assert_eq!(
            legacy.pointer("/items/0/spec/group"),
            Some(&serde_json::json!("grid.praxis-proxy.io"))
        );
        assert_eq!(
            legacy.pointer("/items/0/spec/versions/0"),
            Some(&serde_json::json!({ "name": "v1alpha1", "served": true, "storage": true }))
        );
    }

    #[test]
    fn network_manifest_targets_the_requested_group_and_version() {
        assert!(network_manifest(LEGACY_GROUP, "v1alpha1").contains("grid.praxis-proxy.io/v1alpha1"));
        assert!(network_manifest(GROUP, "v1beta1").contains("grid.praxis.fast/v1beta1"));
    }
}
