//! GLB ingress hot-reload verifier.
//!
//! Runs prerequisite checks then a structured 23-step verification
//! representing the full GLB proof.  Steps 1-4 validate prerequisite
//! infrastructure.  Steps 5-10 verify SWIM cross-cluster discovery
//! (LB services, advertise addresses, seeds, overlay metadata,
//! gateway address advertisement, remote egress addresses).
//! Steps 11-12 check site stacks and edge config.  Steps 13-14
//! verify Forge-managed services are running.  Step 15 proves initial
//! inference routing.  Steps 16-19 prove session affinity: binding,
//! reuse, drain setup, and drain verification.  Steps 20-23 exercise
//! overlay hot-reload: modify the overlay, observe the reload, verify
//! routing, and confirm edge container stability.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Command,
    thread,
    time::Duration,
};

use crate::env::{StepResult, StepStatus, print_validate_all_table, safe_truncate_str, verify};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Edge service name in the GLB demo forge.yaml.
const EDGE_SERVICE: &str = "grid-edge-us-east";

/// Overlay-sync service name in the GLB demo forge.yaml.
const OVERLAY_SYNC_SERVICE: &str = "grid-overlay-sync-us-east";

/// Kubernetes namespace for Grid resources.
const GRID_SYSTEM_NS: &str = "grid-system";

/// Cluster name prefix from the GLB demo config.
const CLUSTER_PREFIX: &str = "grid-glb";

/// Expected cluster names in the GLB demo environment.
const CLUSTER_NAMES: &[&str] = &["site-us-east", "site-us-west", "site-us-central"];

/// Required CLI tools (checked during prerequisites).
const REQUIRED_TOOLS: &[&str] = &["kind", "kubectl", "curl", "docker"];

/// Total number of verification steps.
const TOTAL_STEPS: u32 = 23;

/// Provider-role clusters that advertise a gateway address.
const PROVIDER_CLUSTERS: &[&str] = &["site-us-west", "site-us-central"];

/// SWIM LB service name in the GLB demo.
const SWIM_LB_SERVICE: &str = "operator-swim-lb";

/// Overlay [`ConfigMap`] name on the edge site.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
const OVERLAY_CONFIGMAP: &str = "grid-overlay-glb-demo-consumer-gateway";

/// [`GridNetwork`] resource name in the GLB demo.
///
/// [`GridNetwork`]: crate
const GRID_NETWORK_NAME: &str = "glb-demo";

/// Overlay file relative to the working directory.
const OVERLAY_FILE: &str = ".forge/runtime/edge-us-east/grid-config.json";

/// Edge service host port.
const EDGE_PORT: u16 = 8080;

/// Time to wait for overlay hot-reload (debounce + propagation).
const HOT_RELOAD_WAIT: Duration = Duration::from_millis(1500);

/// Edge container name (deterministic from forge naming).
const EDGE_CONTAINER: &str = "grid-glb-demo-grid-edge-us-east";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Verify GLB ingress hot-reload readiness.
///
/// Checks prerequisites (config, tools, forge binary, placeholder
/// images), then runs a 23-step structured verification.  Exits
/// non-zero if any step is `FAIL` or `BLOCKED`.
///
/// # Errors
///
/// Returns an error if hard prerequisites fail (config, tools,
/// forge binary) or any verification step is not `PASS`.
pub(crate) fn verify_glb_ingress(forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("glb-ingress: checking prerequisites...");
    let ctx = check_prerequisites(forge_config)?;

    let mut results: Vec<StepResult> = Vec::new();
    run_steps(&ctx, &mut results);

    eprintln!();
    eprintln!("## GLB Ingress Hot-Reload Proof");
    print_validate_all_table(&results);

    let any_not_pass = results.iter().any(|r| r.status != StepStatus::Pass);
    if any_not_pass {
        let fail_count = results.iter().filter(|r| r.status.is_failure()).count();
        let blocked_count = results.iter().filter(|r| r.status == StepStatus::Blocked).count();
        Err(format!(
            "glb-ingress: {fail_count} FAIL, {blocked_count} BLOCKED \
             — hot-reload proof incomplete"
        )
        .into())
    } else {
        eprintln!("glb-ingress: all proof points PASS");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Prerequisites
// ---------------------------------------------------------------------------

/// Validated prerequisite context.
#[derive(Debug)]
struct PrereqContext {
    /// Path to the forge config file.
    config: PathBuf,
    /// Resolved forge binary path.
    forge_bin: String,
    /// Overlay file path (for hot-reload testing).
    overlay_path: PathBuf,
    /// Services blocked by placeholder images (warning only).
    placeholders: Vec<(String, String)>,
}

/// Check all prerequisites and return a context for the verification
/// steps.  Fails with a combined error if config, tools, or the forge
/// binary are missing.  Placeholder images are stored in the context
/// for per-step gating (warning, not fatal).  `:latest` images in the
/// GLB demo config are a hard failure.
fn check_prerequisites(forge_config: &Path) -> Result<PrereqContext, Box<dyn std::error::Error>> {
    let (errors, forge_bin) = collect_prereq_errors(forge_config);
    if !errors.is_empty() {
        report_prereq_errors(&errors);
        return Err(format!("{} prerequisite(s) failed", errors.len()).into());
    }

    let config_text = std::fs::read_to_string(forge_config)?;
    check_no_latest_images(&config_text, forge_config)?;

    let placeholders = detect_placeholder_images(&config_text);
    if !placeholders.is_empty() {
        warn_placeholder_images(&placeholders);
    }

    let forge_bin = forge_bin.unwrap_or_else(|| std::process::abort());

    Ok(PrereqContext {
        config: forge_config.to_path_buf(),
        forge_bin,
        overlay_path: PathBuf::from(OVERLAY_FILE),
        placeholders,
    })
}

/// Fail if the forge config or its demo resource files use `:latest`.
fn check_no_latest_images(config_text: &str, forge_config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let latest_images = detect_latest_images(config_text);
    if !latest_images.is_empty() {
        report_latest_images(&latest_images);
        return Err(format!(
            "{} service(s) use :latest — GLB demo requires pinned tags",
            latest_images.len()
        )
        .into());
    }

    let resource_latest = forge_config
        .parent()
        .map(detect_latest_in_resources)
        .unwrap_or_default();
    if !resource_latest.is_empty() {
        report_latest_resources(&resource_latest);
        return Err(format!(
            "{} resource file(s) use :latest — GLB demo requires pinned tags",
            resource_latest.len()
        )
        .into());
    }
    Ok(())
}

/// Print prerequisite errors to stderr.
fn report_prereq_errors(errors: &[String]) {
    eprintln!();
    for e in errors {
        eprintln!("  PREREQ FAIL: {e}");
    }
    eprintln!();
}

/// Warn about placeholder images (steps 7+ will be BLOCKED).
fn warn_placeholder_images(placeholders: &[(String, String)]) {
    eprintln!();
    for (svc, img) in placeholders {
        eprintln!("  WARNING: service '{svc}' uses placeholder image '{img}' — steps 7+ will be BLOCKED");
    }
    eprintln!();
}

/// Report `:latest` images found in the forge config (fatal).
fn report_latest_images(latest: &[(String, String)]) {
    eprintln!();
    for (svc, img) in latest {
        eprintln!("  FAIL: service '{svc}' uses unpinned image '{img}'");
    }
    eprintln!();
}

/// Report `:latest` images found in demo resource files (fatal).
fn report_latest_resources(latest: &[(PathBuf, String)]) {
    eprintln!();
    for (path, img) in latest {
        eprintln!("  FAIL: {} uses unpinned image '{img}'", path.display());
    }
    eprintln!();
}

/// Collect prerequisite errors for config, tools, and forge binary.
fn collect_prereq_errors(forge_config: &Path) -> (Vec<String>, Option<String>) {
    let mut errors: Vec<String> = Vec::new();
    if !forge_config.exists() {
        errors.push(format!("config file not found: {}", forge_config.display()));
    }
    for tool in REQUIRED_TOOLS {
        if !tool_available(tool) {
            errors.push(format!("required tool not found on PATH: {tool}"));
        }
    }
    let forge_bin = resolve_forge_binary();
    if forge_bin.is_none() {
        errors.push(
            "praxis-forge binary not found on PATH or at \
             target/debug/praxis-forge"
                .to_owned(),
        );
    }
    (errors, forge_bin)
}

/// Check whether a CLI tool is available on `PATH` via `which`.
fn tool_available(name: &str) -> bool {
    Command::new("which")
        .arg(name)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// Resolve the forge binary: prefer `praxis-forge` on PATH, fall
/// back to `target/debug/praxis-forge`.
fn resolve_forge_binary() -> Option<String> {
    if tool_available("praxis-forge") {
        return Some("praxis-forge".to_owned());
    }
    let local = "target/debug/praxis-forge";
    if Path::new(local).exists() {
        return Some(local.to_owned());
    }
    None
}

/// Detect placeholder images in forge config text.
///
/// Scans for lines containing both `image:` and `PLACEHOLDER`,
/// tracking the nearest preceding `- name:` line as the service name.
pub(crate) fn detect_placeholder_images(config_text: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut current_service = String::new();

    for line in config_text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("- name:") {
            rest.trim().clone_into(&mut current_service);
        }
        if trimmed.contains("image:") && trimmed.contains("PLACEHOLDER") {
            let image = trimmed
                .split("image:")
                .nth(1)
                .unwrap_or("")
                .trim()
                .trim_matches('"')
                .to_owned();
            results.push((current_service.clone(), image));
        }
    }
    results
}

/// Detect `:latest`-tagged images in forge config text.
///
/// Same scanning pattern as [`detect_placeholder_images`] — tracks
/// the nearest preceding `- name:` line as context.
pub(crate) fn detect_latest_images(config_text: &str) -> Vec<(String, String)> {
    let mut results = Vec::new();
    let mut current_service = String::new();

    for line in config_text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("- name:") {
            rest.trim().clone_into(&mut current_service);
        }
        if let Some(raw) = extract_image_value(trimmed)
            && image_is_latest(&raw)
        {
            results.push((current_service.clone(), raw));
        }
    }
    results
}

/// Detect `:latest`-tagged images in YAML resource files under the
/// demo directory's `resources/` subdirectory.
fn detect_latest_in_resources(demo_dir: &Path) -> Vec<(PathBuf, String)> {
    let resources_dir = demo_dir.join("resources");
    let mut results = Vec::new();
    let Ok(entries) = walk_yaml_files(&resources_dir) else {
        return results;
    };
    for path in entries {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            if let Some(raw) = extract_image_value(line.trim())
                && image_is_latest(&raw)
            {
                results.push((path.clone(), raw));
            }
        }
    }
    results
}

/// Collect `.yaml` / `.yml` file paths under a directory recursively.
fn walk_yaml_files(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Ok(sub) = walk_yaml_files(&path) {
                files.extend(sub);
            }
        } else if path.extension().is_some_and(|ext| ext == "yaml" || ext == "yml") {
            files.push(path);
        }
    }
    Ok(files)
}

/// Extract the image value from a YAML `image:` line.
fn extract_image_value(trimmed: &str) -> Option<String> {
    let rest = if let Some(r) = trimmed.strip_prefix("image:") {
        r
    } else if trimmed.contains("image:") {
        trimmed.split("image:").nth(1)?
    } else {
        return None;
    };
    let raw = rest.trim().trim_matches('"').to_owned();
    if raw.is_empty() { None } else { Some(raw) }
}

/// Check whether an image reference uses `:latest` (explicit or implied).
///
/// Digest-pinned images (`image@sha256:...`) are always considered
/// pinned regardless of whether a tag is also present.
fn image_is_latest(image: &str) -> bool {
    if image.contains('@') {
        return false;
    }
    let tag = image
        .rsplit_once('/')
        .map_or(image, |(_prefix, tail)| tail)
        .rsplit_once(':')
        .map(|(_name, tag)| tag);
    tag == Some("latest") || tag.is_none()
}

/// Parse the host port for a named service from forge config text.
///
/// Looks for the `- name: <service>` block and extracts the first
/// `host:` value from its `ports:` section.
#[cfg(test)]
fn parse_edge_host_port(config_text: &str, service_name: &str) -> Option<u16> {
    let name_marker = format!("- name: {service_name}");
    let mut in_service = false;

    for line in config_text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("- name:") {
            in_service = trimmed.contains(&name_marker);
            continue;
        }
        if in_service && let Some(rest) = trimmed.strip_prefix("host:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Verification steps
// ---------------------------------------------------------------------------

/// Run all 23 verification steps.
#[expect(
    clippy::too_many_lines,
    reason = "sequential proof steps: each step depends on the previous; splitting obscures the proof flow"
)]
fn run_steps(ctx: &PrereqContext, results: &mut Vec<StepResult>) {
    // Step 1: Forge config validation.
    step_banner(1, "validating forge config");
    let config_ok = record_step("prerequisites", results, || {
        validate_forge_config(&ctx.forge_bin, &ctx.config)
    });
    if !config_ok {
        block_remaining(2, "config validation failed", results);
        return;
    }

    // Step 2: Environment status.
    step_banner(2, "checking environment status");
    let status_json = match run_forge_status(&ctx.forge_bin, &ctx.config) {
        Ok(json) => {
            results.push(StepResult::pass("forge status", "forge status returned OK"));
            json
        },
        Err(e) => {
            results.push(StepResult::fail("forge status", e.as_ref()));
            block_remaining(3, "status unavailable", results);
            return;
        },
    };

    // Step 3: All clusters live.
    step_banner(3, "checking clusters live");
    let clusters_ok = record_step("clusters live", results, || check_clusters_live(&status_json));
    if !clusters_ok {
        block_remaining(4, "clusters not live", results);
        return;
    }

    // Step 4: Provider gateway IPs.
    step_banner(4, "checking provider gateway IPs");
    let gateways_ok = record_step("provider gateway IPs", results, check_provider_gateways_captured);

    // Step 5: SWIM LB services.
    step_banner(5, "checking SWIM LB services");
    record_step("swim lb services", results, check_swim_lb_services);

    // Step 6: Operator SWIM advertise address.
    step_banner(6, "checking operator SWIM advertise address");
    record_step("swim advertise addr", results, check_swim_advertise_addr);

    // Step 7: GridNetwork seeds populated.
    step_banner(7, "checking GridNetwork seeds");
    record_step("gridnetwork seeds", results, check_gridnetwork_seeds);

    // Step 8: Overlay metadata.
    step_banner(8, "checking overlay candidate metadata");
    record_step("overlay metadata", results, check_overlay_metadata);

    // Step 9: Provider gateway self-discovery.
    step_banner(9, "checking provider gateway self-discovery");
    let provider_gateway_addrs = match load_provider_gateway_addresses() {
        Ok(addrs) => addrs,
        Err(e) => {
            results.push(StepResult::fail("provider gateway addr", e.as_ref()));
            block_remaining(10, "provider gateway captures unavailable", results);
            return;
        },
    };
    record_step("provider gateway addr", results, || {
        check_provider_gateway_addr(&provider_gateway_addrs)
    });

    // Step 10: Remote GridSite egress addresses.
    step_banner(10, "checking remote GridSite egress addresses");
    record_step("remote gridsite egress", results, || {
        check_remote_gridsite_egress(&provider_gateway_addrs)
    });

    // Step 11: Provider gateways reachable.
    step_banner(11, "checking provider gateway reachability");
    if gateways_ok {
        record_step("provider gateways reachable", results, || {
            check_provider_gateways_reachable()
        });
    } else {
        results.push(StepResult::blocked(
            "provider gateways reachable",
            "gateway IPs not captured",
        ));
    }

    // Step 12: Edge config applied.
    step_banner(12, "checking edge config applied");
    record_step("edge config applied", results, check_site_stacks);

    // Gate steps 13+ on placeholder images.
    if !ctx.placeholders.is_empty() {
        let reason = format!(
            "placeholder images: {}",
            ctx.placeholders
                .iter()
                .map(|(svc, _)| svc.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        block_remaining(13, &reason, results);
        return;
    }

    // Step 13: Overlay-sync service running.
    step_banner(13, "checking overlay-sync service");
    let sync_ok = record_step("overlay-sync running", results, || {
        check_service_running(&status_json, OVERLAY_SYNC_SERVICE)
    });
    if !sync_ok {
        block_remaining(14, "overlay-sync not running", results);
        return;
    }

    // Step 14: Edge service running — capture container ID.
    step_banner(14, "capturing edge service identity");
    let edge_identity = match check_service_running(&status_json, EDGE_SERVICE) {
        Ok(evidence) => {
            let captured = extract_service_identity(&status_json, EDGE_SERVICE);
            results.push(StepResult::pass("edge service running", evidence));
            captured
        },
        Err(e) => {
            results.push(StepResult::fail("edge service running", e.as_ref()));
            block_remaining(15, "edge not running", results);
            return;
        },
    };

    // Step 15: Inference routed (initial request).
    step_banner(15, "sending inference request");
    let routed_ok = record_step("inference routed", results, check_inference_routed);
    if !routed_ok {
        block_remaining(16, "initial inference failed", results);
        return;
    }

    // Step 16: Session affinity — initial bind.
    step_banner(16, "session affinity bind");
    let provider_a = match check_session_bind(EDGE_PORT) {
        Ok((evidence, provider)) => {
            results.push(StepResult::pass("session affinity bind", evidence));
            provider
        },
        Err(e) => {
            results.push(StepResult::fail("session affinity bind", e.as_ref()));
            block_remaining(17, "session bind failed", results);
            return;
        },
    };

    // Step 17: Session affinity — reuse.
    step_banner(17, "session affinity reuse");
    let reuse_ok = record_step("session affinity reuse", results, || {
        check_session_reuse(EDGE_PORT, &provider_a)
    });
    if !reuse_ok {
        block_remaining(18, "session reuse failed", results);
        return;
    }

    // Step 18: Session drain — set candidate to existing_only.
    step_banner(18, "session drain setup");
    let drain_original = match setup_session_drain(&ctx.overlay_path, &provider_a) {
        Ok((evidence, original)) => {
            results.push(StepResult::pass("session drain setup", evidence));
            Some(original)
        },
        Err(e) => {
            results.push(StepResult::fail("session drain setup", e.as_ref()));
            block_remaining(19, "drain setup failed", results);
            return;
        },
    };

    // Step 19: Session drain — verify routing.
    step_banner(19, "session drain verified");
    record_step("session drain verified", results, || {
        check_session_drain(EDGE_PORT, &provider_a)
    });

    // Restore overlay after drain test.
    if let Some(original) = &drain_original {
        restore_overlay(&ctx.overlay_path, original);
        #[expect(
            clippy::disallowed_methods,
            reason = "xtask is synchronous; no async runtime available for tokio::time::sleep"
        )]
        {
            thread::sleep(HOT_RELOAD_WAIT);
        }
    }

    let reload_count_before = count_overlay_reload_logs(EDGE_CONTAINER).unwrap_or(0);

    // Step 20: Modify overlay (remove one provider).
    step_banner(20, "modifying overlay for hot-reload test");
    let original_overlay = match modify_overlay_for_test(&ctx.overlay_path) {
        Ok((evidence, original)) => {
            results.push(StepResult::pass("overlay modified", evidence));
            Some(original)
        },
        Err(e) => {
            results.push(StepResult::fail("overlay modified", e.as_ref()));
            block_remaining(21, "overlay modification failed", results);
            return;
        },
    };

    // Step 21: Hot-reload observed.
    step_banner(21, "checking hot-reload");
    #[expect(
        clippy::disallowed_methods,
        reason = "xtask is synchronous; no async runtime available for tokio::time::sleep"
    )]
    {
        thread::sleep(HOT_RELOAD_WAIT);
    }
    record_step("hot-reload observed", results, || {
        check_hot_reload_observed(EDGE_CONTAINER, reload_count_before)
    });

    // Step 22: Routing after reload.
    step_banner(22, "sending post-reload inference request");
    record_step("routing after reload", results, check_inference_routed);

    // Restore overlay before step 23.
    if let Some(original) = &original_overlay {
        restore_overlay(&ctx.overlay_path, original);
    }

    // Step 23: Edge container stable (same ID, no restart).
    step_banner(23, "checking edge container stability");
    record_step("edge container stable", results, || {
        check_container_stable(EDGE_CONTAINER, edge_identity.as_ref())
    });
}

/// Print a step progress banner.
fn step_banner(step: u32, description: &str) {
    eprintln!("glb-ingress: [{step}/{TOTAL_STEPS}] {description}...");
}

/// Record a step result, returning whether it passed.
fn record_step(
    label: &'static str,
    results: &mut Vec<StepResult>,
    f: impl FnOnce() -> Result<String, Box<dyn std::error::Error>>,
) -> bool {
    match f() {
        Ok(evidence) => {
            results.push(StepResult::pass(label, evidence));
            true
        },
        Err(e) => {
            results.push(StepResult::fail(label, e.as_ref()));
            false
        },
    }
}

/// Labels for all 23 steps, indexed from 0.
const STEP_LABELS: &[&str] = &[
    "prerequisites",
    "forge status",
    "clusters live",
    "provider gateway IPs",
    "swim lb services",
    "swim advertise addr",
    "gridnetwork seeds",
    "overlay metadata",
    "provider gateway addr",
    "remote gridsite egress",
    "provider gateways reachable",
    "edge config applied",
    "overlay-sync running",
    "edge service running",
    "inference routed",
    "session affinity bind",
    "session affinity reuse",
    "session drain setup",
    "session drain verified",
    "overlay modified",
    "hot-reload observed",
    "routing after reload",
    "edge container stable",
];

/// Block all steps from `from_step` (1-indexed) onward.
fn block_remaining(from_step: u32, reason: &str, results: &mut Vec<StepResult>) {
    for label in STEP_LABELS.get((from_step.saturating_sub(1) as usize)..).unwrap_or(&[]) {
        results.push(StepResult::blocked(label, reason.to_owned()));
    }
}

// ---------------------------------------------------------------------------
// Step implementations
// ---------------------------------------------------------------------------

/// Step 1: Validate forge config.
fn validate_forge_config(forge_bin: &str, config: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["config", "validate", "--config", &config.display().to_string()])
        .output()?;
    if output.status.success() {
        Ok("config validation passed".to_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("config validation failed: {}", safe_truncate_str(stderr.trim(), 120)).into())
    }
}

/// Step 2: Run `praxis-forge status --output json` and parse.
pub(crate) fn run_forge_status(
    forge_bin: &str,
    config: &Path,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let output = Command::new(forge_bin)
        .args(["status", "--config", &config.display().to_string(), "--output", "json"])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("forge status failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let stdout = String::from_utf8(output.stdout)?;
    let json: serde_json::Value = serde_json::from_str(&stdout)?;
    Ok(json)
}

/// Step 3: Verify all expected clusters are live.
pub(crate) fn check_clusters_live(status_json: &serde_json::Value) -> Result<String, Box<dyn std::error::Error>> {
    let clusters = status_json
        .get("data")
        .and_then(|d| d.get("clusters"))
        .and_then(serde_json::Value::as_array)
        .ok_or("status JSON missing data.clusters array")?;

    let mut missing = Vec::new();
    for expected in CLUSTER_NAMES {
        let found = clusters.iter().any(|c| {
            c.get("name").and_then(serde_json::Value::as_str) == Some(expected)
                && c.get("live").and_then(serde_json::Value::as_bool) == Some(true)
        });
        if !found {
            missing.push(*expected);
        }
    }

    if missing.is_empty() {
        Ok(format!("all {} clusters live", CLUSTER_NAMES.len()))
    } else {
        Err(format!("clusters not live: {}", missing.join(", ")).into())
    }
}

/// Step 4: Check provider gateway IPs via kubectl.
fn check_provider_gateways_captured() -> Result<String, Box<dyn std::error::Error>> {
    let mut found = Vec::new();
    for cluster in &["site-us-west", "site-us-central"] {
        let context = kubectl_context(cluster);
        let ip = get_provider_gateway_ip(&context)?;
        found.push(format!("{cluster}={ip}"));
    }
    Ok(found.join(", "))
}

/// Step 5: Check provider gateways are reachable via curl.
fn check_provider_gateways_reachable() -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in &["site-us-west", "site-us-central"] {
        let context = kubectl_context(cluster);
        let ip = get_provider_gateway_ip(&context)?;
        let url = format!("http://{ip}:8080/health");
        let resp = verify::curl_get(&url)?;
        if resp.status != 200 {
            return Err(format!("{cluster} gateway returned HTTP {} (expected 200)", resp.status).into());
        }
        verified.push(*cluster);
    }
    Ok(format!("{} gateways healthy", verified.len()))
}

/// Step 5: SWIM LB services exist on all clusters.
fn check_swim_lb_services() -> Result<String, Box<dyn std::error::Error>> {
    let mut found = Vec::new();
    for cluster in CLUSTER_NAMES {
        let ip = get_swim_lb_ip(&kubectl_context(cluster), cluster)?;
        found.push(format!("{cluster}={ip}"));
    }
    Ok(found.join(", "))
}

/// Get the external IP of the SWIM LB service via kubectl.
fn get_swim_lb_ip(context: &str, cluster: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            SWIM_LB_SERVICE,
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "{SWIM_LB_SERVICE} not found on {cluster}: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if !looks_like_ipv4(&ip) {
        return Err(format!("{SWIM_LB_SERVICE} on {cluster} has invalid IP '{ip}'").into());
    }
    Ok(ip)
}

/// Step 6: Operator SWIM advertise address matches LB IP.
fn check_swim_advertise_addr() -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in CLUSTER_NAMES {
        let context = kubectl_context(cluster);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "deploy",
                "grid-operator",
                "-o",
                "jsonpath={.spec.template.spec.containers[0].env}",
            ])
            .output()?;
        let env_json = String::from_utf8(output.stdout)?;
        let addr = parse_env_var_from_json(&env_json, "GRID_SWIM_ADVERTISE_ADDR");
        let Some(addr) = addr else {
            return Err(format!("GRID_SWIM_ADVERTISE_ADDR not set on {cluster}").into());
        };
        if addr.contains("$(POD_IP)") || addr.is_empty() || !addr.ends_with(":7946") {
            return Err(format!("GRID_SWIM_ADVERTISE_ADDR on {cluster} is '{addr}' (expected LB IP:7946)").into());
        }
        verified.push(format!("{cluster}={addr}"));
    }
    Ok(verified.join(", "))
}

/// Parse a named env var value from kubectl jsonpath env array JSON.
fn parse_env_var_from_json(json: &str, var_name: &str) -> Option<String> {
    let arr: Vec<serde_json::Value> = serde_json::from_str(json).ok()?;
    arr.iter().find_map(|entry| {
        let name = entry.get("name")?.as_str()?;
        if name == var_name {
            entry.get("value")?.as_str().map(str::to_owned)
        } else {
            None
        }
    })
}

/// Step 7: `GridNetwork` seeds populated on all clusters.
fn check_gridnetwork_seeds() -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in CLUSTER_NAMES {
        let context = kubectl_context(cluster);
        let output = Command::new("kubectl")
            .args([
                "--context",
                &context,
                "-n",
                GRID_SYSTEM_NS,
                "get",
                "gridnetwork",
                GRID_NETWORK_NAME,
                "-o",
                "jsonpath={.spec.seeds[*]}",
            ])
            .output()?;
        let seeds_raw = String::from_utf8(output.stdout)?.trim().to_owned();
        let count = parse_seeds_count(&seeds_raw);
        if count != 2 {
            return Err(format!("GridNetwork on {cluster} has {count} seed(s) (expected exactly 2)").into());
        }
        verified.push(format!("{cluster}={count}"));
    }
    Ok(format!("seeds: {}", verified.join(", ")))
}

/// Parse seed count from kubectl jsonpath array output.
fn parse_seeds_count(raw: &str) -> usize {
    let trimmed = raw.trim().trim_start_matches('[').trim_end_matches(']');
    if trimmed.is_empty() {
        return 0;
    }
    trimmed.split(' ').filter(|s| !s.is_empty()).count()
}

/// Step 8: Overlay [`ConfigMap`] has candidates with required metadata.
///
/// [`ConfigMap`]: https://kubernetes.io/docs/concepts/configuration/configmap/
fn check_overlay_metadata() -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context("site-us-east");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "cm",
            OVERLAY_CONFIGMAP,
            "-o",
            "jsonpath={.data.grid-config\\.json}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("overlay ConfigMap not found: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let raw = String::from_utf8(output.stdout)?;
    if raw.trim().is_empty() {
        return Err("overlay ConfigMap data is empty".into());
    }
    validate_overlay_json(&raw)
}

/// Validate overlay JSON contains candidates with required metadata.
fn validate_overlay_json(json: &str) -> Result<String, Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(json)?;

    doc.get("generated_at")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or("overlay missing generated_at")?;

    let candidates = doc
        .get("candidates")
        .and_then(serde_json::Value::as_array)
        .ok_or("overlay missing candidates array")?;

    if candidates.is_empty() {
        return Err("overlay has 0 candidates".into());
    }

    validate_candidate_metadata(candidates)?;
    Ok(format!(
        "{} candidate(s), validated: stable_id, admission_state, selection_tier, rank, generated_at",
        candidates.len()
    ))
}

/// Validate each candidate has required metadata fields with
/// correct types and non-empty values.
fn validate_candidate_metadata(candidates: &[serde_json::Value]) -> Result<(), Box<dyn std::error::Error>> {
    let required_strings = ["stable_id", "admission_state", "selection_tier"];
    for (i, c) in candidates.iter().enumerate() {
        for field in &required_strings {
            let val = c
                .get(*field)
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty());
            if val.is_none() {
                return Err(format!("candidate[{i}] missing or empty {field}").into());
            }
        }
        let has_rank = c
            .get("rank")
            .is_some_and(|v| v.as_u64().is_some() || v.as_i64().is_some());
        if !has_rank {
            return Err(format!("candidate[{i}] missing or non-numeric rank").into());
        }
    }
    Ok(())
}

/// Step 9: Provider gateway self-discovery.
///
/// Proves the operator's self-discovery path works end-to-end:
///
/// 1. The `provider-gateway` Service on each provider cluster has a `LoadBalancer` IP matching the independent Forge
///    capture (verifier evidence only — the operator does not read captures).
/// 2. The operator deployment does **not** have `GRID_GATEWAY_ADDRESS` set from Forge captures (confirming it uses
///    self-discovery).
/// 3. The remote `GridSite` egress address on the edge cluster equals the Service LB address (confirming the address
///    was broadcast via SWIM).
fn check_provider_gateway_addr(
    expected_addrs: &BTreeMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut verified = Vec::new();
    for cluster in PROVIDER_CLUSTERS {
        let expected = expected_addrs
            .get(*cluster)
            .ok_or_else(|| format!("missing Forge capture for {cluster} provider gateway"))?;
        let actual = get_service_lb_address(cluster, "provider-gateway")?;
        verify_expected_gateway_addr(cluster, "provider-gateway Service LB", &actual, expected)?;
        verify_no_capture_injection(cluster)?;
        verified.push(format!("{cluster}={actual} (self-discovered, broadcast via SWIM)"));
    }
    Ok(verified.join(", "))
}

/// Confirm the operator does not have `GRID_GATEWAY_ADDRESS` set from
/// Forge capture templates (i.e. containing `captures.`).
fn verify_no_capture_injection(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = kubectl_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "deploy",
            "grid-operator",
            "-o",
            "jsonpath={.spec.template.spec.containers[0].env}",
        ])
        .output()?;
    let env_json = String::from_utf8(output.stdout)?;
    let gw_val = parse_env_var_from_json(&env_json, "GRID_GATEWAY_ADDRESS");
    if let Some(val) = &gw_val.filter(|v| !v.is_empty()) {
        return Err(
            format!("GRID_GATEWAY_ADDRESS on {cluster} is '{val}' — should be unset for self-discovery").into(),
        );
    }
    Ok(())
}

/// Read the first `LoadBalancer` ingress IP from a Service, formatted as `ip:port`.
fn get_service_lb_address(cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let raw = kubectl_service_lb_jsonpath(cluster, service)?;
    parse_service_lb_output(&raw, cluster, service)
}

/// Run kubectl to fetch Service LB IP and port via jsonpath.
fn kubectl_service_lb_jsonpath(cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context(cluster);
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            service,
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip},{.spec.ports[0].port}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "kubectl get svc/{service} on {cluster} failed: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?)
}

/// Parse `"ip,port"` output from kubectl jsonpath into `"ip:port"`.
fn parse_service_lb_output(raw: &str, cluster: &str, service: &str) -> Result<String, Box<dyn std::error::Error>> {
    let parts: Vec<&str> = raw.split(',').collect();
    let ip = parts
        .first()
        .copied()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("svc/{service} on {cluster} has no LoadBalancer IP"))?;
    let port = parts.get(1).copied().unwrap_or("8080");
    Ok(format!("{ip}:{port}"))
}

/// Verify a gateway address matches the independent Forge capture (verifier evidence only).
fn verify_expected_gateway_addr(
    cluster: &str,
    field: &str,
    actual: &str,
    expected: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if actual.is_empty() || actual.contains("$(POD_IP)") {
        return Err(format!("{field} on {cluster} is '{actual}' (expected captured IP)").into());
    }
    if actual != expected {
        return Err(format!("{field} on {cluster} is '{actual}' (expected Forge capture '{expected}')").into());
    }
    Ok(())
}

/// Step 10: Remote [`GridSite`] egress addresses on the edge cluster.
///
/// [`GridSite`]: crate
fn check_remote_gridsite_egress(
    expected_addrs: &BTreeMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context("site-us-east");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridsite",
            "-o",
            "json",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("kubectl get gridsite failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let raw = String::from_utf8(output.stdout)?;
    parse_gridsite_egress(&raw, expected_addrs)
}

/// Parse [`GridSite`] list JSON and verify provider egress addresses.
///
/// [`GridSite`]: crate
fn parse_gridsite_egress(
    json: &str,
    expected_addrs: &BTreeMap<String, String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(json)?;
    let items = doc
        .get("items")
        .and_then(serde_json::Value::as_array)
        .ok_or("gridsite list missing items")?;

    let mut verified = Vec::new();
    for provider in PROVIDER_CLUSTERS {
        let expected = expected_addrs
            .get(*provider)
            .ok_or_else(|| format!("missing Forge capture for {provider} provider gateway"))?;
        let addr = find_gridsite_egress(items, provider)?;
        if addr.is_empty() {
            return Err(format!("GridSite for {provider} has no egress address").into());
        }
        verify_expected_gateway_addr(provider, "GridSite egress", addr, expected)?;
        verified.push(format!("{provider}={addr}"));
    }
    Ok(verified.join(", "))
}

/// Find one provider site's egress address in a `GridSite` list.
fn find_gridsite_egress<'a>(
    items: &'a [serde_json::Value],
    provider: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    let expected_name = format!("{GRID_NETWORK_NAME}-{provider}");
    let site = items.iter().find(|item| {
        item.get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|n| n == expected_name)
    });
    let Some(site) = site else {
        return Err(format!("GridSite for {provider} not found on edge cluster").into());
    };
    Ok(site
        .pointer("/spec/egress/address")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(""))
}

/// Load expected provider gateway addresses from Forge's default state file
/// (verifier evidence only — operators self-discover their own addresses).
fn load_provider_gateway_addresses() -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let state = std::fs::read_to_string(".forge/state.json")?;
    parse_provider_gateway_captures(&state)
}

/// Parse provider gateway captures from Forge state JSON.
fn parse_provider_gateway_captures(json: &str) -> Result<BTreeMap<String, String>, Box<dyn std::error::Error>> {
    let doc: serde_json::Value = serde_json::from_str(json)?;
    let captures = doc
        .get("captures")
        .and_then(serde_json::Value::as_object)
        .ok_or("Forge state missing captures")?;

    let mut addrs = BTreeMap::new();
    for cluster in PROVIDER_CLUSTERS {
        let ip = captures
            .get(*cluster)
            .and_then(|c| c.get("provider-gateway-ip"))
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("Forge state missing captures.{cluster}.provider-gateway-ip"))?;
        addrs.insert((*cluster).to_owned(), format!("{ip}:8080"));
    }
    Ok(addrs)
}

/// Step 12: Verify the expected `GridNetwork` resource exists on the
/// edge cluster.
fn check_site_stacks() -> Result<String, Box<dyn std::error::Error>> {
    let context = kubectl_context("site-us-east");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "gridnetwork",
            GRID_NETWORK_NAME,
            "--no-headers",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "GridNetwork '{GRID_NETWORK_NAME}' not found on site-us-east: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    Ok(format!("GridNetwork '{GRID_NETWORK_NAME}' applied on site-us-east"))
}

/// Steps 7-8: Verify a service is running (phase=running,
/// containerId present).
fn check_service_running(
    status_json: &serde_json::Value,
    service_name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let svc = find_service_in_status(status_json, service_name)?;

    let phase = svc
        .get("phase")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    if phase != "running" {
        return Err(format!("service '{service_name}' phase={phase} (expected running)").into());
    }

    let container_id = svc.get("containerId").and_then(serde_json::Value::as_str);
    let Some(id) = container_id.filter(|id| !id.is_empty()) else {
        return Err(format!("service '{service_name}' has no containerId").into());
    };

    let restart_count = svc.get("restartCount").and_then(serde_json::Value::as_u64);
    let restart_text = restart_count.map_or_else(|| "unknown".to_owned(), |count| count.to_string());

    Ok(format!(
        "containerId={}, phase=running, restartCount={restart_text}",
        safe_truncate_str(id, 12)
    ))
}

/// Baseline identity for a Forge-managed service.
struct ServiceIdentity {
    /// Full container ID.
    id: String,
    /// Restart count observed before the proof window.
    restart_count: Option<u64>,
}

/// Extract baseline service identity from Forge status JSON.
fn extract_service_identity(status_json: &serde_json::Value, service_name: &str) -> Option<ServiceIdentity> {
    let svc = find_service_in_status(status_json, service_name).ok()?;
    let id = svc
        .get("containerId")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)?;
    let restart_count = svc.get("restartCount").and_then(serde_json::Value::as_u64);
    Some(ServiceIdentity { id, restart_count })
}

/// Step 15/22: Send an inference request and verify 200 OK with
/// provider attribution and model echo.
fn check_inference_routed() -> Result<String, Box<dyn std::error::Error>> {
    let resp = curl_post_with_auth(EDGE_PORT)?;
    if resp.status != 200 {
        return Err(format!("inference request returned HTTP {}", resp.status).into());
    }
    let provider = extract_provider(&resp).map_err(|_e| "inference response missing X-Grid-Demo-Provider header")?;
    let body: serde_json::Value =
        serde_json::from_str(&resp.body).map_err(|e| format!("inference response body is not valid JSON: {e}"))?;
    let model = body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .ok_or("inference response missing model field")?;
    Ok(format!("HTTP 200, model={model}, provider={provider}"))
}

/// Step 16: Bind a session and record which provider served it.
fn check_session_bind(port: u16) -> Result<(String, String), Box<dyn std::error::Error>> {
    let resp = curl_edge_request(port, Some("glb-proof-a"))?;
    if resp.status != 200 {
        return Err(format!("session bind returned HTTP {}", resp.status).into());
    }
    let provider = extract_provider(&resp)?;
    Ok((format!("session=glb-proof-a bound to {provider}"), provider))
}

/// Step 17: Verify the same session reuses the same provider.
fn check_session_reuse(port: u16, expected_provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    for i in 0..2 {
        let resp = curl_edge_request(port, Some("glb-proof-a"))?;
        if resp.status != 200 {
            return Err(format!("reuse request {i} returned HTTP {}", resp.status).into());
        }
        let provider = extract_provider(&resp)?;
        if provider != expected_provider {
            return Err(format!("session drift: expected {expected_provider}, got {provider}").into());
        }
    }
    Ok(format!(
        "session=glb-proof-a stable across 3 requests, provider={expected_provider}"
    ))
}

/// Step 18: Drain a provider via `existing_only` and wait for reload.
fn setup_session_drain(
    overlay_path: &Path,
    provider_site: &str,
) -> Result<(String, String), Box<dyn std::error::Error>> {
    let reload_before = count_overlay_reload_logs(EDGE_CONTAINER).unwrap_or(0);
    let (evidence, original) = modify_overlay_drain(overlay_path, provider_site)?;
    #[expect(
        clippy::disallowed_methods,
        reason = "xtask is synchronous; no async runtime available for tokio::time::sleep"
    )]
    {
        thread::sleep(HOT_RELOAD_WAIT);
    }
    match check_hot_reload_observed(EDGE_CONTAINER, reload_before) {
        Ok(reload_evidence) => Ok((format!("{evidence}; {reload_evidence}"), original)),
        Err(e) => {
            restore_overlay(overlay_path, &original);
            Err(e)
        },
    }
}

/// Step 19: Verify drain routing — new session avoids drained, old retains.
fn check_session_drain(port: u16, drained_provider: &str) -> Result<String, Box<dyn std::error::Error>> {
    let new_resp = curl_edge_request(port, Some("glb-proof-c"))?;
    if new_resp.status != 200 {
        return Err(format!("drain new-session returned HTTP {}", new_resp.status).into());
    }
    let new_provider = extract_provider(&new_resp)?;
    if new_provider == drained_provider {
        return Err(format!("new session routed to drained provider {drained_provider}").into());
    }
    let old_resp = curl_edge_request(port, Some("glb-proof-a"))?;
    if old_resp.status != 200 {
        return Err(format!("drain old-session returned HTTP {}", old_resp.status).into());
    }
    let old_provider = extract_provider(&old_resp)?;
    if old_provider != drained_provider {
        return Err(format!("existing session lost binding: expected {drained_provider}, got {old_provider}").into());
    }
    Ok(format!(
        "drained={drained_provider}, new session→{new_provider}, bound session→{old_provider}"
    ))
}

/// Step 20: Modify the overlay file for hot-reload testing.
///
/// Reads the current overlay, removes one candidate, and writes the
/// modified version back. Returns the evidence string and the original
/// content for later restoration.
fn modify_overlay_for_test(overlay_path: &Path) -> Result<(String, String), Box<dyn std::error::Error>> {
    let original = std::fs::read_to_string(overlay_path).map_err(|e| format!("failed to read overlay: {e}"))?;

    let mut doc: serde_json::Value =
        serde_json::from_str(&original).map_err(|e| format!("failed to parse overlay: {e}"))?;

    let candidates = doc
        .get_mut("candidates")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("overlay missing candidates array")?;

    let original_count = candidates.len();
    if original_count < 2 {
        return Err("need at least 2 candidates for hot-reload test".into());
    }
    candidates.pop();

    let modified = serde_json::to_string(&doc).map_err(|e| format!("failed to serialize: {e}"))?;
    write_overlay(overlay_path, &modified)?;

    Ok((
        format!("candidates {original_count} → {}", original_count - 1),
        original,
    ))
}

/// Step 21: Check docker logs for hot-reload evidence and verify
/// the container was not restarted.
fn check_hot_reload_observed(container: &str, previous_count: usize) -> Result<String, Box<dyn std::error::Error>> {
    let current_count = count_overlay_reload_logs(container)?;
    if current_count <= previous_count {
        return Err(format!(
            "overlay reload log count did not increase: before={previous_count}, after={current_count}"
        )
        .into());
    }
    Ok(format!(
        "overlay reload observed: before={previous_count}, after={current_count}"
    ))
}

/// Count overlay reload log entries for a Docker container.
fn count_overlay_reload_logs(container: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let output = Command::new("docker")
        .args(["logs", "--tail", "200", container])
        .output()?;
    let logs = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{logs}{stderr}");
    Ok(count_reload_entries(&combined))
}

/// Count hot-reload log entries in text.
fn count_reload_entries(logs: &str) -> usize {
    logs.matches("overlay reloaded").count()
}

/// Step 23: Verify the edge container was not restarted during the
/// hot-reload test.
fn check_container_stable(
    container: &str,
    expected: Option<&ServiceIdentity>,
) -> Result<String, Box<dyn std::error::Error>> {
    let state = inspect_container_state(container)?;

    if let Some(expected) = expected
        && state.id != expected.id
    {
        return Err(format!(
            "container restarted: expected {}, got {}",
            safe_truncate_str(&expected.id, 12),
            safe_truncate_str(&state.id, 12)
        )
        .into());
    }
    if let Some(expected) = expected.and_then(|id| id.restart_count)
        && state.restart_count != expected
    {
        return Err(format!(
            "container restart count changed: expected {expected}, got {}",
            state.restart_count
        )
        .into());
    }

    Ok(format!(
        "containerId={} unchanged, startedAt={}, restartCount={}",
        safe_truncate_str(&state.id, 12),
        state.started_at,
        state.restart_count
    ))
}

/// Minimal Docker container state needed by the verifier.
struct ContainerState {
    /// Full container ID.
    id: String,
    /// Docker start timestamp.
    started_at: String,
    /// Docker restart count.
    restart_count: u64,
}

/// Inspect a Docker container and parse its identity fields.
fn inspect_container_state(container: &str) -> Result<ContainerState, Box<dyn std::error::Error>> {
    let output = Command::new("docker")
        .args([
            "inspect",
            container,
            "--format",
            "{{.Id}} {{.State.StartedAt}} {{.RestartCount}}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!("docker inspect failed for {container}").into());
    }
    parse_container_state(container, &String::from_utf8_lossy(&output.stdout))
}

/// Parse Docker inspect output from `inspect_container_state`.
fn parse_container_state(container: &str, inspect: &str) -> Result<ContainerState, Box<dyn std::error::Error>> {
    let mut fields = inspect.split_whitespace();
    let id = fields
        .next()
        .ok_or_else(|| format!("docker inspect returned no container ID for {container}"))?
        .to_owned();
    let started_at = fields
        .next()
        .ok_or_else(|| format!("docker inspect returned no start time for {container}"))?
        .to_owned();
    let restart_count = fields
        .next()
        .ok_or_else(|| format!("docker inspect returned no restart count for {container}"))?
        .parse::<u64>()?;
    Ok(ContainerState {
        id,
        started_at,
        restart_count,
    })
}

/// Write content to the overlay file via rename (handles ownership).
fn write_overlay(overlay_path: &Path, content: &str) -> Result<(), Box<dyn std::error::Error>> {
    let parent = overlay_path.parent().ok_or("overlay path has no parent directory")?;
    let tmp = parent.join(".grid-config.json.tmp");
    std::fs::write(&tmp, content).map_err(|e| format!("failed to write temp overlay: {e}"))?;
    std::fs::rename(&tmp, overlay_path).map_err(|e| format!("failed to rename overlay: {e}"))?;
    Ok(())
}

/// Restore the overlay file to its original content.
fn restore_overlay(overlay_path: &Path, original: &str) {
    if write_overlay(overlay_path, original).is_err() {
        eprintln!("glb-ingress: WARNING: failed to restore overlay file");
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Quick IPv4 format check (4 dot-separated octets, each 0-255).
fn looks_like_ipv4(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok())
}

/// Build the kubectl context for a GLB demo cluster.
fn kubectl_context(cluster_name: &str) -> String {
    format!("kind-{CLUSTER_PREFIX}-{cluster_name}")
}

/// Get the external IP of the provider-gateway service via kubectl.
fn get_provider_gateway_ip(context: &str) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("kubectl")
        .args([
            "--context",
            context,
            "-n",
            GRID_SYSTEM_NS,
            "get",
            "svc",
            "provider-gateway",
            "-o",
            "jsonpath={.status.loadBalancer.ingress[0].ip}",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("kubectl get svc failed: {}", safe_truncate_str(stderr.trim(), 120)).into());
    }
    let ip = String::from_utf8(output.stdout)?.trim().to_owned();
    if !looks_like_ipv4(&ip) {
        return Err(format!("provider-gateway on {context} has invalid IP '{ip}'").into());
    }
    Ok(ip)
}

/// Find a service entry in forge status JSON by name.
pub(crate) fn find_service_in_status<'a>(
    status_json: &'a serde_json::Value,
    service_name: &str,
) -> Result<&'a serde_json::Value, Box<dyn std::error::Error>> {
    let services = status_json
        .get("data")
        .and_then(|d| d.get("services"))
        .and_then(serde_json::Value::as_array)
        .ok_or("status JSON missing data.services array")?;

    services
        .iter()
        .find(|s| s.get("name").and_then(serde_json::Value::as_str) == Some(service_name))
        .ok_or_else(|| format!("service '{service_name}' not found in status").into())
}

/// Send a Chat Completions request to the edge with bearer auth.
fn curl_post_with_auth(port: u16) -> Result<verify::HttpResponse, Box<dyn std::error::Error>> {
    curl_edge_request(port, None)
}

/// Chat Completions request body used by the verifier.
const CHAT_BODY: &str = r#"{"model":"sim-model-v1","messages":[{"role":"user","content":"hello"}],"max_tokens":64}"#;

/// Send a Chat Completions request with an optional session header.
fn curl_edge_request(port: u16, session_id: Option<&str>) -> Result<verify::HttpResponse, Box<dyn std::error::Error>> {
    let url = format!("http://127.0.0.1:{port}/v1/chat/completions");
    let header_file = header_dump_path();
    let header_path = header_file.display().to_string();
    let mut cmd = Command::new("curl");
    cmd.args([
        "-s",
        "-w",
        "\n%{http_code}",
        "--connect-timeout",
        "5",
        "--max-time",
        "15",
        "-D",
        &header_path,
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-H",
        "Authorization: Bearer test-token",
    ]);
    if let Some(sid) = session_id {
        cmd.args(["-H", &format!("X-Session-Id: {sid}")]);
    }
    cmd.args(["-d", CHAT_BODY, &url]);
    let output = cmd.output()?;
    let mut resp = verify::parse_curl_output(&String::from_utf8(output.stdout)?)?;
    resp.headers = parse_header_file(&header_file);
    drop(std::fs::remove_file(&header_file));
    Ok(resp)
}

/// Temp file path for curl header dumps.
fn header_dump_path() -> PathBuf {
    std::env::temp_dir().join(format!("glb-verify-headers-{}", std::process::id()))
}

/// Parse a curl `-D` header dump file into a map with lowercase keys.
fn parse_header_file(path: &Path) -> BTreeMap<String, String> {
    let content = std::fs::read_to_string(path).unwrap_or_default();
    parse_header_dump(&content)
}

/// Parse raw header dump text into a map with lowercase keys.
fn parse_header_dump(text: &str) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for line in text.lines() {
        if let Some((key, value)) = line.split_once(':') {
            let k = key.trim().to_lowercase();
            if !k.is_empty() && !k.starts_with("http/") {
                map.insert(k, value.trim().to_owned());
            }
        }
    }
    map
}

/// Extract the `X-Grid-Demo-Provider` header from a response.
fn extract_provider(resp: &verify::HttpResponse) -> Result<String, Box<dyn std::error::Error>> {
    resp.headers
        .get("x-grid-demo-provider")
        .cloned()
        .ok_or_else(|| "missing x-grid-demo-provider header".into())
}

/// Modify overlay to set a candidate's `admission_state` to `existing_only`.
///
/// Finds the candidate whose `site` field matches the given site name
/// and changes its `admission_state`. Returns the evidence string and
/// the original overlay content for restoration.
fn modify_overlay_drain(overlay_path: &Path, site: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let original = std::fs::read_to_string(overlay_path).map_err(|e| format!("failed to read overlay: {e}"))?;
    let mut doc: serde_json::Value =
        serde_json::from_str(&original).map_err(|e| format!("failed to parse overlay: {e}"))?;
    let candidates = doc
        .get_mut("candidates")
        .and_then(serde_json::Value::as_array_mut)
        .ok_or("overlay missing candidates array")?;
    let mut found = false;
    for c in candidates.iter_mut() {
        let s = c.get("site").and_then(serde_json::Value::as_str);
        if s == Some(site) {
            c.as_object_mut().ok_or("candidate is not an object")?.insert(
                "admission_state".to_owned(),
                serde_json::Value::String("existing_only".to_owned()),
            );
            found = true;
        }
    }
    if !found {
        return Err(format!("no candidate with site={site}").into());
    }
    let modified = serde_json::to_string(&doc).map_err(|e| format!("failed to serialize: {e}"))?;
    write_overlay(overlay_path, &modified)?;
    Ok((format!("drained site={site}"), original))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Sample forge.yaml with placeholder images.
    fn sample_config_with_placeholders() -> &'static str {
        "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: grid-glb-demo
spec:
  services:
    - name: grid-overlay-sync-us-east
      image: \"ghcr.io/praxis-proxy/grid-overlay-sync:sha-PLACEHOLDER\"
    - name: grid-edge-us-east
      image: \"ghcr.io/praxis-proxy/praxis-ai:sha-PLACEHOLDER\"
      ports:
        - bindAddress: \"127.0.0.1\"
          host: 8080
          container: 8080"
    }

    /// Sample forge.yaml with real SHA tags.
    fn sample_config_no_placeholders() -> &'static str {
        "\
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: grid-glb-demo
spec:
  services:
    - name: grid-overlay-sync-us-east
      image: \"ghcr.io/praxis-proxy/grid-overlay-sync:sha-abc123\"
    - name: grid-edge-us-east
      image: \"ghcr.io/praxis-proxy/praxis-ai:sha-def456\"
      ports:
        - bindAddress: \"127.0.0.1\"
          host: 9090
          container: 9090"
    }

    /// Build a status JSON with configurable service fields.
    fn status_with_services(phase: &str, container_id: Option<&str>) -> serde_json::Value {
        serde_json::json!({
            "status": "ok",
            "data": {
                "clusters": [
                    {"name": "site-us-east", "statePhase": "running", "live": true},
                    {"name": "site-us-west", "statePhase": "running", "live": true},
                    {"name": "site-us-central", "statePhase": "running", "live": true}
                ],
                "services": [
                    {
                        "name": OVERLAY_SYNC_SERVICE,
                        "containerName": "grid-glb-demo-grid-overlay-sync-us-east",
                        "phase": phase,
                        "health": "unknown",
                        "containerId": container_id,
                        "startedAt": "2026-07-22T14:31:00Z",
                        "restartCount": 0,
                    },
                    {
                        "name": EDGE_SERVICE,
                        "containerName": "grid-glb-demo-grid-edge-us-east",
                        "phase": phase,
                        "health": "unknown",
                        "containerId": container_id,
                        "startedAt": "2026-07-22T14:31:00Z",
                        "restartCount": 0,
                    }
                ]
            }
        })
    }

    #[test]
    fn detects_placeholder_images() {
        let placeholders = detect_placeholder_images(sample_config_with_placeholders());
        assert_eq!(placeholders.len(), 2, "should find 2 placeholders");
        assert_eq!(
            placeholders.first().map(|(n, _)| n.as_str()),
            Some("grid-overlay-sync-us-east"),
            "first placeholder service name"
        );
        assert_eq!(
            placeholders.get(1).map(|(n, _)| n.as_str()),
            Some("grid-edge-us-east"),
            "second placeholder service name"
        );
    }

    #[test]
    fn no_placeholders_in_clean_config() {
        let placeholders = detect_placeholder_images(sample_config_no_placeholders());
        assert!(placeholders.is_empty(), "should find no placeholders in clean config");
    }

    #[test]
    fn placeholder_images_detected() {
        let placeholders = detect_placeholder_images(sample_config_with_placeholders());
        assert!(!placeholders.is_empty(), "should detect placeholder images");
        assert!(
            placeholders.iter().any(|(_, img)| img.contains("PLACEHOLDER")),
            "should contain PLACEHOLDER tag: {placeholders:?}",
        );
    }

    #[test]
    fn detects_latest_explicit_tag() {
        let config = "\
services:
  - name: overlay-sync
    image: \"grid-overlay-sync:latest\"
  - name: edge
    image: \"praxis-ai:glb-demo\"";
        let latest = detect_latest_images(config);
        assert_eq!(latest.len(), 1, "should find 1 :latest image");
        assert_eq!(
            latest.first().map(|(n, _)| n.as_str()),
            Some("overlay-sync"),
            "service name"
        );
        assert_eq!(
            latest.first().map(|(_, i)| i.as_str()),
            Some("grid-overlay-sync:latest"),
            "image value"
        );
    }

    #[test]
    fn detects_latest_implicit_no_tag() {
        let config = "\
services:
  - name: operator
    image: grid-operator";
        let latest = detect_latest_images(config);
        assert_eq!(latest.len(), 1, "untagged image implies :latest");
    }

    #[test]
    fn no_latest_in_pinned_config() {
        let config = "\
services:
  - name: overlay-sync
    image: \"grid-overlay-sync:glb-demo\"
  - name: edge
    image: \"praxis-ai:glb-demo\"";
        let latest = detect_latest_images(config);
        assert!(latest.is_empty(), "pinned tags should not trigger: {latest:?}");
    }

    #[test]
    fn image_is_latest_checks() {
        assert!(image_is_latest("grid-operator:latest"), "explicit :latest");
        assert!(image_is_latest("grid-operator"), "no tag implies :latest");
        assert!(!image_is_latest("grid-operator:glb-demo"), "pinned tag");
        assert!(!image_is_latest("grid-operator:sha-abc123"), "sha tag");
    }

    #[test]
    fn image_is_latest_registry_port() {
        assert!(
            !image_is_latest("localhost:5000/grid-operator:glb-demo"),
            "registry port with pinned tag"
        );
        assert!(
            image_is_latest("localhost:5000/grid-operator:latest"),
            "registry port with :latest"
        );
        assert!(
            image_is_latest("localhost:5000/grid-operator"),
            "registry port with no tag"
        );
    }

    #[test]
    fn image_is_latest_digest_pinned() {
        assert!(
            !image_is_latest("repo/image@sha256:abcdef1234567890"),
            "digest-pinned image"
        );
        assert!(
            !image_is_latest("localhost:5000/repo/image@sha256:abcdef1234567890"),
            "digest-pinned with registry port"
        );
    }

    #[test]
    fn detect_latest_in_resources_finds_nested_yaml() {
        let dir = std::env::temp_dir().join(format!("glb-test-{}", std::process::id()));
        let resources = dir.join("resources").join("nested");
        std::fs::create_dir_all(&resources).unwrap_or_else(|_| std::process::abort());
        std::fs::write(resources.join("bad.yaml"), "  image: grid-operator:latest\n")
            .unwrap_or_else(|_| std::process::abort());
        std::fs::write(resources.join("good.yaml"), "  image: grid-operator:glb-demo\n")
            .unwrap_or_else(|_| std::process::abort());
        std::fs::write(resources.join("notes.txt"), "  image: foo:latest\n").unwrap_or_else(|_| std::process::abort());
        let results = detect_latest_in_resources(&dir);
        assert_eq!(results.len(), 1, "should find 1 :latest in nested yaml: {results:?}");
        assert!(
            results.first().map(|(_, img)| img.as_str()) == Some("grid-operator:latest"),
            "should report the image value"
        );
        drop(std::fs::remove_dir_all(&dir));
    }

    #[test]
    fn parses_forge_status_clusters() {
        let status = status_with_services("running", Some("abc123"));
        let result = check_clusters_live(&status);
        assert!(result.is_ok(), "all clusters should be live: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("3"), "should report 3 clusters: {evidence}");
    }

    #[test]
    fn service_running_check_pass() {
        let status = status_with_services("running", Some("abcdef1234567890"));
        let result = check_service_running(&status, EDGE_SERVICE);
        assert!(result.is_ok(), "should pass: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("containerId="), "evidence: {evidence}");
    }

    #[test]
    fn service_running_check_stopped_fails() {
        let status = status_with_services("stopped", Some("abc123"));
        let Err(err) = check_service_running(&status, EDGE_SERVICE) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("phase=stopped"), "error: {msg}");
    }

    #[test]
    fn service_running_no_container_id_fails() {
        let status = status_with_services("running", None);
        let Err(err) = check_service_running(&status, EDGE_SERVICE) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("no containerId"), "error: {msg}");
    }

    #[test]
    fn service_running_restart_count_nonzero_is_baseline() {
        let mut status = status_with_services("running", Some("abc123"));
        status
            .get_mut("data")
            .and_then(|data| data.get_mut("services"))
            .and_then(serde_json::Value::as_array_mut)
            .and_then(|services| services.get_mut(1))
            .and_then(|svc| svc.get_mut("restartCount"))
            .map_or_else(|| std::process::abort(), |count| *count = serde_json::json!(2));
        let evidence = check_service_running(&status, EDGE_SERVICE).unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("restartCount=2"), "evidence: {evidence}");
    }

    #[test]
    fn reload_entry_counter_counts_exact_messages() {
        let logs = "overlay reloaded\nunrelated\noverlay reloaded";
        assert_eq!(count_reload_entries(logs), 2, "should count reload entries");
    }

    #[test]
    fn extract_service_identity_present() {
        let status = status_with_services("running", Some("abcdef1234567890"));
        let identity = extract_service_identity(&status, EDGE_SERVICE).unwrap_or_else(|| std::process::abort());
        assert_eq!(identity.id, "abcdef1234567890", "should extract container ID");
        assert_eq!(identity.restart_count, Some(0), "should extract restart count");
    }

    #[test]
    fn extract_service_identity_missing() {
        let status = status_with_services("running", None);
        let identity = extract_service_identity(&status, EDGE_SERVICE);
        assert!(identity.is_none(), "should return None when containerId is null");
    }

    #[test]
    fn parses_edge_host_port_from_config() {
        let port = parse_edge_host_port(sample_config_with_placeholders(), "grid-edge-us-east");
        assert_eq!(port, Some(8080), "should parse host port 8080");
    }

    #[test]
    fn parses_custom_edge_host_port() {
        let port = parse_edge_host_port(sample_config_no_placeholders(), "grid-edge-us-east");
        assert_eq!(port, Some(9090), "should parse host port 9090");
    }

    #[test]
    fn edge_port_none_for_unknown_service() {
        let port = parse_edge_host_port(sample_config_with_placeholders(), "nonexistent");
        assert_eq!(port, None, "unknown service should return None");
    }

    #[test]
    fn blocked_steps_cause_nonzero_exit() {
        let results = [
            StepResult::pass("step-a", "ok"),
            StepResult::blocked("step-b", "not implemented"),
        ];
        let any_not_pass = results.iter().any(|r| r.status != StepStatus::Pass);
        assert!(any_not_pass, "BLOCKED step should prevent clean exit");
    }

    #[test]
    fn block_remaining_adds_each_remaining_step_once() {
        let mut results = vec![StepResult::pass("prerequisites", "ok")];
        block_remaining(22, "reload failed", &mut results);
        let blocked = results
            .iter()
            .filter(|r| r.status == StepStatus::Blocked)
            .collect::<Vec<_>>();
        assert_eq!(blocked.len(), 2, "steps 22 and 23 should be blocked once");
        assert_eq!(blocked.first().map(|r| r.label), Some("routing after reload"));
        assert_eq!(blocked.get(1).map(|r| r.label), Some("edge container stable"));
    }

    #[test]
    fn overlay_sync_service_check_pass() {
        let status = status_with_services("running", Some("abc123"));
        let result = check_service_running(&status, OVERLAY_SYNC_SERVICE);
        assert!(result.is_ok(), "overlay-sync should pass: {result:?}");
    }

    #[test]
    fn parse_env_var_finds_value() {
        let json =
            r#"[{"name":"GRID_SWIM_ADVERTISE_ADDR","value":"172.18.0.5:7946"},{"name":"RUST_LOG","value":"info"}]"#;
        let val = parse_env_var_from_json(json, "GRID_SWIM_ADVERTISE_ADDR");
        assert_eq!(val.as_deref(), Some("172.18.0.5:7946"), "should find advertise addr",);
    }

    #[test]
    fn parse_env_var_missing_returns_none() {
        let json = r#"[{"name":"RUST_LOG","value":"info"}]"#;
        let val = parse_env_var_from_json(json, "GRID_SWIM_ADVERTISE_ADDR");
        assert!(val.is_none(), "missing var should return None");
    }

    #[test]
    fn parse_env_var_invalid_json_returns_none() {
        let val = parse_env_var_from_json("not json", "FOO");
        assert!(val.is_none(), "invalid JSON should return None");
    }

    #[test]
    fn seeds_count_two_entries() {
        let raw = r#"["172.18.0.3:7946" "172.18.0.4:7946"]"#;
        assert_eq!(parse_seeds_count(raw), 2, "should count 2 seeds");
    }

    #[test]
    fn seeds_count_empty() {
        assert_eq!(parse_seeds_count("[]"), 0, "empty array should be 0");
        assert_eq!(parse_seeds_count(""), 0, "empty string should be 0");
    }

    #[test]
    fn looks_like_ipv4_valid() {
        assert!(looks_like_ipv4("172.18.0.3"), "standard private IP");
        assert!(looks_like_ipv4("10.0.0.1"), "class A private IP");
        assert!(looks_like_ipv4("0.0.0.0"), "all zeros");
        assert!(looks_like_ipv4("255.255.255.255"), "all max");
    }

    #[test]
    fn looks_like_ipv4_invalid() {
        assert!(!looks_like_ipv4(""), "empty string");
        assert!(!looks_like_ipv4("not-an-ip"), "text");
        assert!(!looks_like_ipv4("172.18.0"), "only 3 octets");
        assert!(!looks_like_ipv4("172.18.0.3:7946"), "IP with port");
        assert!(!looks_like_ipv4("256.0.0.1"), "octet out of range");
    }

    #[test]
    fn overlay_json_valid() {
        let json = serde_json::json!({
            "network": "glb-demo",
            "local_site": "site-us-east",
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {
                    "kind": "InferenceProvider",
                    "name": "sim-provider-us-west",
                    "site": "site-us-west",
                    "stable_id": "abc123",
                    "admission_state": "admitted",
                    "selection_tier": "preferred",
                    "rank": 1
                }
            ]
        });
        let result = validate_overlay_json(&json.to_string());
        assert!(result.is_ok(), "valid overlay should pass: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("1 candidate(s)"), "evidence: {evidence}",);
    }

    #[test]
    fn overlay_json_missing_generated_at_fails() {
        let json = serde_json::json!({
            "candidates": [{"stable_id": "x", "admission_state": "a", "selection_tier": "t", "rank": 1}]
        });
        let result = validate_overlay_json(&json.to_string());
        assert!(result.is_err(), "missing generated_at should fail");
    }

    #[test]
    fn overlay_json_missing_candidate_field_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {"stable_id": "x", "admission_state": "a", "selection_tier": "t"}
            ]
        });
        let Err(err) = validate_overlay_json(&json.to_string()) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("rank"), "error should mention rank: {msg}");
    }

    #[test]
    fn overlay_json_empty_candidates_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": []
        });
        let result = validate_overlay_json(&json.to_string());
        assert!(result.is_err(), "empty candidates should fail");
    }

    #[test]
    fn overlay_json_empty_stable_id_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {"stable_id": "", "admission_state": "admitted", "selection_tier": "preferred", "rank": 1}
            ]
        });
        let Err(err) = validate_overlay_json(&json.to_string()) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("stable_id"), "error should mention stable_id: {msg}");
    }

    #[test]
    fn overlay_json_non_numeric_rank_fails() {
        let json = serde_json::json!({
            "generated_at": "2026-07-25T00:00:00Z",
            "candidates": [
                {"stable_id": "x", "admission_state": "a", "selection_tier": "t", "rank": "high"}
            ]
        });
        let Err(err) = validate_overlay_json(&json.to_string()) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("rank"), "error should mention rank: {msg}");
    }

    #[test]
    fn provider_gateway_captures_parsed() {
        let state = serde_json::json!({
            "captures": {
                "site-us-west": {"provider-gateway-ip": "172.18.0.5"},
                "site-us-central": {"provider-gateway-ip": "172.18.0.6"}
            }
        });
        let addrs = parse_provider_gateway_captures(&state.to_string()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            addrs.get("site-us-west").map(String::as_str),
            Some("172.18.0.5:8080"),
            "west"
        );
        assert_eq!(
            addrs.get("site-us-central").map(String::as_str),
            Some("172.18.0.6:8080"),
            "central"
        );
    }

    #[test]
    fn gridsite_egress_found() {
        let expected = BTreeMap::from([
            ("site-us-west".to_owned(), "172.18.0.5:8080".to_owned()),
            ("site-us-central".to_owned(), "172.18.0.6:8080".to_owned()),
        ]);
        let json = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "glb-demo-site-us-west"},
                    "spec": {"egress": {"address": "172.18.0.5:8080"}}
                },
                {
                    "metadata": {"name": "glb-demo-site-us-central"},
                    "spec": {"egress": {"address": "172.18.0.6:8080"}}
                }
            ]
        });
        let result = parse_gridsite_egress(&json.to_string(), &expected);
        assert!(result.is_ok(), "should find egress: {result:?}");
        let evidence = result.unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("site-us-west="), "evidence: {evidence}");
        assert!(evidence.contains("site-us-central="), "evidence: {evidence}");
    }

    #[test]
    fn gridsite_egress_missing_fails() {
        let expected = BTreeMap::from([
            ("site-us-west".to_owned(), "172.18.0.5:8080".to_owned()),
            ("site-us-central".to_owned(), "172.18.0.6:8080".to_owned()),
        ]);
        let json = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "glb-demo-site-us-west"},
                    "spec": {"egress": {"address": ""}}
                },
                {
                    "metadata": {"name": "glb-demo-site-us-central"},
                    "spec": {"egress": {"address": "172.18.0.6:8080"}}
                }
            ]
        });
        let Err(err) = parse_gridsite_egress(&json.to_string(), &expected) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(msg.contains("site-us-west"), "error should name site: {msg}");
    }

    #[test]
    fn gridsite_egress_mismatch_fails() {
        let expected = BTreeMap::from([
            ("site-us-west".to_owned(), "172.18.0.9:8080".to_owned()),
            ("site-us-central".to_owned(), "172.18.0.6:8080".to_owned()),
        ]);
        let json = serde_json::json!({
            "items": [
                {
                    "metadata": {"name": "glb-demo-site-us-west"},
                    "spec": {"egress": {"address": "172.18.0.5:8080"}}
                },
                {
                    "metadata": {"name": "glb-demo-site-us-central"},
                    "spec": {"egress": {"address": "172.18.0.6:8080"}}
                }
            ]
        });
        let Err(err) = parse_gridsite_egress(&json.to_string(), &expected) else {
            std::process::abort()
        };
        let msg = err.to_string();
        assert!(
            msg.contains("expected Forge capture"),
            "error should name mismatch: {msg}"
        );
    }

    #[test]
    fn provider_gateway_captures_parse() {
        let json = serde_json::json!({
            "captures": {
                "site-us-west": {"provider-gateway-ip": "172.18.0.5"},
                "site-us-central": {"provider-gateway-ip": "172.18.0.6"}
            }
        });
        let captures = parse_provider_gateway_captures(&json.to_string()).unwrap_or_else(|_| std::process::abort());
        assert_eq!(
            captures.get("site-us-west").map(String::as_str),
            Some("172.18.0.5:8080")
        );
        assert_eq!(
            captures.get("site-us-central").map(String::as_str),
            Some("172.18.0.6:8080")
        );
    }

    #[test]
    fn provider_gateway_captures_missing_provider_fails() {
        let json = serde_json::json!({
            "captures": {
                "site-us-west": {"provider-gateway-ip": "172.18.0.5"}
            }
        });
        let Err(err) = parse_provider_gateway_captures(&json.to_string()) else {
            std::process::abort()
        };
        assert!(
            err.to_string().contains("site-us-central"),
            "error should name missing provider: {err}"
        );
    }

    #[test]
    fn step_labels_match_total() {
        assert_eq!(
            STEP_LABELS.len(),
            TOTAL_STEPS as usize,
            "STEP_LABELS length must match TOTAL_STEPS",
        );
    }

    #[test]
    fn parse_header_dump_basic() {
        let dump = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\nx-grid-demo-provider: site-us-west\r\n\r\n";
        let map = parse_header_dump(dump);
        assert_eq!(
            map.get("x-grid-demo-provider").map(String::as_str),
            Some("site-us-west"),
            "should parse provider header"
        );
        assert_eq!(
            map.get("content-type").map(String::as_str),
            Some("application/json"),
            "should parse content-type"
        );
        assert!(!map.contains_key("http/1.1 200 ok"), "should skip HTTP status line");
    }

    #[test]
    fn parse_header_dump_empty() {
        let map = parse_header_dump("");
        assert!(map.is_empty(), "empty input should produce empty map");
    }

    #[test]
    fn extract_provider_present() {
        let resp = verify::HttpResponse {
            status: 200,
            body: String::new(),
            headers: BTreeMap::from([("x-grid-demo-provider".to_owned(), "site-us-central".to_owned())]),
        };
        let result = extract_provider(&resp);
        assert_eq!(
            result.ok().as_deref(),
            Some("site-us-central"),
            "should extract provider"
        );
    }

    #[test]
    fn extract_provider_missing() {
        let resp = verify::HttpResponse {
            status: 200,
            body: String::new(),
            headers: BTreeMap::new(),
        };
        assert!(extract_provider(&resp).is_err(), "missing header should return error");
    }

    #[test]
    fn modify_overlay_drain_sets_existing_only_for_matching_site() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = dir.path().join("grid-config.json");
        let original = serde_json::json!({
            "candidates": [
                {"site": "site-us-west", "admission_state": "new_and_existing"},
                {"site": "site-us-central", "admission_state": "new_and_existing"}
            ]
        })
        .to_string();
        std::fs::write(&path, &original).unwrap_or_else(|_| std::process::abort());

        let (evidence, returned_original) =
            modify_overlay_drain(&path, "site-us-west").unwrap_or_else(|_| std::process::abort());
        let modified: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap_or_default())
            .unwrap_or_else(|_| std::process::abort());
        assert!(evidence.contains("site=site-us-west"), "evidence: {evidence}");
        assert_eq!(returned_original, original, "must return original for restore");
        assert_eq!(
            modified
                .pointer("/candidates/0/admission_state")
                .and_then(serde_json::Value::as_str),
            Some("existing_only")
        );
        assert_eq!(
            modified
                .pointer("/candidates/1/admission_state")
                .and_then(serde_json::Value::as_str),
            Some("new_and_existing")
        );
    }

    #[test]
    fn modify_overlay_drain_missing_site_fails() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| std::process::abort());
        let path = dir.path().join("grid-config.json");
        std::fs::write(
            &path,
            serde_json::json!({"candidates": [{"site": "site-us-central"}]}).to_string(),
        )
        .unwrap_or_else(|_| std::process::abort());

        let Err(err) = modify_overlay_drain(&path, "site-us-west") else {
            std::process::abort()
        };
        assert!(
            err.to_string().contains("site=site-us-west"),
            "error should name missing site: {err}"
        );
    }
}
