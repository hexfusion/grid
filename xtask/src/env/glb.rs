//! GLB ingress hot-reload verifier.
//!
//! Runs prerequisite checks then a structured 17-step verification
//! representing the full GLB hot-reload proof.  Steps 1-4 validate
//! prerequisite infrastructure.  Steps 5-8 verify SWIM cross-cluster
//! discovery (LB services, advertise addresses, seeds, overlay
//! metadata).  Steps 9-10 check site stacks and edge config.
//! Steps 11-12 verify Forge-managed services are running.  Step 13
//! proves initial inference routing.  Steps 14-17 exercise the
//! overlay hot-reload path: modify the overlay file, observe the
//! reload, verify routing still works, and confirm the edge
//! container was never restarted.

use std::{path::Path, process::Command, thread, time::Duration};

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
const TOTAL_STEPS: u32 = 17;

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
/// images), then runs a 17-step structured verification.  Exits
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
    config: std::path::PathBuf,
    /// Resolved forge binary path.
    forge_bin: String,
    /// Overlay file path (for hot-reload testing).
    overlay_path: std::path::PathBuf,
    /// Services blocked by placeholder images (warning only).
    placeholders: Vec<(String, String)>,
}

/// Check all prerequisites and return a context for the verification
/// steps.  Fails with a combined error if config, tools, or the forge
/// binary are missing.  Placeholder images are stored in the context
/// for per-step gating (warning, not fatal).
fn check_prerequisites(forge_config: &Path) -> Result<PrereqContext, Box<dyn std::error::Error>> {
    let (errors, forge_bin) = collect_prereq_errors(forge_config);
    if !errors.is_empty() {
        report_prereq_errors(&errors);
        return Err(format!("{} prerequisite(s) failed", errors.len()).into());
    }

    let config_text = std::fs::read_to_string(forge_config)?;
    let placeholders = detect_placeholder_images(&config_text);
    if !placeholders.is_empty() {
        warn_placeholder_images(&placeholders);
    }

    let forge_bin = forge_bin.unwrap_or_else(|| std::process::abort());

    Ok(PrereqContext {
        config: forge_config.to_path_buf(),
        forge_bin,
        overlay_path: std::path::PathBuf::from(OVERLAY_FILE),
        placeholders,
    })
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

/// Run all 17 verification steps.
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

    // Step 9: Provider gateways reachable.
    step_banner(9, "checking provider gateway reachability");
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

    // Step 10: Edge config applied.
    step_banner(10, "checking edge config applied");
    record_step("edge config applied", results, check_site_stacks);

    // Gate steps 11+ on placeholder images.
    if !ctx.placeholders.is_empty() {
        let reason = format!(
            "placeholder images: {}",
            ctx.placeholders
                .iter()
                .map(|(svc, _)| svc.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        block_remaining(11, &reason, results);
        return;
    }

    // Step 11: Overlay-sync service running.
    step_banner(11, "checking overlay-sync service");
    let sync_ok = record_step("overlay-sync running", results, || {
        check_service_running(&status_json, OVERLAY_SYNC_SERVICE)
    });
    if !sync_ok {
        block_remaining(12, "overlay-sync not running", results);
        return;
    }

    // Step 12: Edge service running — capture container ID.
    step_banner(12, "capturing edge service identity");
    let edge_identity = match check_service_running(&status_json, EDGE_SERVICE) {
        Ok(evidence) => {
            let captured = extract_service_identity(&status_json, EDGE_SERVICE);
            results.push(StepResult::pass("edge service running", evidence));
            captured
        },
        Err(e) => {
            results.push(StepResult::fail("edge service running", e.as_ref()));
            block_remaining(13, "edge not running", results);
            return;
        },
    };

    // Step 13: Inference routed (initial request).
    step_banner(13, "sending inference request");
    let routed_ok = record_step("inference routed", results, check_inference_routed);
    if !routed_ok {
        block_remaining(14, "initial inference failed", results);
        return;
    }

    let reload_count_before = count_overlay_reload_logs(EDGE_CONTAINER).unwrap_or(0);

    // Step 14: Modify overlay (remove one provider).
    step_banner(14, "modifying overlay for hot-reload test");
    let original_overlay = match modify_overlay_for_test(&ctx.overlay_path) {
        Ok((evidence, original)) => {
            results.push(StepResult::pass("overlay modified", evidence));
            Some(original)
        },
        Err(e) => {
            results.push(StepResult::fail("overlay modified", e.as_ref()));
            block_remaining(15, "overlay modification failed", results);
            return;
        },
    };

    // Step 15: Hot-reload observed.
    step_banner(15, "checking hot-reload");
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

    // Step 16: Routing after reload.
    step_banner(16, "sending post-reload inference request");
    record_step("routing after reload", results, check_inference_routed);

    // Restore overlay before step 17.
    if let Some(original) = &original_overlay {
        restore_overlay(&ctx.overlay_path, original);
    }

    // Step 17: Edge container stable (same ID, no restart).
    step_banner(17, "checking edge container stability");
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

/// Labels for all 17 steps, indexed from 0.
const STEP_LABELS: &[&str] = &[
    "prerequisites",
    "forge status",
    "clusters live",
    "provider gateway IPs",
    "swim lb services",
    "swim advertise addr",
    "gridnetwork seeds",
    "overlay metadata",
    "provider gateways reachable",
    "edge config applied",
    "overlay-sync running",
    "edge service running",
    "inference routed",
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
    if ip.is_empty() {
        return Err(format!("{SWIM_LB_SERVICE} on {cluster} has no external IP").into());
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
        if addr.contains("$(POD_IP)") || addr.is_empty() {
            return Err(format!("GRID_SWIM_ADVERTISE_ADDR on {cluster} is '{addr}' (expected LB IP)").into());
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
                "jsonpath={.spec.seeds}",
            ])
            .output()?;
        let seeds_raw = String::from_utf8(output.stdout)?.trim().to_owned();
        let count = parse_seeds_count(&seeds_raw);
        if count < 2 {
            return Err(format!("GridNetwork on {cluster} has {count} seed(s) (expected ≥2)").into());
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

    let required = ["stable_id", "admission_state", "selection_tier", "rank"];
    for (i, c) in candidates.iter().enumerate() {
        for field in &required {
            if c.get(*field).is_none() {
                return Err(format!("candidate[{i}] missing {field}").into());
            }
        }
    }

    Ok(format!(
        "{} candidate(s) with metadata, generated_at present",
        candidates.len()
    ))
}

/// Step 10: Check site-us-east stacks are applied.
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
            "--no-headers",
        ])
        .output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "kubectl get gridnetwork failed: {}",
            safe_truncate_str(stderr.trim(), 120)
        )
        .into());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let count = stdout.lines().count();
    if count == 0 {
        return Err("no GridNetwork resources found on site-us-east".into());
    }
    Ok(format!("{count} GridNetwork resource(s) found"))
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

/// Step 9/12: Send an inference request and verify 200 OK.
fn check_inference_routed() -> Result<String, Box<dyn std::error::Error>> {
    let resp = curl_post_with_auth(EDGE_PORT)?;
    if resp.status != 200 {
        return Err(format!("inference request returned HTTP {}", resp.status).into());
    }
    let model = serde_json::from_str::<serde_json::Value>(&resp.body)
        .ok()
        .and_then(|v| v.get("model").and_then(serde_json::Value::as_str).map(str::to_owned));
    match model {
        Some(m) => Ok(format!("HTTP 200, model={m}")),
        None => Ok("HTTP 200".to_owned()),
    }
}

/// Step 10: Modify the overlay file for hot-reload testing.
///
/// Reads the current overlay, removes one candidate, writes the
/// modified version back.  Returns the evidence string and the
/// original content for later restoration.
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

/// Step 11: Check docker logs for hot-reload evidence and verify
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

/// Step 13: Verify the edge container was not restarted during the
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
    if ip.is_empty() {
        return Err(format!("provider-gateway on {context} has no external IP").into());
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
    let url = format!("http://127.0.0.1:{port}/v1/chat/completions");
    let body = r#"{"model":"sim-model-v1","messages":[{"role":"user","content":"hello"}],"max_tokens":64}"#;
    let output = Command::new("curl")
        .args([
            "-s",
            "-w",
            "\n%{http_code}",
            "--connect-timeout",
            "5",
            "--max-time",
            "15",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-H",
            "Authorization: Bearer test-token",
            "-d",
            body,
            &url,
        ])
        .output()?;
    verify::parse_curl_output(&String::from_utf8(output.stdout)?)
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
    fn step_labels_match_total() {
        assert_eq!(
            STEP_LABELS.len(),
            TOTAL_STEPS as usize,
            "STEP_LABELS length must match TOTAL_STEPS",
        );
    }
}
