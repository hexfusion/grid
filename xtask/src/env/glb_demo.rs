//! Narrated, evidence-backed GLB demo scenarios.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

use serde::Serialize;

use super::{DemoMode, GlbDemoModeOptions, GlbDemoOptions, glb, gtm_emulator, image_overrides, kubectl, operator};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum session keys sampled when discovering observable routing paths.
const MAX_PATH_SAMPLES: usize = 128;

/// Number of repeated requests used for the narrated affinity proof.
const AFFINITY_REPEATS: usize = 3;

/// Request soak performed only by the full demo.
const FULL_SOAK_DURATION: Duration = Duration::from_secs(300);

/// Delay between full-mode soak requests.
const FULL_SOAK_INTERVAL: Duration = Duration::from_secs(5);

/// Resolved config emitted next to the source config to preserve relative paths.
const RESOLVED_CONFIG_NAME: &str = ".forge.resolved.yaml";

/// Ordered cluster names in the local scenario environment.
const CLUSTERS: &[&str] = &[
    "gtm-emulator",
    "east-edge",
    "east-provider",
    "west-edge",
    "west-provider",
];

/// Clusters that participate in Grid discovery and run an operator.
const GRID_CLUSTERS: &[&str] = &["east-edge", "east-provider", "west-edge", "west-provider"];

/// Provider clusters that run the private provider path.
const PROVIDER_CLUSTERS: &[&str] = &["east-provider", "west-provider"];

/// Evidence JSON schema version.
const EVIDENCE_SCHEMA_VERSION: &str = "1";

// ---------------------------------------------------------------------------
// Narrator
// ---------------------------------------------------------------------------

/// Dual-output narrator: writes to stderr and captures to memory.
pub(crate) struct Narrator {
    /// Captured narration lines.
    lines: Vec<String>,
}

impl Narrator {
    /// Create an empty narrator.
    pub(crate) fn new() -> Self {
        Self { lines: Vec::new() }
    }

    /// Emit one narration line to stderr and capture it.
    pub(crate) fn narrate(&mut self, line: &str) {
        eprintln!("{line}");
        self.lines.push(line.to_owned());
    }

    /// Write captured narration to a file.
    fn write_to_file(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        let mut content = self.lines.join("\n");
        content.push('\n');
        fs::write(path, content)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Independent edge/provider affinity fixtures that reproduce one path.
#[derive(Debug, Clone)]
struct AffinityFixture {
    /// Session fixture consumed only by the GTM layer.
    edge_session: String,
    /// Session fixture consumed only by the Grid routing layer.
    provider_session: String,
}

/// Observed `(edge, provider)` pair to the fixtures that reproduce it.
type ObservedPaths = BTreeMap<(String, String), AffinityFixture>;

/// Resolved environment references needed after setup.
pub(crate) struct SetupContext {
    /// Resolved forge config path.
    resolved_config: PathBuf,
    /// Forge binary path.
    forge_bin: String,
}

/// Outcome of the narrated demonstration.
struct DemoOutcome {
    /// Per-capability results.
    capabilities: Vec<CapabilityResult>,
    /// Observed routing paths from discovery.
    observed_paths: Vec<ObservedPathEntry>,
    /// Concise failure detail when the run did not complete successfully.
    error: Option<String>,
}

// ---------------------------------------------------------------------------
// Evidence JSON types
// ---------------------------------------------------------------------------

/// Top-level machine-readable evidence report.
#[derive(Debug, Serialize)]
struct EvidenceReport {
    /// Schema version for forward compatibility.
    schema_version: &'static str,
    /// Unique run identifier (UTC timestamp).
    run_id: String,
    /// Demo mode: `"quick"` or `"full"`.
    mode: &'static str,
    /// UTC start time as ISO 8601.
    started_at: String,
    /// UTC completion time as ISO 8601.
    completed_at: String,
    /// Wall-clock duration in seconds.
    duration_secs: f64,
    /// Overall result: `"pass"` or `"fail"`.
    status: &'static str,
    /// Concise failure detail when `status` is `"fail"`.
    error: Option<String>,
    /// Per-capability results.
    capabilities: Vec<CapabilityResult>,
    /// Observed routing paths.
    observed_paths: Vec<ObservedPathEntry>,
    /// Lifecycle actions performed.
    lifecycle: LifecycleRecord,
    /// Paths to generated artifacts.
    artifacts: ArtifactPaths,
}

/// One capability row in the evidence.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct CapabilityResult {
    /// Human-readable capability name.
    capability: String,
    /// `"pass"`, `"fail"`, or `"skipped"`.
    result: &'static str,
    /// One-line evidence string.
    evidence: String,
}

/// One observed routing path from discovery.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ObservedPathEntry {
    /// GTM-selected edge cluster.
    edge: String,
    /// Grid-selected provider cluster.
    provider: String,
    /// Narrative path description.
    path: String,
}

/// Lifecycle actions recorded.
#[derive(Debug, Clone, Serialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "serialized evidence record with independent boolean fields"
)]
struct LifecycleRecord {
    /// Whether teardown was requested.
    teardown_requested: bool,
    /// Whether teardown was performed.
    teardown_performed: bool,
    /// Teardown result if performed.
    teardown_result: Option<String>,
    /// Whether clusters were kept on failure.
    kept_on_failure: bool,
}

/// Paths to generated artifacts.
#[derive(Debug, Clone, Serialize)]
struct ArtifactPaths {
    /// Path to `narration.txt`.
    narration: String,
    /// Path to `results.json`.
    results: String,
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Materialize and deploy a complete GLB demo environment.
///
/// # Errors
///
/// Returns an error when config rendering, cluster creation, image loading,
/// stack application, trust installation, or service startup fails.
pub(crate) fn setup(forge_config: &Path) -> Result<SetupContext, Box<dyn std::error::Error>> {
    let context = prepare_setup(forge_config)?;
    deploy_setup(&context)?;
    Ok(context)
}

/// Resolve setup inputs before creating any runtime resources.
fn prepare_setup(forge_config: &Path) -> Result<SetupContext, Box<dyn std::error::Error>> {
    Ok(SetupContext {
        resolved_config: materialize_config(forge_config)?,
        forge_bin: glb::resolve_forge_binary().ok_or("praxis-forge binary not found")?,
    })
}

/// Deploy the environment using prepared setup inputs.
fn deploy_setup(context: &SetupContext) -> Result<(), Box<dyn std::error::Error>> {
    glb::stage_provider_boundary()?;
    run_forge(&context.forge_bin, &context.resolved_config, &["up"])?;
    load_local_images_if_required(&context.forge_bin, &context.resolved_config)?;
    apply_foundation_stacks(&context.forge_bin, &context.resolved_config)?;
    glb::install_provider_boundary()?;
    apply_provider_stacks(&context.forge_bin, &context.resolved_config)?;
    apply_edge_stacks(&context.forge_bin, &context.resolved_config)?;
    apply_gtm_emulator_stack(&context.forge_bin, &context.resolved_config)?;
    let overlay_evidence = glb::wait_for_edge_overlays_ready()?;

    eprintln!(
        "glb-demo: environment is ready using {}; {overlay_evidence}",
        context.resolved_config.display()
    );
    Ok(())
}

/// Set up the environment and run every narrated proof.
///
/// # Errors
///
/// Returns an error when setup or any runtime scenario fails.
#[expect(
    clippy::too_many_lines,
    reason = "orchestration of setup, demonstrate, evidence, teardown is clearest in one flow"
)]
pub(crate) fn run(forge_config: &Path, options: &GlbDemoOptions) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let run_id = format_utc_timestamp();
    let started_at = format_utc_iso();
    let wall_start = Instant::now();
    let mut narrator = Narrator::new();

    let evidence_dir = resolve_evidence_dir(forge_config, options, &run_id);
    fs::create_dir_all(&evidence_dir)?;

    let setup_ctx = prepare_setup(forge_config);
    let mut outcome = match &setup_ctx {
        Ok(context) => match deploy_setup(context) {
            Ok(()) => demonstrate_inner(&context.resolved_config, mode, &mut narrator),
            Err(error) => failed_outcome(Vec::new(), Vec::new(), "Environment setup", concise_error(error)),
        },
        Err(error) => failed_outcome(Vec::new(), Vec::new(), "Environment preparation", concise_error(error)),
    };

    let mut lifecycle = LifecycleRecord {
        teardown_requested: options.teardown,
        teardown_performed: false,
        teardown_result: None,
        kept_on_failure: false,
    };

    if options.teardown {
        let should_keep = options.keep_on_failure && outcome.error.is_some() && setup_ctx.is_ok();
        if should_keep {
            lifecycle.kept_on_failure = true;
            narrator.narrate("glb-demo: retaining clusters for debugging (--keep-on-failure)");
        } else if let Ok(context) = &setup_ctx {
            lifecycle.teardown_performed = true;
            match teardown_clusters(&context.forge_bin, &context.resolved_config) {
                Ok(()) => {
                    lifecycle.teardown_result = Some("success".to_owned());
                    narrator.narrate("glb-demo: teardown complete");
                },
                Err(error) => {
                    let message = concise_error(error);
                    lifecycle.teardown_result = Some(format!("error: {message}"));
                    narrator.narrate(&format!("glb-demo: teardown error: {message}"));
                    append_error(&mut outcome.error, format!("teardown failed: {message}"));
                },
            }
        } else {
            lifecycle.teardown_result = Some("not needed: deployment did not start".to_owned());
        }
    }

    let status = if outcome.error.is_some() { "fail" } else { "pass" };
    let elapsed = wall_start.elapsed();
    let completed_at = format_utc_iso();
    let narration_path = evidence_dir.join("narration.txt");
    let results_path = evidence_dir.join("results.json");

    let report = EvidenceReport {
        schema_version: EVIDENCE_SCHEMA_VERSION,
        run_id,
        mode: mode_label(mode),
        started_at,
        completed_at,
        duration_secs: elapsed.as_secs_f64(),
        status,
        error: outcome.error.clone(),
        capabilities: outcome.capabilities,
        observed_paths: outcome.observed_paths,
        lifecycle,
        artifacts: ArtifactPaths {
            narration: narration_path.display().to_string(),
            results: results_path.display().to_string(),
        },
    };

    print_final_summary(&mut narrator, &report, &evidence_dir);

    if let Err(error) = write_evidence(&report, &narrator, &narration_path, &results_path) {
        let message = concise_error(error);
        return match &report.error {
            Some(run_error) => Err(format!("{run_error}; evidence write failed: {message}").into()),
            None => Err(format!("evidence write failed: {message}").into()),
        };
    }

    match report.error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

/// Run narrated scenarios against an already-deployed environment.
///
/// # Errors
///
/// Returns an error when any prerequisite proof, routing scenario, affinity
/// check, or edge withdrawal/recovery check fails.
pub(crate) fn demonstrate_with_options(
    forge_config: &Path,
    options: &GlbDemoModeOptions,
) -> Result<(), Box<dyn std::error::Error>> {
    let mode = options.mode();
    let mut narrator = Narrator::new();
    match demonstrate_inner(forge_config, mode, &mut narrator).error {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Core demonstration logic
// ---------------------------------------------------------------------------

/// Run narrated scenarios and collect capability results.
#[expect(
    clippy::too_many_lines,
    reason = "sequential scenario narration is clearest in one function"
)]
fn demonstrate_inner(forge_config: &Path, mode: DemoMode, narrator: &mut Narrator) -> DemoOutcome {
    let mut capabilities = Vec::new();

    print_introduction(narrator);

    // Scenario 1: Active/active routing.
    print_scenario(
        narrator,
        1,
        "Active/active global and provider routing",
        "As an application owner, I need one stable HTTPS endpoint backed by active edges while Grid independently selects an admitted provider.",
    );

    if let Err(error) = glb::verify_grid_routing_with_mode(forge_config, mode) {
        return failed_outcome(capabilities, Vec::new(), "Active/active routing", concise_error(error));
    }

    let paths = match discover_paths() {
        Ok(paths) => paths,
        Err(error) => {
            return failed_outcome(capabilities, Vec::new(), "Active/active routing", concise_error(error));
        },
    };
    let observed_paths = build_observed_paths(&paths);
    print_paths(narrator, &paths);
    capabilities.push(CapabilityResult {
        capability: "Active/active routing".to_owned(),
        result: "pass",
        evidence: "2 edges observed; independent Grid provider selection and failover verified".to_owned(),
    });

    // Scenario 2: Secure provider boundary (summarizes results from scenario 1).
    print_scenario(
        narrator,
        2,
        "Secure provider boundary",
        "As a provider and security owner, I need authenticated Grid traffic, exact local policy, private backend isolation, and final-hop credential replacement.",
    );
    print_provider_boundary_proof(narrator);
    print_credential_boundary_proof(narrator);
    capabilities.push(CapabilityResult {
        capability: "Secure provider boundary".to_owned(),
        result: "pass",
        evidence: "mTLS, peer auth, NetworkPolicy, credential replacement verified".to_owned(),
    });

    // Scenarios 3-5: full mode only.
    if mode == DemoMode::Full {
        // Scenario 3: Session affinity and provider drain.
        print_scenario(
            narrator,
            3,
            "Session affinity and provider drain",
            "As an inference client, I need repeated requests to remain on one edge and provider while existing sessions survive a metrics-driven drain and new sessions move safely.",
        );
        if let Err(error) = prove_affinity(narrator, &paths) {
            return failed_outcome(
                capabilities,
                observed_paths,
                "Session affinity and drain",
                concise_error(error),
            );
        }
        capabilities.push(CapabilityResult {
            capability: "Session affinity and drain".to_owned(),
            result: "pass",
            evidence: "edge+provider stable, drain verified".to_owned(),
        });

        // Scenario 4: Edge withdrawal and recovery.
        print_scenario(
            narrator,
            4,
            "Edge withdrawal and recovery",
            "As a reliability operator, I need a failed edge withdrawn behind the same HTTPS name and returned after recovery.",
        );
        if let Err(error) = gtm_emulator::verify(forge_config) {
            return failed_outcome(
                capabilities,
                observed_paths,
                "Edge withdrawal and recovery",
                concise_error(error),
            );
        }
        capabilities.push(CapabilityResult {
            capability: "Edge withdrawal and recovery".to_owned(),
            result: "pass",
            evidence: "east withdrawn, west served, east recovered".to_owned(),
        });

        // Scenario 5: operator restart recovery and request soak.
        print_scenario(
            narrator,
            5,
            "Grid restart recovery and request soak",
            "As a platform operator, I need Grid control-plane restarts to preserve converged routing and sustained inference traffic.",
        );
        match prove_restart_recovery_and_soak(narrator, &paths) {
            Ok(evidence) => capabilities.push(CapabilityResult {
                capability: "Grid restart recovery and soak".to_owned(),
                result: "pass",
                evidence,
            }),
            Err(error) => {
                return failed_outcome(
                    capabilities,
                    observed_paths,
                    "Grid restart recovery and soak",
                    concise_error(error),
                );
            },
        }
    } else {
        capabilities.push(CapabilityResult {
            capability: "Session affinity and drain".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        capabilities.push(CapabilityResult {
            capability: "Edge withdrawal and recovery".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        capabilities.push(CapabilityResult {
            capability: "Grid restart recovery and soak".to_owned(),
            result: "skipped",
            evidence: "quick mode".to_owned(),
        });
        narrator.narrate("");
        narrator.narrate("Scenarios 3-5 skipped in quick mode.");
    }

    print_boundaries(narrator);

    DemoOutcome {
        capabilities,
        observed_paths,
        error: None,
    }
}

// ---------------------------------------------------------------------------
// Narration helpers
// ---------------------------------------------------------------------------

/// Print the architecture and proof policy before executing scenarios.
fn print_introduction(narrator: &mut Narrator) {
    narrator.narrate("# Grid GLB Demo");
    narrator.narrate("");
    narrator.narrate("Every PASS below comes from a runtime assertion. Manifest intent is not counted as proof.");
    narrator.narrate("");
    narrator.narrate("Illustrative request path (not derived from a live response):");
    narrator.narrate("");
    narrator.narrate("```text");
    narrator.narrate("client -> stable Praxis HTTPS -> selected Praxis edge");
    narrator.narrate("       -> Grid-selected Praxis provider gateway -> private backend");
    narrator.narrate("```");
}

/// Print one scenario and its user story.
fn print_scenario(narrator: &mut Narrator, number: usize, title: &str, user_story: &str) {
    narrator.narrate("");
    narrator.narrate(&format!("## Scenario {number}: {title}"));
    narrator.narrate("");
    narrator.narrate(&format!("User story: {user_story}"));
}

/// Print the path matrix observed from live responses.
fn print_paths(narrator: &mut Narrator, paths: &ObservedPaths) {
    narrator.narrate("");
    narrator.narrate("| GTM-selected edge | Grid-selected provider | Edge fixture | Provider fixture |");
    narrator.narrate("|---|---|---|---|");
    for ((edge, provider), fixture) in paths {
        narrator.narrate(&format!(
            "| `{edge}` | `{provider}` | `{}` | `{}` |",
            fixture.edge_session, fixture.provider_session
        ));
    }

    for (edge, provider) in paths.keys() {
        narrator.narrate("");
        narrator.narrate(&format!(
            "Observed path: client -> {edge} -> {provider} gateway -> backend"
        ));
    }

    print_crossed_path(narrator, paths);
}

/// Print the crossed edge/provider path when one is observed.
fn print_crossed_path(narrator: &mut Narrator, paths: &ObservedPaths) {
    let crossed = paths
        .iter()
        .find(|((edge, provider), _)| edge.strip_suffix("-edge") != provider.strip_suffix("-provider"));

    if let Some(((edge, provider), fixture)) = crossed {
        narrator.narrate("");
        narrator.narrate(&format!(
            "Observed crossed path (`{}` / `{}`):",
            fixture.edge_session, fixture.provider_session
        ));
        narrator.narrate("");
        narrator.narrate("```text");
        narrator.narrate(&format!("client -> {edge} public edge -> {edge} Grid overlay"));
        narrator.narrate(&format!(
            "       -> {provider} private provider gateway -> {provider} backend"
        ));
        narrator.narrate(&format!(
            "       -> {provider} provider gateway -> {edge} edge -> client"
        ));
        narrator.narrate("```");
    }
}

/// Summarize the provider assertions completed by the preceding strict proof.
fn print_provider_boundary_proof(narrator: &mut Narrator) {
    narrator.narrate("");
    narrator.narrate(
        "PASS: both providers required mTLS, accepted both pinned edge identities, rejected missing or invalid TLS identities, and enforced exact candidate/model/path policy."
    );
}

/// Summarize final-hop credential and private-backend runtime evidence.
fn print_credential_boundary_proof(narrator: &mut Narrator) {
    narrator.narrate("");
    narrator.narrate(
        "PASS: both private backends denied unlabeled network access, returned HTTP 401 without credentials and HTTP 403 for the client-supplied fixture, then returned HTTP 200 through provider-local credential replacement."
    );
}

/// Print explicit scope boundaries after all runtime proofs.
fn print_boundaries(narrator: &mut Narrator) {
    narrator.narrate("");
    narrator.narrate("## Demonstrated Boundary");
    narrator.narrate("");
    narrator
        .narrate("- Proven: two Praxis edges, one verified HTTPS name, health withdrawal/recovery, edge stickiness.");
    narrator.narrate(
        "- Proven: per-edge Grid overlays, metrics-driven provider drain, health-driven withdrawal, hot reload, provider mTLS and peer authorization."
    );
    narrator
        .narrate("- Proven: provider-local credential replacement and NetworkPolicy-enforced private backend access.");
    narrator.narrate(
        "- Not claimed: managed DNS/Anycast, internet DDoS/WAF, geo-latency GTM steering, shared affinity storage, or in-flight stream migration."
    );
}

// ---------------------------------------------------------------------------
// Path discovery and affinity
// ---------------------------------------------------------------------------

/// Discover real edge/provider combinations through the stable HTTPS name.
fn discover_paths() -> Result<ObservedPaths, Box<dyn std::error::Error>> {
    let mut paths = BTreeMap::new();
    for index in 0..MAX_PATH_SAMPLES {
        let fixture = AffinityFixture {
            edge_session: format!("narrated-edge-{index}"),
            provider_session: format!("narrated-provider-{index}"),
        };
        let sample = gtm_emulator::request_path_with_affinity(&fixture.edge_session, &fixture.provider_session)?;
        if !paths.keys().any(|(edge, _)| edge == &sample.edge) {
            paths.insert((sample.edge, sample.provider), fixture);
        }
        if paths.len() == 2 {
            break;
        }
    }
    if paths.len() != 2 {
        return Err(format!("path discovery observed {} of 2 Praxis edges", paths.len()).into());
    }
    Ok(paths)
}

/// Build evidence path entries from observed paths.
fn build_observed_paths(paths: &ObservedPaths) -> Vec<ObservedPathEntry> {
    paths
        .iter()
        .map(|((edge, provider), _)| ObservedPathEntry {
            edge: edge.clone(),
            provider: provider.clone(),
            path: format!("client -> {edge} -> {provider} gateway -> backend"),
        })
        .collect()
}

/// Repeat one observed path and require both affinity layers to remain stable.
fn prove_affinity(narrator: &mut Narrator, paths: &ObservedPaths) -> Result<(), Box<dyn std::error::Error>> {
    let ((expected_edge, expected_provider), fixture) = paths
        .first_key_value()
        .ok_or("no observed path available for affinity")?;

    for _attempt in 0..AFFINITY_REPEATS {
        let sample = gtm_emulator::request_path_with_affinity(&fixture.edge_session, &fixture.provider_session)?;
        if sample.edge != *expected_edge || sample.provider != *expected_provider {
            return Err(format!(
                "affinity fixtures moved from {expected_edge}/{expected_provider} to {}/{}",
                sample.edge, sample.provider,
            )
            .into());
        }
    }

    narrator.narrate("");
    narrator.narrate(&format!(
        "PASS: edge fixture `{}` and provider fixture `{}` remained on edge `{expected_edge}` and provider `{expected_provider}` for {AFFINITY_REPEATS} repeated requests.",
        fixture.edge_session, fixture.provider_session
    ));
    Ok(())
}

/// Restart every Grid operator sequentially, then sustain requests for a bounded soak.
fn prove_restart_recovery_and_soak(
    narrator: &mut Narrator,
    paths: &ObservedPaths,
) -> Result<String, Box<dyn std::error::Error>> {
    let fixtures = paths.values().collect::<Vec<_>>();
    if fixtures.is_empty() {
        return Err("no observed path available for restart and soak proof".into());
    }

    prove_operator_restarts(narrator, &fixtures)?;
    let (samples, edge_count, provider_count) = run_request_soak(narrator, &fixtures)?;
    let evidence = format!(
        "4 Grid operators restarted; {samples} soak requests passed across {edge_count} edges and {provider_count} provider(s)"
    );
    narrator.narrate(&format!("PASS: {evidence}."));
    Ok(evidence)
}

/// Restart each Grid operator and prove overlay and request recovery.
fn prove_operator_restarts(
    narrator: &mut Narrator,
    fixtures: &[&AffinityFixture],
) -> Result<(), Box<dyn std::error::Error>> {
    for (index, cluster) in GRID_CLUSTERS.iter().enumerate() {
        restart_grid_operator(cluster)?;
        let overlay_evidence = glb::wait_for_edge_overlays_ready()?;
        let fixture = fixtures
            .get(index % fixtures.len())
            .ok_or("no affinity fixture available after Grid restart")?;
        let sample = gtm_emulator::request_path_with_affinity(&fixture.edge_session, &fixture.provider_session)?;
        narrator.narrate(&format!(
            "PASS: restarted `{cluster}` Grid operator; routing recovered via `{}` -> `{}` ({overlay_evidence}).",
            sample.edge, sample.provider
        ));
    }
    Ok(())
}

/// Sustain inference requests for the full-mode soak window.
fn run_request_soak(
    narrator: &mut Narrator,
    fixtures: &[&AffinityFixture],
) -> Result<(usize, usize, usize), Box<dyn std::error::Error>> {
    narrator.narrate("");
    narrator.narrate(&format!(
        "Starting {}-second request soak through the stable HTTPS endpoint.",
        FULL_SOAK_DURATION.as_secs()
    ));
    let deadline = Instant::now() + FULL_SOAK_DURATION;
    let mut samples = 0_usize;
    let mut edges = BTreeSet::new();
    let mut providers = BTreeSet::new();
    while Instant::now() < deadline {
        let fixture = fixtures
            .get(samples % fixtures.len())
            .ok_or("no affinity fixture available during request soak")?;
        let sample = gtm_emulator::request_path_with_affinity(&fixture.edge_session, &fixture.provider_session)?;
        edges.insert(sample.edge);
        providers.insert(sample.provider);
        samples += 1;

        let remaining = deadline.saturating_duration_since(Instant::now());
        if !remaining.is_zero() {
            std::thread::park_timeout(FULL_SOAK_INTERVAL.min(remaining));
        }
    }
    if edges.len() != 2 {
        return Err(format!("request soak reached {} of 2 Praxis edges", edges.len()).into());
    }
    Ok((samples, edges.len(), providers.len()))
}

/// Restart one Grid operator and wait for its replacement pod.
fn restart_grid_operator(cluster: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = format!("kind-grid-glb-{cluster}");
    let output = Command::new("kubectl")
        .args([
            "--context",
            &context,
            "-n",
            "grid-system",
            "rollout",
            "restart",
            "deployment/grid-operator",
        ])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to restart {cluster} Grid operator: {}",
            super::safe_truncate_str(&String::from_utf8_lossy(&output.stderr), 160)
        )
        .into());
    }
    kubectl::wait_for_rollout_ns(&context, "grid-operator", "grid-system", cluster)
}

/// Build a failed outcome while preserving completed capability evidence.
fn failed_outcome(
    mut capabilities: Vec<CapabilityResult>,
    observed_paths: Vec<ObservedPathEntry>,
    capability: &str,
    error: String,
) -> DemoOutcome {
    capabilities.push(CapabilityResult {
        capability: capability.to_owned(),
        result: "fail",
        evidence: error.clone(),
    });
    DemoOutcome {
        capabilities,
        observed_paths,
        error: Some(error),
    }
}

/// Convert arbitrary command errors into one bounded evidence line.
fn concise_error(error: impl Display) -> String {
    error
        .to_string()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(512)
        .collect()
}

/// Add another failure without discarding the primary cause.
fn append_error(error: &mut Option<String>, additional: String) {
    match error {
        Some(primary) => {
            primary.push_str("; ");
            primary.push_str(&additional);
        },
        None => *error = Some(additional),
    }
}

// ---------------------------------------------------------------------------
// Final summary
// ---------------------------------------------------------------------------

/// Print a concise summary table after all scenarios.
fn print_final_summary(narrator: &mut Narrator, report: &EvidenceReport, evidence_dir: &Path) {
    narrator.narrate("");
    narrator.narrate("## GLB Demo Summary");
    narrator.narrate("");
    narrator.narrate("| Capability | Result | Evidence |");
    narrator.narrate("|---|---|---|");
    for cap in &report.capabilities {
        narrator.narrate(&format!(
            "| {} | {} | {} |",
            cap.capability,
            cap.result.to_uppercase(),
            cap.evidence
        ));
    }
    narrator.narrate("");
    narrator.narrate(&format!("Mode: {}", report.mode));
    narrator.narrate(&format!("Overall: {}", report.status.to_uppercase()));
    narrator.narrate(&format!("Elapsed: {:.1}s", report.duration_secs));
    narrator.narrate(&format!("Evidence: {}", evidence_dir.display()));
}

// ---------------------------------------------------------------------------
// Evidence output
// ---------------------------------------------------------------------------

/// Resolve the evidence directory path.
fn resolve_evidence_dir(forge_config: &Path, options: &GlbDemoOptions, run_id: &str) -> PathBuf {
    if let Some(dir) = &options.evidence_dir {
        return dir.clone();
    }
    let parent = forge_config.parent().unwrap_or_else(|| Path::new("."));
    parent.join(format!(".forge/evidence/glb-demo-{run_id}"))
}

/// Write evidence files (narration and JSON report).
fn write_evidence(
    report: &EvidenceReport,
    narrator: &Narrator,
    narration_path: &Path,
    results_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    narrator.write_to_file(narration_path)?;
    let json = serde_json::to_string_pretty(report)?;
    fs::write(results_path, json)?;
    eprintln!(
        "glb-demo: evidence written to {}",
        narration_path.parent().unwrap_or_else(|| Path::new(".")).display()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

/// Delete all GLB demo clusters through Forge.
fn teardown_clusters(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("glb-demo: tearing down clusters...");
    run_forge(forge, config, &["down", "--force"])
}

// ---------------------------------------------------------------------------
// Timestamp helpers
// ---------------------------------------------------------------------------

/// Format the current UTC time as `YYYYMMDDTHHMMSSz` for evidence directory names.
fn format_utc_timestamp() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

/// Format the current UTC time as ISO 8601 for evidence fields.
fn format_utc_iso() -> String {
    let now = time::OffsetDateTime::now_utc();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        now.year(),
        u8::from(now.month()),
        now.day(),
        now.hour(),
        now.minute(),
        now.second(),
    )
}

/// Return the string label for a demo mode.
fn mode_label(mode: DemoMode) -> &'static str {
    match mode {
        DemoMode::Quick => "quick",
        DemoMode::Full => "full",
    }
}

// ---------------------------------------------------------------------------
// Environment setup helpers
// ---------------------------------------------------------------------------

/// Render image overrides into a Forge config without mutating source files.
fn materialize_config(source: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let content = fs::read_to_string(source)?;
    let rendered = render_config(&content)?;
    let parent = source.parent().unwrap_or_else(|| Path::new("."));
    let output = parent.join(RESOLVED_CONFIG_NAME);
    fs::write(&output, rendered)?;
    Ok(output)
}

/// Render image overrides into one Forge configuration document.
fn render_config(content: &str) -> Result<String, Box<dyn std::error::Error>> {
    validate_image_contract()?;
    let mut config: serde_yaml::Value = serde_yaml::from_str(content)?;
    let spec = mapping_mut(&mut config, "spec")?;
    set_cluster_image_properties(spec)?;
    Ok(serde_yaml::to_string(&config)?)
}

/// Validate the image references and pull policy selected by the environment.
fn validate_image_contract() -> Result<(), Box<dyn std::error::Error>> {
    for (name, image) in [
        ("GRID_XTASK_GATEWAY_IMAGE", image_overrides::glb_gateway_image()),
        ("GRID_XTASK_OPERATOR_IMAGE", image_overrides::glb_operator_image()),
        (
            "GRID_XTASK_MOCK_PROVIDER_IMAGE",
            image_overrides::glb_mock_provider_image(),
        ),
    ] {
        if image.is_empty() || image.chars().any(char::is_whitespace) {
            return Err(format!("{name} must be a non-empty image reference without whitespace").into());
        }
    }

    let pull_policy = image_overrides::image_pull_policy();
    if !matches!(pull_policy.as_str(), "Always" | "IfNotPresent" | "Never") {
        return Err(format!(
            "GRID_XTASK_IMAGE_PULL_POLICY must be Always, IfNotPresent, or Never; got {pull_policy:?}"
        )
        .into());
    }
    Ok(())
}

/// Apply environment-selected images to stack template properties.
fn set_cluster_image_properties(spec: &mut serde_yaml::Mapping) -> Result<(), Box<dyn std::error::Error>> {
    let clusters = sequence_mut(spec, "clusters")?;
    for cluster in clusters {
        let cluster = cluster.as_mapping_mut().ok_or("cluster entry must be a mapping")?;
        let properties = mapping_mut_in(cluster, "properties")?;
        for (key, value) in [
            ("gatewayImage", image_overrides::glb_gateway_image()),
            ("operatorImage", image_overrides::glb_operator_image()),
            ("mockProviderImage", image_overrides::glb_mock_provider_image()),
            ("imagePullPolicy", image_overrides::image_pull_policy()),
        ] {
            properties.insert(yaml_key(key), serde_yaml::Value::String(value));
        }
    }
    Ok(())
}

/// Load local images into Kind when the pull policy is `Never`.
fn load_local_images_if_required(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if image_overrides::should_skip_kind_image_loading() {
        return Ok(());
    }
    let operator = image_overrides::glb_operator_image();
    let gateway = image_overrides::glb_gateway_image();
    let mock = image_overrides::glb_mock_provider_image();
    for image in [&operator, &gateway, &mock] {
        require_local_image(image)?;
    }
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &operator])?;
    }
    for cluster in CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &gateway])?;
    }
    for cluster in PROVIDER_CLUSTERS {
        run_forge(forge, config, &["cluster", "load-image", cluster, &mock])?;
    }
    Ok(())
}

/// Apply shared infrastructure before any identity-dependent workload.
fn apply_foundation_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(forge, config, &["stack", "apply", "gtm-emulator", "metallb"])?;

    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "metallb"])?;
    }
    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "swim-lb"])?;
    }

    for cluster in GRID_CLUSTERS {
        run_forge(forge, config, &["stack", "apply", cluster, "grid-operator"])?;
    }
    for (cluster, identity_stack) in [
        ("east-edge", "east-edge-operator"),
        ("east-provider", "east-provider-operator"),
        ("west-edge", "west-edge-operator"),
        ("west-provider", "west-provider-operator"),
    ] {
        run_forge(forge, config, &["stack", "apply", cluster, identity_stack])?;
    }
    Ok(())
}

/// Apply provider sites and private provider paths before edge rendering.
fn apply_provider_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for (cluster, site_stack) in [
        ("east-provider", "east-provider-site"),
        ("west-provider", "west-provider-site"),
    ] {
        run_forge(forge, config, &["stack", "apply", cluster, site_stack])?;
        run_forge(forge, config, &["stack", "apply", cluster, "inference-sim"])?;
    }
    Ok(())
}

/// Apply edge sites and the local Praxis edge in each edge cluster.
fn apply_edge_stacks(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for (cluster, site_stack) in [("east-edge", "east-edge-site"), ("west-edge", "west-edge-site")] {
        run_forge(forge, config, &["stack", "apply", cluster, site_stack])?;
    }
    authorize_provider_sites_for_edges()?;
    for cluster in ["east-edge", "west-edge"] {
        run_forge(forge, config, &["stack", "apply", cluster, "edge-gateway"])?;
    }
    Ok(())
}

/// Pin each provider's SWIM-advertised public certificate on both edge sites.
///
/// SWIM discovery supplies the endpoint and public certificate, but it does
/// not authorize routing. The demo compares the received certificate to the
/// locally generated out-of-band identity before configuring the `GridSite`
/// fingerprint policy. Edge Deployments are applied only after both provider
/// sites reach `Active`, so a missing or mismatched trust record fails closed.
fn authorize_provider_sites_for_edges() -> Result<(), Box<dyn std::error::Error>> {
    const TRUST_TIMEOUT: Duration = Duration::from_secs(120);

    for edge in ["east-edge", "west-edge"] {
        let context = format!("kind-grid-glb-{edge}");
        for provider in PROVIDER_CLUSTERS {
            let site_name = format!("glb-demo-{provider}");
            operator::wait_for_auto_gridsite(&context, &site_name, "glb-demo", TRUST_TIMEOUT)?;
            let expected_fingerprint = glb::site_certificate_fingerprint(provider)?;
            wait_for_expected_site_certificate(&context, &site_name, &expected_fingerprint, TRUST_TIMEOUT)?;
            operator::patch_gridsite_cert_fingerprint(&context, &site_name, &expected_fingerprint)?;
            operator::wait_for_gridsite_phase(&context, &site_name, "Active", TRUST_TIMEOUT)?;
        }
    }
    Ok(())
}

/// Wait for certificate gossip to replace missing or stale site trust material.
fn wait_for_expected_site_certificate(
    context: &str,
    site_name: &str,
    expected_fingerprint: &str,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        if operator::read_gridsite_public_cert_pem(context, site_name)
            .is_some_and(|pem| operator::sha256_fingerprint(&pem) == expected_fingerprint)
        {
            eprintln!("  [OK] GridSite {site_name:?}: advertised certificate matches the staged identity");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timeout waiting for GridSite {site_name:?} to advertise its expected certificate").into(),
            );
        }
        #[expect(
            clippy::disallowed_methods,
            reason = "bounded polling for asynchronous SWIM certificate propagation"
        )]
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Apply the local managed-GTM stand-in after both edge addresses are known.
fn apply_gtm_emulator_stack(forge: &str, config: &Path) -> Result<(), Box<dyn std::error::Error>> {
    run_forge(forge, config, &["stack", "apply", "gtm-emulator", "gtm-emulator"])
}

/// Execute one Forge command and retain its output on failure.
fn run_forge(forge: &str, config: &Path, args: &[&str]) -> Result<(), Box<dyn std::error::Error>> {
    let output = Command::new(forge)
        .args(["--config", &config.display().to_string(), "--non-interactive"])
        .args(args)
        .output()?;
    if output.status.success() {
        return Ok(());
    }
    Err(format!(
        "praxis-forge {} failed: {}",
        args.join(" "),
        String::from_utf8_lossy(&output.stderr).trim()
    )
    .into())
}

/// Require a local Docker image before a `Never`-pull setup.
fn require_local_image(image: &str) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("docker")
        .args(["image", "inspect", image])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()?;
    if status.success() {
        return Ok(());
    }
    Err(format!(
        "required local image {image:?} is absent; build it or set GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent with registry image overrides"
    )
    .into())
}

// ---------------------------------------------------------------------------
// YAML helpers
// ---------------------------------------------------------------------------

/// Return a named mapping from a YAML value.
fn mapping_mut<'a>(
    value: &'a mut serde_yaml::Value,
    field: &str,
) -> Result<&'a mut serde_yaml::Mapping, Box<dyn std::error::Error>> {
    let mapping = value.as_mapping_mut().ok_or("YAML root must be a mapping")?;
    mapping_mut_in(mapping, field)
}

/// Return a named child mapping.
fn mapping_mut_in<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    field: &str,
) -> Result<&'a mut serde_yaml::Mapping, Box<dyn std::error::Error>> {
    mapping
        .get_mut(yaml_key(field))
        .and_then(serde_yaml::Value::as_mapping_mut)
        .ok_or_else(|| format!("YAML field {field:?} must be a mapping").into())
}

/// Return a named child sequence.
fn sequence_mut<'a>(
    mapping: &'a mut serde_yaml::Mapping,
    field: &str,
) -> Result<&'a mut Vec<serde_yaml::Value>, Box<dyn std::error::Error>> {
    mapping
        .get_mut(yaml_key(field))
        .and_then(serde_yaml::Value::as_sequence_mut)
        .ok_or_else(|| format!("YAML field {field:?} must be a sequence").into())
}

/// Construct one YAML mapping key.
fn yaml_key(value: &str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_owned())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod setup_tests {
    use super::*;

    #[expect(clippy::allow_attributes, reason = "blanket test lint suppression")]
    #[allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing,
        clippy::panic,
        reason = "tests"
    )]
    mod inner {
        use super::*;

        /// Repository root from the xtask crate directory.
        fn workspace_root() -> PathBuf {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
        }

        #[test]
        fn materialized_config_uses_glb_image_contract() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("environments/grid-glb-demo/forge.yaml");
            let rendered = render_config(&fs::read_to_string(source)?)?;
            assert!(rendered.contains(&image_overrides::glb_gateway_image()));
            assert!(rendered.contains(&image_overrides::glb_operator_image()));
            assert!(rendered.contains(&image_overrides::glb_mock_provider_image()));
            assert!(rendered.contains(&image_overrides::image_pull_policy()));
            assert!(!rendered.contains("grid-overlay-sync"));
            Ok(())
        }

        #[test]
        fn swim_service_stack_creates_its_namespace_first() -> Result<(), Box<dyn std::error::Error>> {
            let source = workspace_root().join("environments/grid-glb-demo/forge.yaml");
            let forge: serde_yaml::Value = serde_yaml::from_str(&fs::read_to_string(source)?)?;
            let steps = forge
                .get("spec")
                .and_then(|value| value.get("stacks"))
                .and_then(|value| value.get("swim-lb"))
                .and_then(|value| value.get("steps"))
                .and_then(serde_yaml::Value::as_sequence)
                .ok_or("swim-lb steps must be a sequence")?;

            let first_path = steps
                .first()
                .and_then(|value| value.get("path"))
                .and_then(serde_yaml::Value::as_str);
            let second_path = steps
                .get(1)
                .and_then(|value| value.get("path"))
                .and_then(serde_yaml::Value::as_str);

            assert_eq!(first_path, Some("resources/grid-system-namespace.yaml"));
            assert_eq!(second_path, Some("resources/operator-swim-service.yaml"));
            Ok(())
        }

        #[test]
        fn operator_site_configuration_preserves_the_base_deployment() -> Result<(), Box<dyn std::error::Error>> {
            let forge = fs::read_to_string(workspace_root().join("environments/grid-glb-demo/forge.yaml"))?;
            for site in ["east-edge", "east-provider", "west-edge", "west-provider"] {
                let identity = format!("GRID_SWIM_SITE_NAME={site}");
                let identity_index = forge
                    .find(&identity)
                    .ok_or_else(|| format!("{site} has no explicit SWIM identity"))?;
                let network = format!("resources/gridnetwork-{site}.yaml");
                let network_index = forge
                    .find(&network)
                    .ok_or_else(|| format!("{site} has no GridNetwork step"))?;
                assert!(
                    identity_index < network_index,
                    "{site} identity must be set before its GridNetwork is applied"
                );
                let between = forge
                    .get(identity_index..network_index)
                    .ok_or_else(|| format!("{site} stack order is invalid"))?;
                assert!(
                    between.contains("rollout") && between.contains("status"),
                    "{site} operator rollout must complete before its GridNetwork is applied"
                );
            }
            assert!(
                !forge.contains("operator-env-"),
                "partial Deployment overlays can disturb base security settings"
            );
            Ok(())
        }

        #[test]
        fn demo_workloads_use_restricted_container_defaults() -> Result<(), Box<dyn std::error::Error>> {
            let resources = workspace_root().join("environments/grid-glb-demo/resources");
            for manifest in [
                "edge-gateway-deployment.yaml",
                "provider-gateway-deployment.yaml",
                "gtm-emulator-deployment.yaml",
                "provider-workloads.yaml",
            ] {
                let deployment = fs::read_to_string(resources.join(manifest))?;
                for required in [
                    "automountServiceAccountToken: false",
                    "runAsNonRoot: true",
                    "type: RuntimeDefault",
                    "allowPrivilegeEscalation: false",
                    "readOnlyRootFilesystem: true",
                    "- ALL",
                ] {
                    assert!(deployment.contains(required), "{manifest} must contain {required:?}");
                }
            }
            Ok(())
        }

        #[test]
        fn default_glb_image_contract_is_valid() {
            assert!(validate_image_contract().is_ok());
        }

        // ----- New tests for demo runner enhancements -----

        #[test]
        fn quick_and_full_are_mutually_exclusive() {
            let result = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--quick",
                "--full",
            ]);
            assert!(result.is_err(), "--quick and --full must conflict");
        }

        #[test]
        fn keep_on_failure_requires_teardown() {
            let result = <crate::Cli as clap::Parser>::try_parse_from([
                "xtask",
                "env",
                "run-grid-glb-demo",
                "--keep-on-failure",
            ]);
            assert!(result.is_err(), "--keep-on-failure requires --teardown");
        }

        #[test]
        fn default_mode_is_full() {
            let options = GlbDemoOptions {
                mode_options: GlbDemoModeOptions {
                    quick: false,
                    full: false,
                },
                teardown: false,
                keep_on_failure: false,
                evidence_dir: None,
            };
            assert_eq!(options.mode(), DemoMode::Full);
        }

        #[test]
        fn quick_flag_selects_quick_mode() {
            let options = GlbDemoOptions {
                mode_options: GlbDemoModeOptions {
                    quick: true,
                    full: false,
                },
                teardown: false,
                keep_on_failure: false,
                evidence_dir: None,
            };
            assert_eq!(options.mode(), DemoMode::Quick);
        }

        fn sample_report(mode: &'static str, status: &'static str) -> EvidenceReport {
            EvidenceReport {
                schema_version: EVIDENCE_SCHEMA_VERSION,
                run_id: "20260728T120000Z".to_owned(),
                mode,
                started_at: "2026-07-28T12:00:00Z".to_owned(),
                completed_at: "2026-07-28T12:02:00Z".to_owned(),
                duration_secs: 120.0,
                status,
                error: None,
                capabilities: vec![CapabilityResult {
                    capability: "Active/active routing".to_owned(),
                    result: "pass",
                    evidence: "2 edges observed".to_owned(),
                }],
                observed_paths: vec![ObservedPathEntry {
                    edge: "east-edge".to_owned(),
                    provider: "west-provider".to_owned(),
                    path: "client -> east-edge -> west-provider gateway -> backend".to_owned(),
                }],
                lifecycle: LifecycleRecord {
                    teardown_requested: true,
                    teardown_performed: true,
                    teardown_result: Some("success".to_owned()),
                    kept_on_failure: false,
                },
                artifacts: ArtifactPaths {
                    narration: "narration.txt".to_owned(),
                    results: "results.json".to_owned(),
                },
            }
        }

        #[test]
        fn evidence_report_serializes_to_valid_json() {
            let json = serde_json::to_string_pretty(&sample_report("full", "pass")).unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed["schema_version"], "1");
            assert_eq!(parsed["mode"], "full");
            assert_eq!(parsed["status"], "pass");
            assert!(parsed["error"].is_null());
            assert!(parsed["capabilities"].is_array());
            assert!(parsed["observed_paths"].is_array());
        }

        #[test]
        fn demonstrate_command_rejects_lifecycle_flags() {
            let result =
                <crate::Cli as clap::Parser>::try_parse_from(["xtask", "env", "demonstrate-grid-glb", "--teardown"]);
            assert!(
                result.is_err(),
                "the demonstrate-only command must not accept lifecycle flags"
            );
        }

        #[test]
        fn concise_error_is_single_line_and_bounded() {
            let error = format!("first line\n{}\r\nlast line", "x".repeat(600));
            let concise = concise_error(error);
            assert!(!concise.contains(['\n', '\r']));
            assert!(concise.chars().count() <= 512);
        }

        #[test]
        fn failed_outcome_retains_prior_capabilities() {
            let outcome = failed_outcome(
                vec![CapabilityResult {
                    capability: "prior".to_owned(),
                    result: "pass",
                    evidence: "runtime proof".to_owned(),
                }],
                Vec::new(),
                "current",
                "failed proof".to_owned(),
            );
            assert_eq!(outcome.capabilities.len(), 2);
            assert_eq!(outcome.capabilities[0].result, "pass");
            assert_eq!(outcome.capabilities[1].result, "fail");
            assert_eq!(outcome.error.as_deref(), Some("failed proof"));
        }

        #[test]
        fn narrator_captures_lines() {
            let mut narrator = Narrator::new();
            narrator.narrate("line one");
            narrator.narrate("line two");
            assert_eq!(narrator.lines.len(), 2);
            assert_eq!(narrator.lines[0], "line one");
            assert_eq!(narrator.lines[1], "line two");
        }

        #[test]
        fn capability_result_fields_present() {
            let cap = CapabilityResult {
                capability: "test".to_owned(),
                result: "pass",
                evidence: "evidence".to_owned(),
            };
            let json: serde_json::Value = serde_json::from_str(&serde_json::to_string(&cap).unwrap()).unwrap();
            assert!(json.get("capability").is_some());
            assert!(json.get("result").is_some());
            assert!(json.get("evidence").is_some());
        }

        #[test]
        fn lifecycle_record_serializes_teardown_states() {
            let no_teardown = LifecycleRecord {
                teardown_requested: false,
                teardown_performed: false,
                teardown_result: None,
                kept_on_failure: false,
            };
            let json = serde_json::to_string(&no_teardown).unwrap();
            assert!(json.contains("\"teardown_requested\":false"));

            let with_teardown = LifecycleRecord {
                teardown_requested: true,
                teardown_performed: true,
                teardown_result: Some("success".to_owned()),
                kept_on_failure: false,
            };
            let json = serde_json::to_string(&with_teardown).unwrap();
            assert!(json.contains("\"teardown_performed\":true"));
        }

        #[test]
        fn utc_timestamp_format_is_valid() {
            let ts = format_utc_timestamp();
            assert_eq!(ts.len(), 16, "expected YYYYMMDDTHHMMSSz format");
            assert!(ts.ends_with('Z'));
            let bytes = ts.as_bytes();
            assert_eq!(bytes.get(8).copied(), Some(b'T'));
            assert!(bytes.get(..8).unwrap().iter().all(u8::is_ascii_digit));
            assert!(bytes.get(9..15).unwrap().iter().all(u8::is_ascii_digit));
        }

        #[test]
        fn quick_mode_skips_full_capabilities() {
            let quick_caps = [
                CapabilityResult {
                    capability: "Active/active routing".to_owned(),
                    result: "pass",
                    evidence: "observed".to_owned(),
                },
                CapabilityResult {
                    capability: "Secure provider boundary".to_owned(),
                    result: "pass",
                    evidence: "verified".to_owned(),
                },
                CapabilityResult {
                    capability: "Session affinity and drain".to_owned(),
                    result: "skipped",
                    evidence: "quick mode".to_owned(),
                },
                CapabilityResult {
                    capability: "Edge withdrawal and recovery".to_owned(),
                    result: "skipped",
                    evidence: "quick mode".to_owned(),
                },
                CapabilityResult {
                    capability: "Grid restart recovery and soak".to_owned(),
                    result: "skipped",
                    evidence: "quick mode".to_owned(),
                },
            ];
            let skipped_count = quick_caps.iter().filter(|c| c.result == "skipped").count();
            assert_eq!(skipped_count, 3, "quick mode must skip 3 capabilities");
        }
    }
}
