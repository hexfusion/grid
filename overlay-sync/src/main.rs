//! Grid overlay-sync entry point.
//!
//! Reads a sync configuration file, validates it, and enters the
//! polling loop.  See the crate-level documentation on [`overlay_sync`]
//! for design details and security invariants.

use std::process::ExitCode;

use clap::Parser;
use overlay_sync::SyncError;

/// Grid overlay-sync: watches a Kubernetes `ConfigMap` and writes its
/// data to a local file for the Praxis AI edge gateway.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Path to the sync configuration file.
    #[arg(long, default_value = "/etc/overlay-sync/sync.yaml")]
    config: String,
}

/// Entry point.
fn main() -> ExitCode {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match run(&cli.config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!(error = %e, "fatal error");
            ExitCode::FAILURE
        },
    }
}

/// Load config, validate, and enter the sync loop.
fn run(config_path: &str) -> Result<(), SyncError> {
    let cfg = overlay_sync::config::load(config_path)?;
    overlay_sync::config::validate(&cfg)?;
    overlay_sync::run_sync_loop(&cfg)
}
