//! Container network lifecycle management.
//!
//! Creates, removes, and inspects Docker/Podman networks for Forge
//! environments.  All commands are structured [`CommandSpec`] values
//! executed through [`CommandRunner`].  No shell strings.

use std::collections::BTreeMap;

use crate::{
    command::runner::{CommandOutput, CommandRunner, CommandSpec},
    error::ForgeError,
};

// ---------------------------------------------------------------
// Naming
// ---------------------------------------------------------------

/// Name of the shared network for an environment.
pub fn network_name(env_name: &str) -> String {
    format!("{env_name}-net")
}

/// Verify the resolved runtime supports cross-cluster networking.
///
/// kind names the override after the provider it chose for itself, so Docker
/// reads `KIND_EXPERIMENTAL_DOCKER_NETWORK` and Podman reads
/// `KIND_EXPERIMENTAL_PODMAN_NETWORK`. Forge sets both, so either runtime
/// works and only a third one is refused.
///
/// # Errors
///
/// Returns [`ForgeError::Config`] if the runtime is neither Docker nor Podman.
pub fn require_supported_runtime_for_cross_cluster(binary: &str) -> Result<(), ForgeError> {
    if matches!(binary, "docker" | "podman") {
        return Ok(());
    }
    Err(ForgeError::Config(format!(
        "cross-cluster networking needs Docker or Podman, but the runtime resolved to {binary:?}"
    )))
}

// ---------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------

/// Create a container network with ownership labels.
///
/// Idempotent: returns `Ok(())` if the network already exists
/// and is owned by this environment.
///
/// # Errors
///
/// Returns [`ForgeError`] if the network cannot be created or
/// an existing network has mismatched ownership labels.
pub fn create_network(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
    env_name: &str,
) -> Result<(), ForgeError> {
    if network_exists(runner, binary, net_name)? {
        return verify_ownership(runner, binary, net_name, env_name);
    }
    let spec = create_spec(binary, net_name, env_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network create")
}

/// Remove a container network after verifying ownership.
///
/// Idempotent: returns `Ok(())` if the network does not exist.
///
/// # Errors
///
/// Returns [`ForgeError`] if ownership verification fails or
/// the network cannot be removed.
pub fn remove_network(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
    env_name: &str,
) -> Result<(), ForgeError> {
    if !network_exists(runner, binary, net_name)? {
        return Ok(());
    }
    verify_ownership(runner, binary, net_name, env_name)?;
    let spec = remove_spec(binary, net_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network rm")
}

/// Check whether a container network with the given name exists.
///
/// # Errors
///
/// Returns [`ForgeError`] if the runtime binary cannot execute.
pub fn network_exists(runner: &dyn CommandRunner, binary: &str, net_name: &str) -> Result<bool, ForgeError> {
    let spec = inspect_spec(binary, net_name);
    let output = runner.run(&spec)?;
    Ok(output.status == 0)
}

/// Read the current IPv4 subnet from a container network.
///
/// # Errors
///
/// Returns [`ForgeError`] if the inspect command fails or the network does
/// not expose a valid IPv4 subnet.
pub fn inspect_network_cidr(runner: &dyn CommandRunner, binary: &str, net_name: &str) -> Result<String, ForgeError> {
    let spec = cidr_spec(binary, net_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network inspect")?;
    parse_ipam_config(&output.stdout)
}

// ---------------------------------------------------------------
// Ownership
// ---------------------------------------------------------------

/// Verify that an existing network is owned by this environment.
fn verify_ownership(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
    env_name: &str,
) -> Result<(), ForgeError> {
    let labels = inspect_labels(runner, binary, net_name)?;
    check_label(&labels, "forge.managed", "true", net_name)?;
    check_label(&labels, "forge.environment", env_name, net_name)
}

/// Fetch labels from an existing network.
fn inspect_labels(
    runner: &dyn CommandRunner,
    binary: &str,
    net_name: &str,
) -> Result<BTreeMap<String, String>, ForgeError> {
    let spec = labels_spec(binary, net_name);
    let output = runner.run(&spec)?;
    check_success(&output, "network inspect")?;
    parse_labels(&output.stdout)
}

/// Verify a single label value matches the expected value.
fn check_label(labels: &BTreeMap<String, String>, key: &str, expected: &str, net_name: &str) -> Result<(), ForgeError> {
    match labels.get(key) {
        Some(val) if val == expected => Ok(()),
        Some(val) => Err(ownership_mismatch(net_name, key, expected, val)),
        None => Err(missing_label(net_name, key)),
    }
}

/// Build an error for a mismatched ownership label.
fn ownership_mismatch(net_name: &str, key: &str, expected: &str, actual: &str) -> ForgeError {
    ForgeError::State(format!("network '{net_name}' has {key}={actual}, expected {expected}"))
}

/// Build an error for a missing ownership label.
fn missing_label(net_name: &str, key: &str) -> ForgeError {
    ForgeError::State(format!(
        "network '{net_name}' missing label {key} \u{2014} not managed by Forge"
    ))
}

// ---------------------------------------------------------------
// Command specs
// ---------------------------------------------------------------

/// Build a `<binary> network create` command spec with labels.
fn create_spec(binary: &str, net_name: &str, env_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec![
            "network".into(),
            "create".into(),
            "--label".into(),
            "forge.managed=true".into(),
            "--label".into(),
            format!("forge.environment={env_name}").into(),
            net_name.into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network rm` command spec.
fn remove_spec(binary: &str, net_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec!["network".into(), "rm".into(), net_name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network inspect` command spec.
fn inspect_spec(binary: &str, net_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec!["network".into(), "inspect".into(), net_name.into()],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network inspect --format` spec for labels.
fn labels_spec(binary: &str, net_name: &str) -> CommandSpec {
    CommandSpec {
        program: binary.into(),
        args: vec![
            "network".into(),
            "inspect".into(),
            net_name.into(),
            "--format".into(),
            "{{json .Labels}}".into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

/// Build a `<binary> network inspect --format` spec for the IPAM config.
fn cidr_spec(binary: &str, net_name: &str) -> CommandSpec {
    // `{{json .}}` rather than `{{json .IPAM.Config}}`. Podman has no IPAM
    // field, so asking it to evaluate the Docker path fails before the subnet
    // is ever reached. Asking for the whole document works on both runtimes,
    // and keeps this call distinct from the bare inspect used to test whether
    // the network exists at all.
    CommandSpec {
        program: binary.into(),
        args: vec![
            "network".into(),
            "inspect".into(),
            net_name.into(),
            "--format".into(),
            "{{json .}}".into(),
        ],
        env: BTreeMap::default(),
        stdin: None,
        redact: Vec::new(),
    }
}

// ---------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------

/// Parse JSON labels from `docker network inspect --format` output.
fn parse_labels(stdout: &str) -> Result<BTreeMap<String, String>, ForgeError> {
    let trimmed = stdout.trim();
    if trimmed.is_empty() {
        return Ok(BTreeMap::new());
    }
    serde_json::from_str(trimmed).map_err(|err| ForgeError::State(format!("cannot parse network labels: {err}")))
}

/// Parse and validate the first IPv4 subnet in a formatted IPAM config.
fn parse_ipam_config(stdout: &str) -> Result<String, ForgeError> {
    let document: serde_json::Value = serde_json::from_str(stdout.trim())
        .map_err(|err| ForgeError::State(format!("cannot parse network IPAM config: {err}")))?;
    let subnet = first_ipv4_subnet(&document)
        .ok_or_else(|| ForgeError::State("network IPAM config has no subnet".to_owned()))?;
    validate_ipv4_cidr(&subnet)?;
    Ok(subnet)
}

/// Find the first IPv4 subnet, whichever runtime described the network.
///
/// Docker nests it under `IPAM.Config[].Subnet`; Podman puts it in
/// `subnets[].subnet`. The two also differ on whether the document is a list
/// of networks or the config array itself, so this walks whatever it is
/// looking for either key rather than encoding four shapes.
pub(crate) fn first_ipv4_subnet(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Array(items) => items.iter().find_map(first_ipv4_subnet),
        serde_json::Value::Object(fields) => {
            for key in ["Subnet", "subnet"] {
                if let Some(found) = fields.get(key).and_then(serde_json::Value::as_str) {
                    // IPv6 entries sit alongside IPv4 ones and are not what the
                    // caller wants, so keep looking rather than failing here.
                    if validate_ipv4_cidr(found).is_ok() {
                        return Some(found.to_owned());
                    }
                }
            }
            for key in ["IPAM", "Config", "subnets"] {
                if let Some(found) = fields.get(key).and_then(first_ipv4_subnet) {
                    return Some(found);
                }
            }
            None
        },
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => None,
    }
}

/// Validate an IPv4 CIDR without accepting host-only or IPv6 forms.
fn validate_ipv4_cidr(cidr: &str) -> Result<(), ForgeError> {
    let (address, prefix) = cidr
        .split_once('/')
        .ok_or_else(|| ForgeError::State(format!("network subnet is not CIDR: {cidr:?}")))?;
    address
        .parse::<std::net::Ipv4Addr>()
        .map_err(|err| ForgeError::State(format!("network subnet has an invalid IPv4 address: {err}")))?;
    let prefix = prefix
        .parse::<u8>()
        .map_err(|err| ForgeError::State(format!("network subnet has an invalid prefix: {err}")))?;
    if prefix > 32 {
        return Err(ForgeError::State(format!(
            "network subnet prefix /{prefix} exceeds /32"
        )));
    }
    Ok(())
}

/// Check command output for success (exit code 0).
fn check_success(output: &CommandOutput, context: &str) -> Result<(), ForgeError> {
    if output.status == 0 {
        return Ok(());
    }
    Err(ForgeError::Command {
        program: context.to_owned(),
        message: format!("exit code {}: {}", output.status, output.stderr.trim()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::runner::MockRunner;

    /// Successful empty command output.
    fn ok() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    /// Failed command output (network not found).
    fn not_found() -> CommandOutput {
        CommandOutput {
            status: 1,
            stdout: String::new(),
            stderr: "network test-net not found\n".to_owned(),
        }
    }

    /// Labels JSON for a Forge-managed network.
    fn owned_labels(env: &str) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: format!(r#"{{"forge.managed":"true","forge.environment":"{env}"}}"#),
            stderr: String::new(),
        }
    }

    /// Labels JSON for a network not managed by Forge.
    fn foreign_labels() -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: r#"{"some.other":"label"}"#.to_owned(),
            stderr: String::new(),
        }
    }

    /// Formatted Docker IPAM response for one IPv4 subnet.
    /// A Docker `network inspect` document.
    fn docker_network(cidr: &str) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: format!(
                r#"[{{"Name":"test-net","IPAM":{{"Driver":"default","Config":[{{"Subnet":"{cidr}","Gateway":"172.18.0.1"}}]}}}}]"#
            ),
            stderr: String::new(),
        }
    }

    /// A Podman `network inspect` document, which nests the subnet elsewhere
    /// and spells the keys in lower case.
    fn podman_network(cidr: &str) -> CommandOutput {
        CommandOutput {
            status: 0,
            stdout: format!(
                r#"[{{"name":"test-net","driver":"bridge","subnets":[{{"subnet":"{cidr}","gateway":"172.18.0.1"}}]}}]"#
            ),
            stderr: String::new(),
        }
    }

    #[test]
    fn either_runtime_can_do_cross_cluster() {
        // kind names the override after the provider it chose for itself, and
        // forge sets both, so neither runtime is the special one.
        assert!(
            matches!(require_supported_runtime_for_cross_cluster("docker"), Ok(())),
            "docker is supported"
        );
        assert!(
            matches!(require_supported_runtime_for_cross_cluster("podman"), Ok(())),
            "podman is supported"
        );
    }

    #[test]
    fn a_third_runtime_is_refused() {
        let refused = require_supported_runtime_for_cross_cluster("nerdctl");
        assert!(refused.is_err(), "only docker and podman set a kind network");
    }

    #[test]
    fn network_name_format() {
        assert_eq!(network_name("test"), "test-net", "simple name");
        assert_eq!(network_name("prod-env"), "prod-env-net", "hyphenated name");
    }

    #[test]
    fn create_when_not_exists() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", not_found());
        runner.respond("docker", ok());

        create_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("network create"), "should call network create");
        assert!(runner.was_called("forge.managed=true"), "should include managed label");
        assert!(runner.was_called("forge.environment=test"), "should include env label");
    }

    #[test]
    fn create_skips_when_exists_with_correct_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("test"),
        );

        create_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(
            !runner.was_called("network create"),
            "should not create existing network"
        );
    }

    #[test]
    fn create_rejects_wrong_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("other-env"),
        );

        let result = create_network(&runner, "docker", "test-net", "test");
        let Err(err) = result else {
            std::process::abort();
        };
        let msg = err.to_string();
        assert!(
            msg.contains("expected test"),
            "error should mention expected env: {msg}"
        );
    }

    #[test]
    fn create_rejects_unmanaged_network() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            foreign_labels(),
        );

        let result = create_network(&runner, "docker", "test-net", "test");
        assert!(result.is_err(), "should reject unmanaged network");
    }

    #[test]
    fn remove_with_correct_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("test"),
        );
        runner.respond("docker network rm test-net", ok());

        remove_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("network rm"), "should call network rm");
    }

    #[test]
    fn remove_skips_when_not_exists() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", not_found());

        remove_network(&runner, "docker", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(
            !runner.was_called("network rm"),
            "should not call rm on missing network"
        );
    }

    #[test]
    fn remove_rejects_wrong_owner() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            owned_labels("other-env"),
        );

        let result = remove_network(&runner, "docker", "test-net", "test");
        assert!(result.is_err(), "should reject mismatched owner on remove");
    }

    #[test]
    fn remove_refuses_unmanaged_network() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());
        runner.respond(
            "docker network inspect test-net --format {{json .Labels}}",
            foreign_labels(),
        );

        let result = remove_network(&runner, "docker", "test-net", "test");
        assert!(result.is_err(), "should reject unmanaged network on remove");
        assert!(!runner.was_called("network rm"), "must not remove unmanaged network");
    }

    #[test]
    fn exists_true_when_present() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", ok());

        let exists = network_exists(&runner, "docker", "test-net").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(exists, "should report network as existing");
    }

    #[test]
    fn exists_false_when_missing() {
        let mut runner = MockRunner::new();
        runner.respond("docker network inspect test-net", not_found());

        let exists = network_exists(&runner, "docker", "test-net").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(!exists, "should report network as not existing");
    }

    #[test]
    fn inspect_network_cidr_reads_formatted_ipam_config() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker network inspect test-net --format {{json .}}",
            docker_network("172.18.0.0/16"),
        );

        let cidr = inspect_network_cidr(&runner, "docker", "test-net").unwrap_or_else(|_| std::process::abort());
        assert_eq!(cidr, "172.18.0.0/16");
    }

    #[test]
    fn inspect_network_cidr_reads_a_podman_document() {
        // Podman has no IPAM field at all, so asking it to evaluate the Docker
        // template fails before the subnet is ever reached. This is the shape
        // it returns instead.
        let mut runner = MockRunner::new();
        runner.respond(
            "podman network inspect test-net --format {{json .}}",
            podman_network("10.89.0.0/24"),
        );

        let cidr = inspect_network_cidr(&runner, "podman", "test-net").unwrap_or_else(|_| std::process::abort());
        assert_eq!(cidr, "10.89.0.0/24");
    }

    #[test]
    fn an_ipv6_entry_beside_an_ipv4_one_does_not_win() {
        // Dual-stack networks list both. The MetalLB allocator is IPv4, so the
        // IPv6 entry has to be stepped over rather than taken and rejected.
        let mut runner = MockRunner::new();
        runner.respond(
            "podman network inspect test-net --format {{json .}}",
            CommandOutput {
                status: 0,
                stdout: r#"[{"name":"test-net","subnets":[{"subnet":"fd00::/64"},{"subnet":"10.89.0.0/24"}]}]"#
                    .to_owned(),
                stderr: String::new(),
            },
        );

        let cidr = inspect_network_cidr(&runner, "podman", "test-net").unwrap_or_else(|_| std::process::abort());
        assert_eq!(cidr, "10.89.0.0/24");
    }

    #[test]
    fn inspect_network_cidr_rejects_invalid_subnet() {
        let mut runner = MockRunner::new();
        runner.respond(
            "docker network inspect test-net --format {{json .}}",
            docker_network("fd00::/64"),
        );

        assert!(
            inspect_network_cidr(&runner, "docker", "test-net").is_err(),
            "IPv6 must not be accepted by the IPv4 MetalLB allocator"
        );
    }

    #[test]
    fn parse_labels_valid_json() {
        let input = r#"{"forge.managed":"true","forge.environment":"test"}"#;
        let labels = parse_labels(input).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(
            labels.get("forge.managed").map(String::as_str),
            Some("true"),
            "managed label"
        );
        assert_eq!(
            labels.get("forge.environment").map(String::as_str),
            Some("test"),
            "env label"
        );
    }

    #[test]
    fn parse_labels_empty_string() {
        let labels = parse_labels("").unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(labels.is_empty(), "empty input should yield empty map");
    }

    #[test]
    fn podman_uses_correct_binary() {
        let mut runner = MockRunner::new();
        runner.respond("podman network inspect test-net", not_found());
        runner.respond("podman", ok());

        create_network(&runner, "podman", "test-net", "test").unwrap_or_else(|_| std::process::abort());
        assert!(runner.was_called("podman"), "should use podman binary");
    }
}
