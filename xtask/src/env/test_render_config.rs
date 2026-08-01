use super::external_provider::ExternalProviderDescriptor;

const MINIMAL_FORGE_CONFIG: &str = "
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test-env
spec:
  runtime:
    provider: docker
  clusters:
    - name: test-cluster
      stacks: [inference-sim]
      properties:
        region: test
  stacks:
    inference-sim:
      description: Private inference backend
      steps:
        - type: helm
          release: provider-gateway
          values:
            credentials:
              - name: mock-credential
                mountPath: /etc/praxis/credentials/mock
";

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "test assertions benefit from unwrap for clear failure location"
)]
fn test_normal_mode_no_openai() {
    let result = super::render_config(MINIMAL_FORGE_CONFIG, super::IngressMode::Global, None).unwrap();
    let config: serde_yaml::Value = serde_yaml::from_str(&result).unwrap();

    let stacks = config.get("spec").unwrap().get("stacks").unwrap().as_mapping().unwrap();

    assert!(stacks.get("inference-sim-openai").is_none());

    let inference_sim = stacks.get("inference-sim").unwrap();
    let steps = inference_sim.get("steps").unwrap().as_sequence().unwrap();

    for step in steps {
        if let Some(step_map) = step.as_mapping()
            && step_map.get("type").and_then(|v| v.as_str()) == Some("helm")
            && step_map.get("release").and_then(|v| v.as_str()) == Some("provider-gateway")
        {
            let values = step_map.get("values").unwrap().as_mapping().unwrap();
            let credentials = values.get("credentials").unwrap().as_sequence().unwrap();

            for cred in credentials {
                let name = cred.get("name").unwrap().as_str().unwrap();
                assert_ne!(name, "openai-api-key");
            }
        }
    }
}

#[test]
#[expect(
    clippy::unwrap_used,
    clippy::too_many_lines,
    reason = "one linear assertion verifies the generated credential mount"
)]
fn test_openai_mode_creates_required_credential() {
    let external_provider = ExternalProviderDescriptor::openai("gpt-4o-mini");

    let result = super::render_config(
        MINIMAL_FORGE_CONFIG,
        super::IngressMode::Global,
        Some(&external_provider),
    )
    .unwrap();
    let config: serde_yaml::Value = serde_yaml::from_str(&result).unwrap();

    let stacks = config.get("spec").unwrap().get("stacks").unwrap().as_mapping().unwrap();

    let openai_stack = stacks.get("inference-sim-openai").unwrap();
    let steps = openai_stack.get("steps").unwrap().as_sequence().unwrap();

    let mut found_openai_credential = false;
    for step in steps {
        if let Some(step_map) = step.as_mapping()
            && step_map.get("type").and_then(|v| v.as_str()) == Some("helm")
            && step_map.get("release").and_then(|v| v.as_str()) == Some("provider-gateway")
        {
            let values = step_map.get("values").unwrap().as_mapping().unwrap();
            let credentials = values.get("credentials").unwrap().as_sequence().unwrap();

            for cred in credentials {
                let name = cred.get("name").unwrap().as_str().unwrap();
                if name == "openai-api-key" {
                    assert!(!found_openai_credential);
                    found_openai_credential = true;

                    assert_eq!(
                        cred.get("mountPath").unwrap().as_str().unwrap(),
                        "/etc/praxis/credentials/openai"
                    );
                    assert!(!cred.get("optional").unwrap().as_bool().unwrap());
                }
            }
        }
    }

    assert!(found_openai_credential);
}

#[test]
#[expect(
    clippy::unwrap_used,
    reason = "test assertions benefit from unwrap for clear failure location"
)]
fn test_normal_stack_unchanged_in_openai_mode() {
    let external_provider = ExternalProviderDescriptor::openai("gpt-4o-mini");

    let result = super::render_config(
        MINIMAL_FORGE_CONFIG,
        super::IngressMode::Global,
        Some(&external_provider),
    )
    .unwrap();
    let config: serde_yaml::Value = serde_yaml::from_str(&result).unwrap();

    let stacks = config.get("spec").unwrap().get("stacks").unwrap().as_mapping().unwrap();

    let inference_sim = stacks.get("inference-sim").unwrap();
    let steps = inference_sim.get("steps").unwrap().as_sequence().unwrap();

    for step in steps {
        if let Some(step_map) = step.as_mapping()
            && step_map.get("type").and_then(|v| v.as_str()) == Some("helm")
            && step_map.get("release").and_then(|v| v.as_str()) == Some("provider-gateway")
        {
            let values = step_map.get("values").unwrap().as_mapping().unwrap();
            let credentials = values.get("credentials").unwrap().as_sequence().unwrap();

            for cred in credentials {
                let name = cred.get("name").unwrap().as_str().unwrap();
                assert_ne!(name, "openai-api-key");
            }
        }
    }
}

#[test]
fn test_missing_helm_paths_fail() {
    let config_without_steps = "
apiVersion: forge.praxis.dev/v1alpha1
spec:
  clusters:
    - name: test-cluster
      stacks: [inference-sim]
      properties:
        region: test
  stacks:
    inference-sim:
      description: Test
";

    let external_provider = ExternalProviderDescriptor::openai("gpt-4o-mini");

    let result = super::render_config(
        config_without_steps,
        super::IngressMode::Global,
        Some(&external_provider),
    );
    let error_msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => unreachable!("Expected error but got success"),
    };
    assert_eq!(error_msg, "inference-sim-openai.steps not found or not a sequence");
}

#[test]
fn test_missing_stacks_fails() {
    let config_without_stacks = "
apiVersion: forge.praxis.dev/v1alpha1
spec:
  clusters:
    - name: test-cluster
      stacks: [inference-sim]
  runtime:
    provider: docker
";

    let external_provider = ExternalProviderDescriptor::openai("gpt-4o-mini");

    let result = super::render_config(
        config_without_stacks,
        super::IngressMode::Global,
        Some(&external_provider),
    );
    assert!(result.is_err());
}

#[test]
fn test_stack_selection() {
    assert_eq!(super::select_stack_for_provider("east", true), "inference-sim-openai");
    assert_eq!(super::select_stack_for_provider("east", false), "inference-sim");
    assert_eq!(super::select_stack_for_provider("west", true), "inference-sim");
    assert_eq!(super::select_stack_for_provider("west", false), "inference-sim");
}

#[test]
#[expect(clippy::too_many_lines, reason = "Test case with extensive configuration data")]
fn test_existing_openai_stack_fails() {
    let config_with_existing_stack = "
apiVersion: forge.praxis.dev/v1alpha1
kind: Environment
metadata:
  name: test-env
spec:
  runtime:
    provider: docker
  clusters:
    - name: test-cluster
      stacks: [inference-sim]
      properties:
        region: test
  stacks:
    inference-sim:
      description: Private inference backend
      steps:
        - type: helm
          release: provider-gateway
          values:
            credentials: []
    inference-sim-openai:
      description: Existing stack
";

    let external_provider = ExternalProviderDescriptor::openai("gpt-4o-mini");

    let result = super::render_config(
        config_with_existing_stack,
        super::IngressMode::Global,
        Some(&external_provider),
    );
    let error_msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => unreachable!("Expected error but got success"),
    };
    assert_eq!(error_msg, "inference-sim-openai stack already exists");
}
