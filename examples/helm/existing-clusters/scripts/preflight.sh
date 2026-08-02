#!/usr/bin/env bash
# Preflight checks for existing-cluster Grid installation.
# Validates cluster access, permissions, image availability, and connectivity
# before making any changes.
#
# Usage: ./preflight.sh <inventory.yaml>
#
# Requires: kubectl, helm, yq (https://github.com/mikefarah/yq)

set -euo pipefail

INVENTORY="${1:?Usage: $0 <inventory.yaml>}"

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

for cmd in kubectl helm yq; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: required command not found: $cmd" >&2
    exit 1
  fi
done

TOPOLOGY=$(yq '.topology' "$INVENTORY")
if [[ "$TOPOLOGY" != "dedicated-edge" && "$TOPOLOGY" != "combined-site" ]]; then
  echo "ERROR: inventory topology must be 'dedicated-edge' or 'combined-site', got: $TOPOLOGY" >&2
  exit 1
fi

ERRORS=0

check() {
  local desc="$1"
  shift
  if "$@" &>/dev/null; then
    echo "  PASS  $desc"
  else
    echo "  FAIL  $desc" >&2
    ERRORS=$((ERRORS + 1))
  fi
}

OP_REPO=$(yq '.images.operator.repository // "ghcr.io/praxis-proxy/grid-operator"' "$INVENTORY")
OP_TAG=$(yq '.images.operator.tag // "v0.1.1"' "$INVENTORY")
# The Helm chart's liveness/readiness probes require the operator health
# server added in v0.1.1. Earlier images will CrashLoopBackOff.
case "$OP_TAG" in
  v0.0.*|v0.1.0)
    echo "FAIL  operator image ${OP_REPO}:${OP_TAG} lacks health endpoints (requires v0.1.1+)" >&2
    ERRORS=$((ERRORS + 1))
    ;;
esac

SITE_NAMES=$(yq '.sites | keys | .[]' "$INVENTORY")

echo "Preflight: topology=$TOPOLOGY"
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq ".sites.${SITE}.context" "$INVENTORY")
  echo "Site: $SITE (context: $CONTEXT)"

  GW_ADDR=$(yq ".sites.${SITE}.gatewayAddress // \"\"" "$INVENTORY")
  if [[ -z "$GW_ADDR" || "$GW_ADDR" == "replace-me" ]]; then
    echo "  FAIL  gatewayAddress not configured for $SITE" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "  PASS  gatewayAddress: $GW_ADDR"
  fi

  SWIM_ADDR=$(yq ".sites.${SITE}.swimAddress // \"\"" "$INVENTORY")
  if [[ -z "$SWIM_ADDR" || "$SWIM_ADDR" == "replace-me" ]]; then
    echo "  FAIL  swimAddress not configured for $SITE" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "  PASS  swimAddress: $SWIM_ADDR"
  fi

  check "cluster reachable" kubectl --context "$CONTEXT" cluster-info
  check "namespace create permission" kubectl --context "$CONTEXT" auth can-i create namespaces
  check "deployment create permission" kubectl --context "$CONTEXT" auth can-i create deployments -n grid-system
  check "secret create permission" kubectl --context "$CONTEXT" auth can-i create secrets -n grid-system
  check "service create permission" kubectl --context "$CONTEXT" auth can-i create services -n grid-system
  check "configmap create permission" kubectl --context "$CONTEXT" auth can-i create configmaps -n grid-system

  GW_REPO=$(yq '.images.gateway.repository // "ghcr.io/praxis-proxy/grid-ai-rollup"' "$INVENTORY")
  GW_TAG=$(yq '.images.gateway.tag // "v0.1.0"' "$INVENTORY")
  GATEWAY_IMAGE="${GW_REPO}:${GW_TAG}"

  check "gateway image pullable" kubectl --context "$CONTEXT" run preflight-pull-test \
    --image="$GATEWAY_IMAGE" --restart=Never --rm -i --command -- echo ok 2>/dev/null

  OPERATOR_IMAGE="${OP_REPO}:${OP_TAG}"
  check "operator image pullable" kubectl --context "$CONTEXT" run preflight-op-pull-test \
    --image="$OPERATOR_IMAGE" --restart=Never --rm -i --command -- echo ok 2>/dev/null

  KUBE_VERSION=$(kubectl --context "$CONTEXT" version --short 2>/dev/null | grep "Server" | grep -oP 'v\K[0-9]+\.[0-9]+' || echo "0.0")
  MAJOR=$(echo "$KUBE_VERSION" | cut -d. -f1)
  MINOR=$(echo "$KUBE_VERSION" | cut -d. -f2)
  if [[ "$MAJOR" -ge 1 && "$MINOR" -ge 27 ]]; then
    echo "  PASS  Kubernetes version >= 1.27 ($KUBE_VERSION)"
  else
    echo "  FAIL  Kubernetes version >= 1.27 (got $KUBE_VERSION)" >&2
    ERRORS=$((ERRORS + 1))
  fi

  # Check user-managed prerequisite resources in grid-system namespace.
  # These must be created before install; the installer does not create them.
  if ! kubectl --context "$CONTEXT" get namespace grid-system &>/dev/null; then
    echo "  FAIL  grid-system namespace does not exist — create it and populate prerequisite resources before installing" >&2
    ERRORS=$((ERRORS + 1))
  else
    ROLES=$(yq ".sites.${SITE}.roles[]" "$INVENTORY" 2>/dev/null || echo "")
    HAS_CONSUMER=false
    HAS_PROVIDER=false
    if [[ "$TOPOLOGY" == "combined-site" ]]; then
      HAS_CONSUMER=true
      HAS_PROVIDER=true
    elif echo "$ROLES" | grep -q "consumer"; then
      HAS_CONSUMER=true
    elif echo "$ROLES" | grep -q "provider"; then
      HAS_PROVIDER=true
    fi

    if $HAS_CONSUMER; then
      check "consumer-praxis-config ConfigMap" kubectl --context "$CONTEXT" -n grid-system \
        get configmap consumer-praxis-config
      check "consumer-tls Secret" kubectl --context "$CONTEXT" -n grid-system \
        get secret consumer-tls
    fi

    if $HAS_PROVIDER; then
      check "provider-praxis-config ConfigMap" kubectl --context "$CONTEXT" -n grid-system \
        get configmap provider-praxis-config
      check "provider-tls Secret" kubectl --context "$CONTEXT" -n grid-system \
        get secret provider-tls
      check "mock-inference-credential Secret" kubectl --context "$CONTEXT" -n grid-system \
        get secret mock-inference-credential
    fi
  fi

  echo ""
done

SEEN_NAMES=""
for SITE in $SITE_NAMES; do
  if echo "$SEEN_NAMES" | grep -qw "$SITE"; then
    echo "  FAIL  duplicate site name: $SITE" >&2
    ERRORS=$((ERRORS + 1))
  fi
  SEEN_NAMES="$SEEN_NAMES $SITE"
done

if [[ "$ERRORS" -gt 0 ]]; then
  echo "Preflight FAILED with $ERRORS error(s). Fix issues before running install.sh." >&2
  exit 1
fi

echo "Preflight PASSED. All $( echo "$SITE_NAMES" | wc -w) sites ready."
