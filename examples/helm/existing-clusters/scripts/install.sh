#!/usr/bin/env bash
# Install Grid components onto existing clusters using Helm.
# Reads a local inventory file for cluster contexts and topology.
#
# Usage: ./install.sh <inventory.yaml>
#
# Requires: kubectl, helm, yq

set -euo pipefail

INVENTORY="${1:?Usage: $0 <inventory.yaml>}"

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

TOPOLOGY=$(yq '.topology' "$INVENTORY")
CHART_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)/charts"
VALUES_DIR="$(cd "$(dirname "$0")/.." && pwd)/${TOPOLOGY}/values"

if [[ ! -d "$VALUES_DIR" ]]; then
  echo "ERROR: values directory not found for topology '$TOPOLOGY': $VALUES_DIR" >&2
  exit 1
fi

SITE_NAMES=$(yq '.sites | keys | .[]' "$INVENTORY")

# Build --set arguments for inventory image overrides.
IMAGE_SETS=()
OP_REPO=$(yq '.images.operator.repository // ""' "$INVENTORY")
OP_TAG=$(yq '.images.operator.tag // ""' "$INVENTORY")
GW_REPO=$(yq '.images.gateway.repository // ""' "$INVENTORY")
GW_TAG=$(yq '.images.gateway.tag // ""' "$INVENTORY")
[[ -n "$OP_REPO" ]] && IMAGE_SETS+=(--set "image.repository=$OP_REPO")
[[ -n "$OP_TAG" ]]  && IMAGE_SETS+=(--set "image.tag=$OP_TAG")

GW_SETS=()
[[ -n "$GW_REPO" ]] && GW_SETS+=(--set "image.repository=$GW_REPO")
[[ -n "$GW_TAG" ]]  && GW_SETS+=(--set "image.tag=$GW_TAG")

# Run preflight before making any changes.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PREFLIGHT="${SCRIPT_DIR}/preflight.sh"
if [[ ! -f "$PREFLIGHT" ]]; then
  echo "ERROR: preflight.sh not found at $PREFLIGHT" >&2
  exit 1
fi
echo "Running preflight checks..."
bash "$PREFLIGHT" "$INVENTORY"
echo ""

echo "Installing Grid components"
echo "  Topology: $TOPOLOGY"
echo "  Charts:   $CHART_DIR"
echo "  Values:   $VALUES_DIR"
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq ".sites.${SITE}.context" "$INVENTORY")
  ROLES=$(yq ".sites.${SITE}.roles[]" "$INVENTORY" 2>/dev/null || echo "")

  echo "--- Site: $SITE (context: $CONTEXT) ---"

  echo "  Creating namespace grid-system..."
  kubectl --context "$CONTEXT" create namespace grid-system --dry-run=client -o yaml \
    | kubectl --context "$CONTEXT" apply -f -

  # Build SWIM seeds from all other sites' swimAddress.
  SWIM_SEEDS=""
  for PEER in $SITE_NAMES; do
    [[ "$PEER" == "$SITE" ]] && continue
    PEER_ADDR=$(yq ".sites.${PEER}.swimAddress // \"\"" "$INVENTORY")
    if [[ -n "$PEER_ADDR" && "$PEER_ADDR" != "replace-me" ]]; then
      [[ -n "$SWIM_SEEDS" ]] && SWIM_SEEDS="${SWIM_SEEDS},"
      SWIM_SEEDS="${SWIM_SEEDS}${PEER_ADDR}:7946"
    fi
  done

  OPERATOR_VALUES="${VALUES_DIR}/${SITE}-operator.yaml"
  if [[ -f "$OPERATOR_VALUES" ]]; then
    SEED_SETS=()
    [[ -n "$SWIM_SEEDS" ]] && SEED_SETS+=(--set "swim.seeds=${SWIM_SEEDS}")

    echo "  Installing grid-operator..."
    helm upgrade --install grid-operator "$CHART_DIR/grid-operator" \
      --kube-context "$CONTEXT" \
      --namespace grid-system \
      --values "$OPERATOR_VALUES" \
      "${IMAGE_SETS[@]}" \
      "${SEED_SETS[@]}" \
      --wait --timeout 120s
  else
    echo "  WARN: no operator values at $OPERATOR_VALUES, skipping" >&2
  fi

  if [[ "$TOPOLOGY" == "dedicated-edge" ]]; then
    GATEWAY_VALUES="${VALUES_DIR}/${SITE}-gateway.yaml"
    if [[ -f "$GATEWAY_VALUES" ]]; then
      RELEASE_NAME="gateway"
      if echo "$ROLES" | grep -q "consumer"; then
        RELEASE_NAME="consumer-gateway"
      elif echo "$ROLES" | grep -q "provider"; then
        RELEASE_NAME="provider-gateway"
      fi
      echo "  Installing $RELEASE_NAME..."
      helm upgrade --install "$RELEASE_NAME" "$CHART_DIR/praxis-gateway" \
        --kube-context "$CONTEXT" \
        --namespace grid-system \
        --values "$GATEWAY_VALUES" \
        "${GW_SETS[@]}" \
        --wait --timeout 120s
    fi
  elif [[ "$TOPOLOGY" == "combined-site" ]]; then
    CONSUMER_VALUES="${VALUES_DIR}/${SITE}-consumer-gateway.yaml"
    if [[ -f "$CONSUMER_VALUES" ]]; then
      echo "  Installing consumer-gateway..."
      helm upgrade --install consumer-gateway "$CHART_DIR/praxis-gateway" \
        --kube-context "$CONTEXT" \
        --namespace grid-system \
        --values "$CONSUMER_VALUES" \
        "${GW_SETS[@]}" \
        --wait --timeout 120s
    fi

    PROVIDER_VALUES="${VALUES_DIR}/${SITE}-provider-gateway.yaml"
    if [[ -f "$PROVIDER_VALUES" ]]; then
      echo "  Installing provider-gateway..."
      helm upgrade --install provider-gateway "$CHART_DIR/praxis-gateway" \
        --kube-context "$CONTEXT" \
        --namespace grid-system \
        --values "$PROVIDER_VALUES" \
        "${GW_SETS[@]}" \
        --wait --timeout 120s
    fi
  fi

  echo ""
done

echo "Installation complete. Run verify.sh to check deployment health."
