#!/usr/bin/env bash
# Uninstall Grid components from existing clusters.
# Removes Helm releases and the grid-system namespace.
# Does NOT delete clusters, CRDs, or persistent volumes.
#
# Usage: ./uninstall.sh <inventory.yaml>
#
# Requires: kubectl, helm, yq

set -euo pipefail

INVENTORY="${1:?Usage: $0 <inventory.yaml>}"

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

TOPOLOGY=$(yq eval '.topology' "$INVENTORY")
SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")

echo "Uninstalling Grid components"
echo "  Topology: $TOPOLOGY"
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  echo "--- Site: $SITE (context: $CONTEXT) ---"

  if [[ "$TOPOLOGY" == "combined-site" ]]; then
    echo "  Removing provider-gateway..."
    helm uninstall provider-gateway \
      --kube-context "$CONTEXT" \
      --namespace grid-system \
      --wait 2>/dev/null || echo "  (not installed)"

    echo "  Removing consumer-gateway..."
    helm uninstall consumer-gateway \
      --kube-context "$CONTEXT" \
      --namespace grid-system \
      --wait 2>/dev/null || echo "  (not installed)"
  else
    for RELEASE in provider-gateway consumer-gateway gateway; do
      if helm status "$RELEASE" --kube-context "$CONTEXT" --namespace grid-system &>/dev/null; then
        echo "  Removing $RELEASE..."
        helm uninstall "$RELEASE" \
          --kube-context "$CONTEXT" \
          --namespace grid-system \
          --wait
      fi
    done
  fi

  echo "  Removing grid-mock-providers..."
  helm uninstall grid-mock-providers \
    --kube-context "$CONTEXT" \
    --namespace grid-system \
    --wait 2>/dev/null || echo "  (not installed)"

  echo "  Removing grid-site..."
  helm uninstall grid-site \
    --kube-context "$CONTEXT" \
    --namespace grid-system \
    --wait 2>/dev/null || echo "  (not installed)"

  echo "  Removing grid-operator..."
  helm uninstall grid-operator \
    --kube-context "$CONTEXT" \
    --namespace grid-system \
    --wait 2>/dev/null || echo "  (not installed)"

  echo "  Removing installer-created ConfigMaps..."
  for CM in provider-praxis-config consumer-praxis-config; do
    kubectl --context "$CONTEXT" -n grid-system delete configmap "$CM" \
      --ignore-not-found 2>/dev/null || true
  done

  echo "  Removing test Jobs..."
  kubectl --context "$CONTEXT" -n grid-system delete jobs \
    -l app.kubernetes.io/part-of=grid-verify --ignore-not-found 2>/dev/null || true

  echo ""
done

echo "Uninstall complete. Clusters, CRDs, and the grid-system namespace were NOT removed."
echo "To remove the namespace: kubectl delete namespace grid-system --context <context>"
