#!/bin/bash
# Generate Grid CRDs for deployment manifests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CRD_DIR="$REPO_ROOT/deploy/crds"

cd "$REPO_ROOT"

echo "Generating Grid CRDs..."
mkdir -p "$CRD_DIR"

# Generate CRDs and split into individual YAML files
cargo run -p operator --bin generate_crds | jq -r '.items[0]' | yq eval -P > "$CRD_DIR/agenttoolprovider.yaml"
cargo run -p operator --bin generate_crds | jq -r '.items[1]' | yq eval -P > "$CRD_DIR/gridnetwork.yaml"
cargo run -p operator --bin generate_crds | jq -r '.items[2]' | yq eval -P > "$CRD_DIR/gridsite.yaml"
cargo run -p operator --bin generate_crds | jq -r '.items[3]' | yq eval -P > "$CRD_DIR/inferenceprovider.yaml"

# Kustomization over the generated CRDs, for kubectl apply -k and kustomize consumers.
{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "resources:"
  for f in "$CRD_DIR"/*.yaml; do
    [ "$(basename "$f")" = kustomization.yaml ] && continue
    echo "  - $(basename "$f")"
  done
} > "$CRD_DIR/kustomization.yaml"

# Opt-in managed CRDs (crds.managed): the same CRDs as templates, so they
# upgrade with the release. resource-policy keep retains them on uninstall.
CHART_CRD_DIR="$REPO_ROOT/charts/grid-operator/templates/crds"
mkdir -p "$CHART_CRD_DIR"
rm -f "$CHART_CRD_DIR"/*.yaml
for f in "$CRD_DIR"/*.yaml; do
  [ "$(basename "$f")" = kustomization.yaml ] && continue
  if grep -q '{{' "$f"; then
    echo "error: $f contains '{{', which Helm would template" >&2
    exit 1
  fi
  {
    echo '{{- if .Values.crds.managed }}'
    yq -P '.metadata.annotations = ((.metadata.annotations // {}) + {"helm.sh/resource-policy": "keep"})' "$f"
    echo '{{- end }}'
  } > "$CHART_CRD_DIR/$(basename "$f")"
done

echo "CRDs generated in $CRD_DIR:"
ls -la "$CRD_DIR"

echo ""
echo "To validate CRDs:"
echo "  kubectl --dry-run=server create -k deploy/crds/"
echo ""
echo "To regenerate after schema changes:"
echo "  ./scripts/generate-deployment-crds.sh"
