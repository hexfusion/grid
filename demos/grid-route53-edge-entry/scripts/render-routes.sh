#!/usr/bin/env bash
# Render OpenShift Route manifests for Route 53 edge entry demo.
#
# Reads the inventory and produces two Route manifests per site:
#   - Origin Route (direct per-site hostname for health checks)
#   - Global Route (shared public hostname accepted on every cluster)
#
# Rendered manifests are written to --output-dir with umask 077 because
# they may contain TLS private key material.
#
# Does NOT apply anything. The operator applies with explicit kubectl.
#
# Required tools: yq (mikefarah/yq >= 4.18), envsubst
#
# Usage:
#   ./render-routes.sh <inventory.yaml> --output-dir /secure/path/rendered \
#     [--tls-cert <path>] [--tls-key <path>] [--tls-ca <path>]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TEMPLATE="$SCRIPT_DIR/../openshift/consumer-route.yaml.tpl"

# ── Argument parsing ────────────────────────────────────────────────

INVENTORY=""
OUTPUT_DIR=""
TLS_CERT=""
TLS_KEY=""
TLS_CA=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    --tls-cert)   TLS_CERT="$2";  shift 2 ;;
    --tls-key)    TLS_KEY="$2";   shift 2 ;;
    --tls-ca)     TLS_CA="$2";    shift 2 ;;
    -*)           echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
    *)
      if [[ -z "$INVENTORY" ]]; then
        INVENTORY="$1"; shift
      else
        echo "ERROR: unexpected argument: $1" >&2; exit 1
      fi
      ;;
  esac
done

if [[ -z "$INVENTORY" ]]; then
  echo "Usage: $0 <inventory.yaml> --output-dir <dir> [--tls-cert <path>] [--tls-key <path>] [--tls-ca <path>]" >&2
  exit 1
fi

if [[ -z "$OUTPUT_DIR" ]]; then
  echo "ERROR: --output-dir is required" >&2
  exit 1
fi

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory not found: $INVENTORY" >&2
  exit 1
fi

if [[ ! -f "$TEMPLATE" ]]; then
  echo "ERROR: template not found: $TEMPLATE" >&2
  exit 1
fi

# ── Validate output directory is outside the repository ─────────────

REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_ABS="$(mkdir -p "$OUTPUT_DIR" && cd "$OUTPUT_DIR" && pwd)"

if [[ "$OUTPUT_ABS" == "$REPO_ROOT"* ]]; then
  echo "ERROR: output directory must be outside the repository" >&2
  echo "  repo root:  $REPO_ROOT" >&2
  echo "  output dir: $OUTPUT_ABS" >&2
  exit 1
fi

# ── Validate TLS arguments ──────────────────────────────────────────

if [[ -n "$TLS_CERT" && -z "$TLS_KEY" ]] || [[ -z "$TLS_CERT" && -n "$TLS_KEY" ]]; then
  echo "ERROR: --tls-cert and --tls-key must both be provided" >&2
  exit 1
fi

for f in "$TLS_CERT" "$TLS_KEY" "$TLS_CA"; do
  if [[ -n "$f" && ! -f "$f" ]]; then
    echo "ERROR: file not found: $f" >&2
    exit 1
  fi
done

# ── Read inventory ──────────────────────────────────────────────────

GLOBAL_HOSTNAME=$(yq eval '.globalHostname' "$INVENTORY")
NAMESPACE=$(yq eval '.namespace // "grid-system"' "$INVENTORY")
SERVICE_NAME=$(yq eval '.serviceName // "consumer-gateway"' "$INVENTORY")
SERVICE_PORT=$(yq eval '.servicePort // "8080"' "$INVENTORY")
TLS_TERMINATION=$(yq eval '.tlsTermination // "edge"' "$INVENTORY")

if [[ -z "$GLOBAL_HOSTNAME" || "$GLOBAL_HOSTNAME" == "null" ]]; then
  echo "ERROR: required inventory field missing: globalHostname" >&2
  exit 1
fi

SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")
if [[ -z "$SITE_NAMES" ]]; then
  echo "ERROR: no sites defined in inventory" >&2
  exit 1
fi

# ── Render ──────────────────────────────────────────────────────────

umask 077

ENVSUBST_VARS='${ROUTE_NAME} ${ROUTE_HOSTNAME} ${NAMESPACE} ${SERVICE_NAME} ${SERVICE_PORT} ${TLS_TERMINATION}'

RENDERED=0

for SITE in $SITE_NAMES; do
  ORIGIN_HOSTNAME=$(yq eval ".sites.${SITE}.originHostname" "$INVENTORY")

  if [[ -z "$ORIGIN_HOSTNAME" || "$ORIGIN_HOSTNAME" == "null" ]]; then
    echo "ERROR: site '$SITE' missing originHostname" >&2
    exit 1
  fi

  # Origin Route (direct per-site hostname for health checks)
  export ROUTE_NAME="consumer-edge-origin-${SITE}"
  export ROUTE_HOSTNAME="$ORIGIN_HOSTNAME"
  export NAMESPACE SERVICE_NAME SERVICE_PORT TLS_TERMINATION

  ORIGIN_FILE="$OUTPUT_ABS/${SITE}-origin-route.yaml"
  envsubst "$ENVSUBST_VARS" < "$TEMPLATE" > "$ORIGIN_FILE"

  if [[ -n "$TLS_CERT" ]]; then
    CERT_PEM=$(cat "$TLS_CERT")
    KEY_PEM=$(cat "$TLS_KEY")
    yq eval -i ".spec.tls.certificate = \"$CERT_PEM\"" "$ORIGIN_FILE"
    yq eval -i ".spec.tls.key = \"$KEY_PEM\"" "$ORIGIN_FILE"
    if [[ -n "$TLS_CA" ]]; then
      CA_PEM=$(cat "$TLS_CA")
      yq eval -i ".spec.tls.caCertificate = \"$CA_PEM\"" "$ORIGIN_FILE"
    fi
  fi

  echo "  rendered: $ORIGIN_FILE"
  RENDERED=$((RENDERED + 1))

  # Global Route (shared public hostname accepted on every cluster)
  export ROUTE_NAME="consumer-edge-global-${SITE}"
  export ROUTE_HOSTNAME="$GLOBAL_HOSTNAME"

  GLOBAL_FILE="$OUTPUT_ABS/${SITE}-global-route.yaml"
  envsubst "$ENVSUBST_VARS" < "$TEMPLATE" > "$GLOBAL_FILE"

  if [[ -n "$TLS_CERT" ]]; then
    yq eval -i ".spec.tls.certificate = \"$CERT_PEM\"" "$GLOBAL_FILE"
    yq eval -i ".spec.tls.key = \"$KEY_PEM\"" "$GLOBAL_FILE"
    if [[ -n "$TLS_CA" ]]; then
      yq eval -i ".spec.tls.caCertificate = \"$CA_PEM\"" "$GLOBAL_FILE"
    fi
  fi

  echo "  rendered: $GLOBAL_FILE"
  RENDERED=$((RENDERED + 1))
done

# ── Regional Route (if configured) ─────────────────────────────────

REGIONAL_HOSTNAME=$(yq eval '.regionalHostname // ""' "$INVENTORY")
if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
  REGIONAL_SITES=$(yq eval '.regionalSites[]' "$INVENTORY" 2>/dev/null || true)

  for SITE in $REGIONAL_SITES; do
    export ROUTE_NAME="consumer-edge-regional-${SITE}"
    export ROUTE_HOSTNAME="$REGIONAL_HOSTNAME"

    REGIONAL_FILE="$OUTPUT_ABS/${SITE}-regional-route.yaml"
    envsubst "$ENVSUBST_VARS" < "$TEMPLATE" > "$REGIONAL_FILE"

    if [[ -n "$TLS_CERT" ]]; then
      yq eval -i ".spec.tls.certificate = \"$CERT_PEM\"" "$REGIONAL_FILE"
      yq eval -i ".spec.tls.key = \"$KEY_PEM\"" "$REGIONAL_FILE"
      if [[ -n "$TLS_CA" ]]; then
        yq eval -i ".spec.tls.caCertificate = \"$CA_PEM\"" "$REGIONAL_FILE"
      fi
    fi

    echo "  rendered: $REGIONAL_FILE"
    RENDERED=$((RENDERED + 1))
  done
fi

echo ""
echo "Rendered $RENDERED Route manifests to $OUTPUT_ABS"
echo "Apply per-site with: kubectl --context <context> apply -f <file>"
