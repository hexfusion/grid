#!/usr/bin/env bash
# Phase A: Render Route 53 health-check configurations from inventory and
# captured runtime mappings.
#
# Generates one TCP 443 health-check config JSON per site. The output
# files are ready to pass directly to:
#
#   aws route53 create-health-check \
#     --caller-reference "<prefix>-<site>-$(date +%s)" \
#     --health-check-config file://<output-dir>/health-check-<site>.json
#
# After creating health checks, record the returned HealthCheck.Id for
# each site in a mappings file and pass it to materialize-records.sh
# (Phase B).
#
# Required tools: yq (mikefarah/yq >= 4.18), jq
#
# Usage:
#   ./render-health-checks.sh <inventory.yaml> <mappings.yaml> \
#     --output-dir /secure/path
set -euo pipefail

INVENTORY=""
MAPPINGS=""
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    -*)           echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
    *)
      if [[ -z "$INVENTORY" ]]; then
        INVENTORY="$1"; shift
      elif [[ -z "$MAPPINGS" ]]; then
        MAPPINGS="$1"; shift
      else
        echo "ERROR: unexpected argument: $1" >&2; exit 1
      fi
      ;;
  esac
done

if [[ -z "$INVENTORY" || -z "$MAPPINGS" ]]; then
  echo "Usage: $0 <inventory.yaml> <mappings.yaml> --output-dir <dir>" >&2
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
if [[ ! -f "$MAPPINGS" ]]; then
  echo "ERROR: mappings not found: $MAPPINGS" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_ABS="$(mkdir -p "$OUTPUT_DIR" && cd "$OUTPUT_DIR" && pwd)"

if [[ "$OUTPUT_ABS" == "$REPO_ROOT"* ]]; then
  echo "ERROR: output directory must be outside the repository" >&2
  exit 1
fi

umask 077

SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")
SET_ID_PREFIX=$(yq eval '.setIdentifierPrefix // "grid-edge"' "$INVENTORY")

if [[ -z "$SITE_NAMES" ]]; then
  echo "ERROR: no sites defined in inventory" >&2
  exit 1
fi

echo "Phase A: Rendering health-check configurations"
echo ""

for SITE in $SITE_NAMES; do
  ROUTER_HOSTNAME=$(yq eval ".sites.${SITE}.routerCanonicalHostname // \"\"" "$MAPPINGS")

  if [[ -z "$ROUTER_HOSTNAME" || "$ROUTER_HOSTNAME" == "null" ]] ||
     echo "$ROUTER_HOSTNAME" | grep -qiE 'PLACEHOLDER|<[a-zA-Z]'; then
    echo "ERROR: site '$SITE' missing resolved routerCanonicalHostname in mappings" >&2
    exit 1
  fi

  HC_FILE="$OUTPUT_ABS/health-check-${SITE}.json"
  jq -n \
    --arg fqdn "$ROUTER_HOSTNAME" \
    '{
      Type: "TCP",
      Port: 443,
      FullyQualifiedDomainName: $fqdn,
      RequestInterval: 30,
      FailureThreshold: 3
    }' > "$HC_FILE"

  echo "  rendered: $HC_FILE"
done

echo ""
echo "Create each health check with:"
echo "  aws route53 create-health-check \\"
echo "    --caller-reference \"${SET_ID_PREFIX}-<site>-\$(date +%s)\" \\"
echo "    --health-check-config file://<health-check-file>"
echo ""
echo "Then record each returned HealthCheck.Id in the mappings file"
echo "for Phase B."
