#!/usr/bin/env bash
# Remove edge entry Routes from all clusters in the inventory.
#
# Deletes only the exact Routes created by render-routes.sh:
#   consumer-edge-origin-${site}
#   consumer-edge-global-${site}
#   consumer-edge-regional-${site}  (if regional hostname is configured)
#
# Does NOT touch Grid Helm releases, namespaces, CRDs, Route 53
# records, health checks, or any other resources.
#
# Route 53 record and health-check cleanup is a separate operator
# step using the aws CLI. See the README for exact cleanup commands.
#
# Required tools: kubectl, yq (mikefarah/yq >= 4.18)
#
# Usage:
#   ./uninstall-routes.sh <inventory.yaml> [--delete-tls]
set -euo pipefail

INVENTORY=""
DELETE_TLS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --delete-tls) DELETE_TLS=true; shift ;;
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
  echo "Usage: $0 <inventory.yaml> [--delete-tls]" >&2
  exit 1
fi

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory not found: $INVENTORY" >&2
  exit 1
fi

NAMESPACE=$(yq eval '.namespace // "grid-system"' "$INVENTORY")
SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")
REGIONAL_HOSTNAME=$(yq eval '.regionalHostname // ""' "$INVENTORY")
REGIONAL_SITES=$(yq eval '.regionalSites[]' "$INVENTORY" 2>/dev/null || true)

if [[ -z "$SITE_NAMES" ]]; then
  echo "ERROR: no sites defined in inventory" >&2
  exit 1
fi

DELETED=0
ERRORS=0

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")

  if [[ -z "$CONTEXT" || "$CONTEXT" == "null" ]]; then
    echo "SKIP  site $SITE: no context" >&2
    continue
  fi

  echo "--- Site: $SITE (context: $CONTEXT) ---"

  for ROUTE_NAME in "consumer-edge-origin-${SITE}" "consumer-edge-global-${SITE}"; do
    if kubectl --context "$CONTEXT" -n "$NAMESPACE" delete route "$ROUTE_NAME" --ignore-not-found 2>/dev/null; then
      echo "  OK    $ROUTE_NAME"
      DELETED=$((DELETED + 1))
    else
      echo "  FAIL  $ROUTE_NAME" >&2
      ERRORS=$((ERRORS + 1))
    fi
  done

  # Delete regional Route if this site is in the regional set
  if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
    if echo "$REGIONAL_SITES" | grep -qxF "$SITE"; then
      ROUTE_NAME="consumer-edge-regional-${SITE}"
      if kubectl --context "$CONTEXT" -n "$NAMESPACE" delete route "$ROUTE_NAME" --ignore-not-found 2>/dev/null; then
        echo "  OK    $ROUTE_NAME"
        DELETED=$((DELETED + 1))
      else
        echo "  FAIL  $ROUTE_NAME" >&2
        ERRORS=$((ERRORS + 1))
      fi
    fi
  fi

  if [[ "$DELETE_TLS" == "true" ]]; then
    TLS_SECRET="consumer-edge-tls-${SITE}"
    if kubectl --context "$CONTEXT" -n "$NAMESPACE" delete secret "$TLS_SECRET" --ignore-not-found 2>/dev/null; then
      echo "  OK    Secret $TLS_SECRET"
    else
      echo "  FAIL  Secret $TLS_SECRET" >&2
      ERRORS=$((ERRORS + 1))
    fi
  fi

  echo ""
done

echo "Deleted $DELETED Route(s)."

if [[ "$ERRORS" -gt 0 ]]; then
  echo "$ERRORS error(s) during cleanup." >&2
  exit 1
fi
