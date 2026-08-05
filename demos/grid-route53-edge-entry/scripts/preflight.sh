#!/usr/bin/env bash
# Pre-flight checks for Route 53 edge entry demo.
#
# Validates required tools, inventory structure, cluster access, consumer
# gateway readiness, and optionally TLS certificate SANs. Does NOT check
# Route 53 records or make any AWS mutations.
#
# Required tools: kubectl, yq (mikefarah/yq >= 4.18), envsubst
# Optional tools: openssl (for TLS SAN validation), aws (for caller check)
#
# Usage:
#   ./preflight.sh <inventory.yaml> [--tls-cert <path>]
set -euo pipefail

INVENTORY=""
TLS_CERT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tls-cert) TLS_CERT="$2"; shift 2 ;;
    -*)         echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
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
  echo "Usage: $0 <inventory.yaml> [--tls-cert <path>]" >&2
  exit 1
fi

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory not found: $INVENTORY" >&2
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

# ── Required tools ──────────────────────────────────────────────────

echo "=== Required tools ==="
check "kubectl" command -v kubectl
check "yq" command -v yq

if command -v yq &>/dev/null; then
  YQ_VER=$(yq --version 2>&1 | grep -oP '\d+\.\d+' | head -1)
  YQ_MAJOR=$(echo "$YQ_VER" | cut -d. -f1)
  YQ_MINOR=$(echo "$YQ_VER" | cut -d. -f2)
  if [[ "$YQ_MAJOR" -ge 4 ]] && [[ "$YQ_MINOR" -ge 18 ]]; then
    echo "  PASS  yq >= 4.18 ($YQ_VER)"
  else
    echo "  FAIL  yq >= 4.18 required (found $YQ_VER)" >&2
    ERRORS=$((ERRORS + 1))
  fi
fi

check "envsubst" command -v envsubst
check "jq" command -v jq

echo ""
echo "=== Optional tools ==="
if command -v aws &>/dev/null; then
  echo "  PASS  aws CLI"
else
  echo "  SKIP  aws CLI not found (needed for Route 53 operations)"
fi

if command -v dig &>/dev/null; then
  echo "  PASS  dig"
else
  echo "  SKIP  dig not found (needed for DNS verification)"
fi

if command -v curl &>/dev/null; then
  echo "  PASS  curl"
else
  echo "  SKIP  curl not found (needed for inference tests)"
fi

if command -v openssl &>/dev/null; then
  echo "  PASS  openssl"
else
  echo "  SKIP  openssl not found (needed for TLS verification)"
fi

# ── Inventory structure ─────────────────────────────────────────────

echo ""
echo "=== Inventory structure ==="

for field in hostedZoneId hostedZoneDomain globalHostname; do
  val=$(yq eval ".$field" "$INVENTORY")
  if [[ -n "$val" && "$val" != "null" ]]; then
    echo "  PASS  $field: $val"
  else
    echo "  FAIL  $field: missing or empty" >&2
    ERRORS=$((ERRORS + 1))
  fi
done

GLOBAL_HOSTNAME=$(yq eval '.globalHostname' "$INVENTORY")
REGIONAL_HOSTNAME=$(yq eval '.regionalHostname // ""' "$INVENTORY")
NAMESPACE=$(yq eval '.namespace // "grid-system"' "$INVENTORY")
SERVICE_NAME=$(yq eval '.serviceName // "consumer-gateway"' "$INVENTORY")

if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
  echo "  PASS  regionalHostname: $REGIONAL_HOSTNAME"

  REGIONAL_SITES=$(yq eval '.regionalSites[]' "$INVENTORY" 2>/dev/null || true)
  if [[ -z "$REGIONAL_SITES" ]]; then
    echo "  FAIL  regionalSites: required when regionalHostname is set" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "  PASS  regionalSites: $REGIONAL_SITES"
  fi
fi

SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")
if [[ -z "$SITE_NAMES" ]]; then
  echo "  FAIL  no sites defined" >&2
  ERRORS=$((ERRORS + 1))
fi

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  ORIGIN=$(yq eval ".sites.${SITE}.originHostname" "$INVENTORY")
  REGION=$(yq eval ".sites.${SITE}.awsRegion" "$INVENTORY")

  if [[ -z "$CONTEXT" || "$CONTEXT" == "null" ]]; then
    echo "  FAIL  site $SITE: missing context" >&2
    ERRORS=$((ERRORS + 1))
    continue
  fi

  if [[ -z "$ORIGIN" || "$ORIGIN" == "null" ]]; then
    echo "  FAIL  site $SITE: missing originHostname" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "  PASS  site $SITE: originHostname $ORIGIN"
  fi

  if [[ -z "$REGION" || "$REGION" == "null" ]]; then
    echo "  FAIL  site $SITE: missing awsRegion" >&2
    ERRORS=$((ERRORS + 1))
  else
    echo "  PASS  site $SITE: awsRegion $REGION"
  fi
done

# ── Cluster access ──────────────────────────────────────────────────

echo ""
echo "=== Cluster access ==="

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  [[ -z "$CONTEXT" || "$CONTEXT" == "null" ]] && continue

  check "site $SITE: cluster reachable (context: $CONTEXT)" \
    kubectl --context "$CONTEXT" cluster-info
done

# ── Consumer gateway Service ────────────────────────────────────────

echo ""
echo "=== Consumer gateway Service ==="

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  [[ -z "$CONTEXT" || "$CONTEXT" == "null" ]] && continue

  check "site $SITE: Service $SERVICE_NAME exists" \
    kubectl --context "$CONTEXT" -n "$NAMESPACE" get service "$SERVICE_NAME"
done

# ── AWS caller identity ────────────────────────────────────────────

if command -v aws &>/dev/null; then
  echo ""
  echo "=== AWS identity ==="

  CALLER=$(aws sts get-caller-identity --output json 2>/dev/null || echo "")
  if [[ -n "$CALLER" ]]; then
    ACCOUNT=$(echo "$CALLER" | jq -r '.Account')
    ARN=$(echo "$CALLER" | jq -r '.Arn')
    echo "  PASS  AWS caller: $ARN (account: $ACCOUNT)"
  else
    echo "  FAIL  AWS caller identity not available" >&2
    ERRORS=$((ERRORS + 1))
  fi
fi

# ── TLS certificate SANs ───────────────────────────────────────────

if [[ -n "$TLS_CERT" ]]; then
  echo ""
  echo "=== TLS certificate ==="

  if [[ ! -f "$TLS_CERT" ]]; then
    echo "  FAIL  certificate not found: $TLS_CERT" >&2
    ERRORS=$((ERRORS + 1))
  elif command -v openssl &>/dev/null; then
    SANS=$(openssl x509 -in "$TLS_CERT" -noout -ext subjectAltName 2>/dev/null \
      | grep -oP 'DNS:[^ ,]+' | sed 's/DNS://' || true)

    check_san() {
      local hostname="$1"
      if echo "$SANS" | grep -qxF "$hostname"; then
        return 0
      fi
      for san in $SANS; do
        if [[ "$san" == "*."* ]]; then
          wildcard_suffix="${san#\*.}"
          if [[ "$hostname" == *".$wildcard_suffix" ]]; then
            parent="${hostname%%.*}"
            remainder="${hostname#*.}"
            if [[ "$remainder" == "$wildcard_suffix" && "$parent" != *"."* ]]; then
              return 0
            fi
          fi
        fi
      done
      return 1
    }

    check "SAN covers $GLOBAL_HOSTNAME" check_san "$GLOBAL_HOSTNAME"

    if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
      check "SAN covers $REGIONAL_HOSTNAME" check_san "$REGIONAL_HOSTNAME"
    fi

    for SITE in $SITE_NAMES; do
      ORIGIN=$(yq eval ".sites.${SITE}.originHostname" "$INVENTORY")
      [[ -z "$ORIGIN" || "$ORIGIN" == "null" ]] && continue
      check "SAN covers $ORIGIN" check_san "$ORIGIN"
    done

    NOT_AFTER=$(openssl x509 -in "$TLS_CERT" -noout -enddate 2>/dev/null \
      | sed 's/notAfter=//')
    if [[ -n "$NOT_AFTER" ]]; then
      echo "  INFO  certificate expires: $NOT_AFTER"
    fi
  else
    echo "  SKIP  openssl not available for SAN validation"
  fi
fi

# ── Summary ─────────────────────────────────────────────────────────

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  echo "Preflight passed."
else
  echo "Preflight failed with $ERRORS error(s)." >&2
  exit 1
fi
