#!/usr/bin/env bash
# Verify OpenShift Routes are admitted and edge origins are reachable.
#
# Checks Route admission, routerCanonicalHostname (the DNS target for
# Route 53 records), DNS resolution, TLS handshake, and optionally
# inference via POST.
#
# Required tools: kubectl, yq (mikefarah/yq >= 4.18)
# Optional tools: curl (for --test-inference), dig (for DNS checks),
#                 openssl (for TLS verification)
#
# Usage:
#   ./verify-origins.sh <inventory.yaml> [--test-inference]
set -euo pipefail

INVENTORY=""
TEST_INFERENCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --test-inference) TEST_INFERENCE=true; shift ;;
    -*)               echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
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
  echo "Usage: $0 <inventory.yaml> [--test-inference]" >&2
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

NAMESPACE=$(yq eval '.namespace // "grid-system"' "$INVENTORY")
SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")

REGIONAL_HOSTNAME=$(yq eval '.regionalHostname // ""' "$INVENTORY")
REGIONAL_SITES=$(yq eval '.regionalSites[]' "$INVENTORY" 2>/dev/null || true)

if [[ -z "$SITE_NAMES" ]]; then
  echo "ERROR: no sites defined in inventory" >&2
  exit 1
fi

echo "=== Route admission and DNS targets ==="
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  ORIGIN_HOSTNAME=$(yq eval ".sites.${SITE}.originHostname" "$INVENTORY")

  echo "--- Site: $SITE (context: $CONTEXT) ---"

  ORIGIN_ROUTE="consumer-edge-origin-${SITE}"
  GLOBAL_ROUTE="consumer-edge-global-${SITE}"

  # Check origin Route admission
  ORIGIN_ADMITTED=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get route "$ORIGIN_ROUTE" \
    -o jsonpath='{.status.ingress[0].conditions[?(@.type=="Admitted")].status}' 2>/dev/null || echo "")
  if [[ "$ORIGIN_ADMITTED" == "True" ]]; then
    echo "  PASS  origin Route $ORIGIN_ROUTE admitted"
  else
    echo "  FAIL  origin Route $ORIGIN_ROUTE not admitted (status: $ORIGIN_ADMITTED)" >&2
    ERRORS=$((ERRORS + 1))
  fi

  # Check global Route admission
  GLOBAL_ADMITTED=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get route "$GLOBAL_ROUTE" \
    -o jsonpath='{.status.ingress[0].conditions[?(@.type=="Admitted")].status}' 2>/dev/null || echo "")
  if [[ "$GLOBAL_ADMITTED" == "True" ]]; then
    echo "  PASS  global Route $GLOBAL_ROUTE admitted"
  else
    echo "  FAIL  global Route $GLOBAL_ROUTE not admitted (status: $GLOBAL_ADMITTED)" >&2
    ERRORS=$((ERRORS + 1))
  fi

  # Check regional Route admission (if this site is in the regional set)
  if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
    if echo "$REGIONAL_SITES" | grep -qxF "$SITE"; then
      REGIONAL_ROUTE="consumer-edge-regional-${SITE}"
      REGIONAL_ADMITTED=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get route "$REGIONAL_ROUTE" \
        -o jsonpath='{.status.ingress[0].conditions[?(@.type=="Admitted")].status}' 2>/dev/null || echo "")
      if [[ "$REGIONAL_ADMITTED" == "True" ]]; then
        echo "  PASS  regional Route $REGIONAL_ROUTE admitted"
      else
        echo "  FAIL  regional Route $REGIONAL_ROUTE not admitted (status: $REGIONAL_ADMITTED)" >&2
        ERRORS=$((ERRORS + 1))
      fi
    fi
  fi

  # Read routerCanonicalHostname (the DNS CNAME target)
  CANONICAL=$(kubectl --context "$CONTEXT" -n "$NAMESPACE" get route "$ORIGIN_ROUTE" \
    -o jsonpath='{.status.ingress[0].routerCanonicalHostname}' 2>/dev/null || echo "")
  if [[ -n "$CANONICAL" ]]; then
    echo "  INFO  routerCanonicalHostname: $CANONICAL"
    echo "        (use this as the CNAME target for Route 53 records)"
  else
    echo "  WARN  routerCanonicalHostname not available" >&2
  fi

  # DNS resolution of origin hostname
  if command -v dig &>/dev/null; then
    if [[ -n "$ORIGIN_HOSTNAME" && "$ORIGIN_HOSTNAME" != "null" ]]; then
      DNS_RESULT=$(dig +short "$ORIGIN_HOSTNAME" 2>/dev/null || echo "")
      if [[ -n "$DNS_RESULT" ]]; then
        echo "  PASS  DNS resolves: $ORIGIN_HOSTNAME -> $(echo "$DNS_RESULT" | head -1)"
      else
        echo "  SKIP  DNS not yet configured for $ORIGIN_HOSTNAME"
      fi
    fi
  fi

  # TLS handshake to origin (if DNS resolves)
  if command -v openssl &>/dev/null && [[ -n "$ORIGIN_HOSTNAME" && "$ORIGIN_HOSTNAME" != "null" ]]; then
    DNS_CHECK=$(dig +short "$ORIGIN_HOSTNAME" 2>/dev/null || echo "")
    if [[ -n "$DNS_CHECK" ]]; then
      VERIFY=$(echo | openssl s_client -connect "${ORIGIN_HOSTNAME}:443" \
        -servername "$ORIGIN_HOSTNAME" 2>/dev/null | grep 'Verify return code' || echo "")
      if [[ "$VERIFY" == *"0 (ok)"* ]]; then
        echo "  PASS  TLS handshake verified: $ORIGIN_HOSTNAME"
      elif [[ -n "$VERIFY" ]]; then
        echo "  WARN  TLS handshake completed but verification failed: $VERIFY"
      else
        echo "  FAIL  TLS handshake failed: $ORIGIN_HOSTNAME" >&2
        ERRORS=$((ERRORS + 1))
      fi
    fi
  fi

  # Inference test (POST /v1/chat/completions)
  if [[ "$TEST_INFERENCE" == "true" ]] && command -v curl &>/dev/null; then
    if command -v dig &>/dev/null; then
      DNS_CHECK=$(dig +short "$ORIGIN_HOSTNAME" 2>/dev/null || echo "")
    else
      DNS_CHECK="skip-dig"
    fi
    if [[ -n "$DNS_CHECK" ]]; then
      HTTP_CODE=$(curl -so /dev/null -w '%{http_code}' --connect-timeout 10 --max-time 30 \
        -X POST "https://${ORIGIN_HOSTNAME}/v1/chat/completions" \
        -H 'Content-Type: application/json' \
        -d '{"model":"sim-model-v1","messages":[{"role":"user","content":"test"}]}' \
        2>/dev/null || echo "000")
      if [[ "$HTTP_CODE" == "200" ]]; then
        echo "  PASS  inference POST: HTTP $HTTP_CODE"
      else
        echo "  FAIL  inference POST: HTTP $HTTP_CODE (expected 200)" >&2
        ERRORS=$((ERRORS + 1))
      fi
    else
      echo "  SKIP  inference test: DNS not configured for $ORIGIN_HOSTNAME"
    fi
  fi

  echo ""
done

# ── Summary ─────────────────────────────────────────────────────────

if [[ "$ERRORS" -eq 0 ]]; then
  echo "Origin verification passed."
else
  echo "Origin verification failed with $ERRORS error(s)." >&2
  exit 1
fi
