#!/usr/bin/env bash
# Verify Route 53 DNS records for the edge entry demo.
#
# Checks authoritative and recursive DNS answers for the global and
# regional hostnames. Optionally verifies health-check status and
# weighted distribution.
#
# Does NOT create or modify any DNS records.
#
# Required tools: dig, yq (mikefarah/yq >= 4.18)
# Optional tools: aws (for health-check status), curl (for inference)
#
# Usage:
#   ./verify-dns.sh <inventory.yaml> [--nameserver <ns>] [--test-inference]
set -euo pipefail

INVENTORY=""
NAMESERVER=""
TEST_INFERENCE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nameserver)     NAMESERVER="$2"; shift 2 ;;
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
  echo "Usage: $0 <inventory.yaml> [--nameserver <ns>] [--test-inference]" >&2
  exit 1
fi

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory not found: $INVENTORY" >&2
  exit 1
fi

if ! command -v dig &>/dev/null; then
  echo "ERROR: dig is required for DNS verification" >&2
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

GLOBAL_HOSTNAME=$(yq eval '.globalHostname' "$INVENTORY")
REGIONAL_HOSTNAME=$(yq eval '.regionalHostname // ""' "$INVENTORY")
SET_ID_PREFIX=$(yq eval '.setIdentifierPrefix // "grid-edge"' "$INVENTORY")
SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")

# ── Origin CNAME records ───────────────────────────────────────────

echo "=== Origin CNAME records ==="

for SITE in $SITE_NAMES; do
  ORIGIN_HOSTNAME=$(yq eval ".sites.${SITE}.originHostname" "$INVENTORY")

  CNAME=$(dig +short "$ORIGIN_HOSTNAME" CNAME 2>/dev/null || echo "")
  if [[ -n "$CNAME" ]]; then
    echo "  PASS  $ORIGIN_HOSTNAME -> $CNAME"
  else
    echo "  FAIL  $ORIGIN_HOSTNAME: no CNAME record" >&2
    ERRORS=$((ERRORS + 1))
  fi
done

# ── Global hostname (authoritative) ────────────────────────────────

echo ""
echo "=== Global hostname: $GLOBAL_HOSTNAME ==="

if [[ -n "$NAMESERVER" ]]; then
  echo "  INFO  querying authoritative nameserver: $NAMESERVER"
  echo ""
  echo "  --- Authoritative sample (20 queries) ---"
  for _ in $(seq 1 20); do
    dig "@$NAMESERVER" "$GLOBAL_HOSTNAME" CNAME +short 2>/dev/null
  done | sort | uniq -c | sort -rn | while read -r count target; do
    echo "    $count  $target"
  done
else
  echo "  INFO  no --nameserver specified; using recursive resolver"
fi

echo ""
echo "  --- Recursive resolver ---"
RECURSIVE=$(dig +short "$GLOBAL_HOSTNAME" CNAME 2>/dev/null || echo "")
if [[ -n "$RECURSIVE" ]]; then
  echo "  PASS  recursive: $GLOBAL_HOSTNAME -> $RECURSIVE"
else
  echo "  FAIL  recursive: $GLOBAL_HOSTNAME: no answer" >&2
  ERRORS=$((ERRORS + 1))
fi

# ── Regional hostname ──────────────────────────────────────────────

if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
  echo ""
  echo "=== Regional hostname: $REGIONAL_HOSTNAME ==="

  if [[ -n "$NAMESERVER" ]]; then
    echo "  --- Authoritative sample (20 queries) ---"
    for _ in $(seq 1 20); do
      dig "@$NAMESERVER" "$REGIONAL_HOSTNAME" CNAME +short 2>/dev/null
    done | sort | uniq -c | sort -rn | while read -r count target; do
      echo "    $count  $target"
    done
  fi

  echo ""
  RECURSIVE=$(dig +short "$REGIONAL_HOSTNAME" CNAME 2>/dev/null || echo "")
  if [[ -n "$RECURSIVE" ]]; then
    echo "  PASS  recursive: $REGIONAL_HOSTNAME -> $RECURSIVE"
  else
    echo "  FAIL  recursive: $REGIONAL_HOSTNAME: no answer" >&2
    ERRORS=$((ERRORS + 1))
  fi
fi

# ── Health-check status ────────────────────────────────────────────

if command -v aws &>/dev/null; then
  echo ""
  echo "=== Health-check status ==="
  echo "  INFO  listing health checks with ${SET_ID_PREFIX} prefix"

  HEALTH_CHECKS=$(aws route53 list-health-checks \
    --query "HealthChecks[?contains(CallerReference, \`${SET_ID_PREFIX}\`)].[Id,HealthCheckConfig.FullyQualifiedDomainName]" \
    --output text 2>/dev/null || echo "")

  if [[ -n "$HEALTH_CHECKS" ]]; then
    while IFS=$'\t' read -r HC_ID HC_FQDN; do
      STATUS=$(aws route53 get-health-check-status --health-check-id "$HC_ID" \
        --query 'HealthCheckObservations[0].StatusReport.Status' \
        --output text 2>/dev/null || echo "unknown")
      if [[ "$STATUS" == *"Success"* ]]; then
        echo "  PASS  $HC_FQDN ($HC_ID): $STATUS"
      else
        echo "  FAIL  $HC_FQDN ($HC_ID): $STATUS" >&2
        ERRORS=$((ERRORS + 1))
      fi
    done <<< "$HEALTH_CHECKS"
  else
    echo "  SKIP  no health checks found with ${SET_ID_PREFIX} caller reference"
  fi
fi

# ── Inference through global hostname ──────────────────────────────

if [[ "$TEST_INFERENCE" == "true" ]] && command -v curl &>/dev/null; then
  echo ""
  echo "=== Inference through global hostname ==="

  RESPONSE=$(curl -s --connect-timeout 10 --max-time 30 \
    -X POST "https://${GLOBAL_HOSTNAME}/v1/chat/completions" \
    -H 'Content-Type: application/json' \
    -d '{"model":"sim-model-v1","messages":[{"role":"user","content":"test"}]}' \
    -w '\n%{http_code}' 2>/dev/null || echo -e "\n000")

  HTTP_CODE=$(echo "$RESPONSE" | tail -1)
  BODY=$(echo "$RESPONSE" | sed '$d')

  if [[ "$HTTP_CODE" == "200" ]]; then
    echo "  PASS  inference POST: HTTP $HTTP_CODE"
    echo "  INFO  response (first 200 chars): ${BODY:0:200}"
  else
    echo "  FAIL  inference POST: HTTP $HTTP_CODE (expected 200)" >&2
    ERRORS=$((ERRORS + 1))
  fi
fi

# ── TTL observation ────────────────────────────────────────────────

echo ""
echo "=== TTL observation ==="

TTL_LINE=$(dig "$GLOBAL_HOSTNAME" CNAME +noall +answer 2>/dev/null | head -1)
if [[ -n "$TTL_LINE" ]]; then
  OBSERVED_TTL=$(echo "$TTL_LINE" | awk '{print $2}')
  echo "  INFO  observed recursive TTL: ${OBSERVED_TTL}s"
  echo "  INFO  recursive resolvers may cache beyond the configured TTL"
  echo "  INFO  existing HTTP connections do not re-resolve DNS"
else
  echo "  SKIP  no answer to observe TTL"
fi

# ── Summary ─────────────────────────────────────────────────────────

echo ""
if [[ "$ERRORS" -eq 0 ]]; then
  echo "DNS verification passed."
else
  echo "DNS verification failed with $ERRORS error(s)." >&2
  exit 1
fi
