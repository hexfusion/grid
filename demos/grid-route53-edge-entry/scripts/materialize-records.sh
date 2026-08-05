#!/usr/bin/env bash
# Phase B: Materialize Route 53 record payloads from inventory and
# runtime mappings.
#
# Reads the inventory for DNS structure (hostedZoneId, hostnames, TTL,
# setIdentifierPrefix, awsRegion) and a mappings file for live runtime
# values (routerCanonicalHostname, healthCheckId per site).
#
# Validates all inputs and rejects:
#   - PLACEHOLDER values
#   - angle-bracket substitutions (<...>)
#   - empty routerCanonicalHostname or healthCheckId
#   - duplicate awsRegion values (latency routing constraint)
#   - duplicate SetIdentifier values
#
# Generates complete, AWS CLI-ready payloads under:
#   <output-dir>/create/   — CREATE change batches
#   <output-dir>/delete/   — exact matching DELETE change batches
#
# Every CREATE payload has a corresponding DELETE payload built from
# the same resolved values. The DELETE payload preserves every field
# required to identify the exact Route 53 record set.
#
# Required tools: yq (mikefarah/yq >= 4.18), jq
#
# Usage:
#   ./materialize-records.sh <inventory.yaml> <mappings.yaml> \
#     --output-dir /secure/path/records
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
mkdir -p "$OUTPUT_ABS/create" "$OUTPUT_ABS/delete"

# ── Read inventory ────────────────────────────────────────────────────

HOSTED_ZONE_ID=$(yq eval '.hostedZoneId' "$INVENTORY")
GLOBAL_HOSTNAME=$(yq eval '.globalHostname' "$INVENTORY")
REGIONAL_HOSTNAME=$(yq eval '.regionalHostname // ""' "$INVENTORY")
RECORD_TTL=$(yq eval '.recordTTL // 60' "$INVENTORY")
SET_ID_PREFIX=$(yq eval '.setIdentifierPrefix // "grid-edge"' "$INVENTORY")
SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")

if [[ -z "$HOSTED_ZONE_ID" || "$HOSTED_ZONE_ID" == "null" ]]; then
  echo "ERROR: inventory missing hostedZoneId" >&2
  exit 1
fi
if [[ -z "$GLOBAL_HOSTNAME" || "$GLOBAL_HOSTNAME" == "null" ]]; then
  echo "ERROR: inventory missing globalHostname" >&2
  exit 1
fi
if [[ -z "$SITE_NAMES" ]]; then
  echo "ERROR: no sites in inventory" >&2
  exit 1
fi

# ── Validation helpers ────────────────────────────────────────────────

reject_value() {
  local label="$1" value="$2"

  if [[ -z "$value" || "$value" == "null" ]]; then
    echo "ERROR: $label is empty or null" >&2
    return 1
  fi
  if echo "$value" | grep -qiE 'PLACEHOLDER'; then
    echo "ERROR: $label contains PLACEHOLDER: $value" >&2
    return 1
  fi
  if echo "$value" | grep -qE '<[a-zA-Z]'; then
    echo "ERROR: $label contains angle-bracket substitution: $value" >&2
    return 1
  fi
  return 0
}

# ── Validate mappings ─────────────────────────────────────────────────

echo "Phase B: Materializing Route 53 record payloads"
echo ""
echo "  hosted zone:       $HOSTED_ZONE_ID"
echo "  global hostname:   $GLOBAL_HOSTNAME"
echo "  regional hostname: ${REGIONAL_HOSTNAME:-none}"
echo "  record TTL:        $RECORD_TTL"
echo "  set ID prefix:     $SET_ID_PREFIX"
echo ""

ERRORS=0
declare -A RCH_MAP
declare -A HC_MAP

for SITE in $SITE_NAMES; do
  RCH=$(yq eval ".sites.${SITE}.routerCanonicalHostname // \"\"" "$MAPPINGS")
  HC_ID=$(yq eval ".sites.${SITE}.healthCheckId // \"\"" "$MAPPINGS")

  if ! reject_value "$SITE.routerCanonicalHostname" "$RCH"; then
    ERRORS=$((ERRORS + 1))
  else
    RCH_MAP[$SITE]="$RCH"
  fi

  if ! reject_value "$SITE.healthCheckId" "$HC_ID"; then
    ERRORS=$((ERRORS + 1))
  else
    HC_MAP[$SITE]="$HC_ID"
  fi
done

# ── Validate awsRegion uniqueness ─────────────────────────────────────

SEEN_REGIONS=""
SEEN_SET_IDS=""

for SITE in $SITE_NAMES; do
  AWS_REGION=$(yq eval ".sites.${SITE}.awsRegion" "$INVENTORY")

  if [[ -z "$AWS_REGION" || "$AWS_REGION" == "null" ]]; then
    echo "ERROR: site '$SITE' missing awsRegion in inventory" >&2
    ERRORS=$((ERRORS + 1))
    continue
  fi

  if echo "$SEEN_REGIONS" | grep -qxF "$AWS_REGION"; then
    echo "ERROR: duplicate awsRegion '$AWS_REGION' (site '$SITE')" >&2
    echo "  latency routing requires one record per unique Region per Name+Type" >&2
    ERRORS=$((ERRORS + 1))
  fi
  SEEN_REGIONS="${SEEN_REGIONS}${AWS_REGION}
"
done

if [[ "$ERRORS" -gt 0 ]]; then
  echo "" >&2
  echo "Mapping validation failed with $ERRORS error(s)." >&2
  exit 1
fi

echo "  all mappings validated"
echo ""

# ── Helper: build change batch ────────────────────────────────────────

build_batch() {
  local action="$1" comment="$2"
  shift 2
  local changes="$1"
  echo "$changes" | jq --arg action "$action" --arg comment "$comment" \
    '[.[] | .Action = $action] | {Comment: $comment, Changes: .}'
}

# ── Helper: check SetIdentifier uniqueness ────────────────────────────

check_set_id() {
  local setid="$1"
  if echo "$SEEN_SET_IDS" | grep -qxF "$setid"; then
    echo "ERROR: duplicate SetIdentifier '$setid'" >&2
    exit 1
  fi
  SEEN_SET_IDS="${SEEN_SET_IDS}${setid}
"
}

# ── Origin CNAME records ──────────────────────────────────────────────

echo "=== Origin CNAME records ==="

ORIGIN_RECORDS="[]"
for SITE in $SITE_NAMES; do
  ORIGIN_HOSTNAME=$(yq eval ".sites.${SITE}.originHostname" "$INVENTORY")
  RCH="${RCH_MAP[$SITE]}"

  ORIGIN_RECORDS=$(echo "$ORIGIN_RECORDS" | jq \
    --arg name "$ORIGIN_HOSTNAME" \
    --argjson ttl "$RECORD_TTL" \
    --arg value "$RCH" \
    '. + [{
      Action: "CREATE",
      ResourceRecordSet: {
        Name: $name,
        Type: "CNAME",
        TTL: $ttl,
        ResourceRecords: [{Value: $value}]
      }
    }]')
done

build_batch "CREATE" "Grid edge entry origin CNAME records" "$ORIGIN_RECORDS" \
  > "$OUTPUT_ABS/create/origin-records.json"
build_batch "DELETE" "Delete Grid edge entry origin CNAME records" "$ORIGIN_RECORDS" \
  > "$OUTPUT_ABS/delete/origin-records.json"
echo "  create: $OUTPUT_ABS/create/origin-records.json"
echo "  delete: $OUTPUT_ABS/delete/origin-records.json"

# ── Weighted records ──────────────────────────────────────────────────

echo ""
echo "=== Weighted records ==="

SITE_COUNT=$(echo "$SITE_NAMES" | wc -l)
WEIGHT=$((100 / SITE_COUNT))

WEIGHTED_RECORDS="[]"
for SITE in $SITE_NAMES; do
  RCH="${RCH_MAP[$SITE]}"
  HC_ID="${HC_MAP[$SITE]}"
  SET_ID="${SET_ID_PREFIX}-${SITE}"
  check_set_id "$SET_ID"

  WEIGHTED_RECORDS=$(echo "$WEIGHTED_RECORDS" | jq \
    --arg name "$GLOBAL_HOSTNAME" \
    --arg setid "$SET_ID" \
    --argjson weight "$WEIGHT" \
    --argjson ttl "$RECORD_TTL" \
    --arg value "$RCH" \
    --arg hcid "$HC_ID" \
    '. + [{
      Action: "CREATE",
      ResourceRecordSet: {
        Name: $name,
        Type: "CNAME",
        SetIdentifier: $setid,
        Weight: $weight,
        TTL: $ttl,
        ResourceRecords: [{Value: $value}],
        HealthCheckId: $hcid
      }
    }]')
done

build_batch "CREATE" "Grid edge entry weighted records" "$WEIGHTED_RECORDS" \
  > "$OUTPUT_ABS/create/weighted-records.json"
build_batch "DELETE" "Delete Grid edge entry weighted records" "$WEIGHTED_RECORDS" \
  > "$OUTPUT_ABS/delete/weighted-records.json"
echo "  create: $OUTPUT_ABS/create/weighted-records.json"
echo "  delete: $OUTPUT_ABS/delete/weighted-records.json"

# ── Latency records ───────────────────────────────────────────────────

echo ""
echo "=== Latency records ==="

LATENCY_RECORDS="[]"
for SITE in $SITE_NAMES; do
  RCH="${RCH_MAP[$SITE]}"
  HC_ID="${HC_MAP[$SITE]}"
  AWS_REGION=$(yq eval ".sites.${SITE}.awsRegion" "$INVENTORY")
  SET_ID="${SET_ID_PREFIX}-latency-${AWS_REGION}"
  check_set_id "$SET_ID"

  LATENCY_RECORDS=$(echo "$LATENCY_RECORDS" | jq \
    --arg name "$GLOBAL_HOSTNAME" \
    --arg setid "$SET_ID" \
    --arg region "$AWS_REGION" \
    --argjson ttl "$RECORD_TTL" \
    --arg value "$RCH" \
    --arg hcid "$HC_ID" \
    '. + [{
      Action: "CREATE",
      ResourceRecordSet: {
        Name: $name,
        Type: "CNAME",
        SetIdentifier: $setid,
        Region: $region,
        TTL: $ttl,
        ResourceRecords: [{Value: $value}],
        HealthCheckId: $hcid
      }
    }]')
done

build_batch "CREATE" "Grid edge entry latency records" "$LATENCY_RECORDS" \
  > "$OUTPUT_ABS/create/latency-records.json"
build_batch "DELETE" "Delete Grid edge entry latency records" "$LATENCY_RECORDS" \
  > "$OUTPUT_ABS/delete/latency-records.json"
echo "  create: $OUTPUT_ABS/create/latency-records.json"
echo "  delete: $OUTPUT_ABS/delete/latency-records.json"

# ── Regional records ──────────────────────────────────────────────────

if [[ -n "$REGIONAL_HOSTNAME" && "$REGIONAL_HOSTNAME" != "null" ]]; then
  echo ""
  echo "=== Regional records ==="

  REGIONAL_SITES=$(yq eval '.regionalSites[]' "$INVENTORY" 2>/dev/null || true)
  if [[ -z "$REGIONAL_SITES" ]]; then
    echo "ERROR: regionalSites required when regionalHostname is set" >&2
    exit 1
  fi

  R_SITE_COUNT=$(echo "$REGIONAL_SITES" | wc -l)
  R_WEIGHT=$((100 / R_SITE_COUNT))

  REGIONAL_RECORDS="[]"
  for SITE in $REGIONAL_SITES; do
    RCH="${RCH_MAP[$SITE]}"
    HC_ID="${HC_MAP[$SITE]}"
    SET_ID="${SET_ID_PREFIX}-regional-${SITE}"
    check_set_id "$SET_ID"

    REGIONAL_RECORDS=$(echo "$REGIONAL_RECORDS" | jq \
      --arg name "$REGIONAL_HOSTNAME" \
      --arg setid "$SET_ID" \
      --argjson weight "$R_WEIGHT" \
      --argjson ttl "$RECORD_TTL" \
      --arg value "$RCH" \
      --arg hcid "$HC_ID" \
      '. + [{
        Action: "CREATE",
        ResourceRecordSet: {
          Name: $name,
          Type: "CNAME",
          SetIdentifier: $setid,
          Weight: $weight,
          TTL: $ttl,
          ResourceRecords: [{Value: $value}],
          HealthCheckId: $hcid
        }
      }]')
  done

  build_batch "CREATE" "Grid edge entry regional records" "$REGIONAL_RECORDS" \
    > "$OUTPUT_ABS/create/regional-records.json"
  build_batch "DELETE" "Delete Grid edge entry regional records" "$REGIONAL_RECORDS" \
    > "$OUTPUT_ABS/delete/regional-records.json"
  echo "  create: $OUTPUT_ABS/create/regional-records.json"
  echo "  delete: $OUTPUT_ABS/delete/regional-records.json"
fi

# ── Final validation ──────────────────────────────────────────────────

echo ""
echo "=== Validating generated payloads ==="

VALIDATION_ERRORS=0
for PAYLOAD in "$OUTPUT_ABS"/create/*.json "$OUTPUT_ABS"/delete/*.json; do
  if ! jq . "$PAYLOAD" > /dev/null 2>&1; then
    echo "  FAIL  $PAYLOAD: invalid JSON" >&2
    VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
    continue
  fi

  if grep -qiE 'PLACEHOLDER' "$PAYLOAD"; then
    echo "  FAIL  $PAYLOAD: contains PLACEHOLDER value" >&2
    VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
    continue
  fi

  if grep -qE '<[a-zA-Z]' "$PAYLOAD"; then
    echo "  FAIL  $PAYLOAD: contains angle-bracket substitution" >&2
    VALIDATION_ERRORS=$((VALIDATION_ERRORS + 1))
    continue
  fi

  echo "  PASS  $(basename "$(dirname "$PAYLOAD")")/$(basename "$PAYLOAD")"
done

if [[ "$VALIDATION_ERRORS" -gt 0 ]]; then
  echo "" >&2
  echo "Payload validation failed with $VALIDATION_ERRORS error(s)." >&2
  exit 1
fi

echo ""
echo "All payloads materialized and validated."
echo ""
echo "Submit with:"
echo "  aws route53 change-resource-record-sets \\"
echo "    --hosted-zone-id $HOSTED_ZONE_ID \\"
echo "    --change-batch file://<create-file>"
echo ""
echo "Clean up with the matching delete/ payloads in reverse order."
