#!/usr/bin/env bash
# Tests for materialize-records.sh and render-health-checks.sh.
#
# Creates temporary fixtures, runs the scripts, and verifies outputs.
# Exits non-zero on first failure.
#
# Usage:
#   ./test-materialize.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MATERIALIZE="$SCRIPT_DIR/materialize-records.sh"
RENDER_HC="$SCRIPT_DIR/render-health-checks.sh"
WORK_DIR=""
TESTS_RUN=0
TESTS_PASSED=0

cleanup() {
  if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

WORK_DIR=$(mktemp -d)

pass() { TESTS_PASSED=$((TESTS_PASSED + 1)); echo "  PASS  $1"; }
fail() { echo "  FAIL  $1" >&2; exit 1; }

run_test() {
  TESTS_RUN=$((TESTS_RUN + 1))
  echo ""
  echo "--- Test: $1 ---"
}

# ── Fixtures ──────────────────────────────────────────────────────────

FIXTURE_DIR="$WORK_DIR/fixtures"
mkdir -p "$FIXTURE_DIR"

cat > "$FIXTURE_DIR/inventory.yaml" << 'YAML'
hostedZoneId: Z0EXAMPLE000000
hostedZoneDomain: grid.example.com
globalHostname: inference.grid.example.com
regionalHostname: inference-east.grid.example.com
regionalSites:
  - east1
  - east2
namespace: grid-system
serviceName: consumer-gateway
servicePort: "8080"
tlsTermination: edge
recordTTL: 60
setIdentifierPrefix: grid-edge
sites:
  east1:
    context: east1-context
    originHostname: east1.origin.grid.example.com
    awsRegion: us-east-1
  east2:
    context: east2-context
    originHostname: east2.origin.grid.example.com
    awsRegion: us-east-2
  west1:
    context: west1-context
    originHostname: west1.origin.grid.example.com
    awsRegion: us-west-1
  west2:
    context: west2-context
    originHostname: west2.origin.grid.example.com
    awsRegion: us-west-2
YAML

cat > "$FIXTURE_DIR/mappings.yaml" << 'YAML'
sites:
  east1:
    routerCanonicalHostname: "router-default.apps.east1.example.com"
    healthCheckId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  east2:
    routerCanonicalHostname: "router-default.apps.east2.example.com"
    healthCheckId: "b2c3d4e5-f6a7-8901-bcde-f12345678901"
  west1:
    routerCanonicalHostname: "router-default.apps.west1.example.com"
    healthCheckId: "c3d4e5f6-a7b8-9012-cdef-123456789012"
  west2:
    routerCanonicalHostname: "router-default.apps.west2.example.com"
    healthCheckId: "d4e5f6a7-b8c9-0123-defa-234567890123"
YAML

# ══════════════════════════════════════════════════════════════════════
# Test 1: Successful materialization
# ══════════════════════════════════════════════════════════════════════

run_test "successful materialization"

OUT="$WORK_DIR/out-success"
"$MATERIALIZE" "$FIXTURE_DIR/inventory.yaml" "$FIXTURE_DIR/mappings.yaml" \
  --output-dir "$OUT" > /dev/null 2>&1

for FILE in create/origin-records.json create/weighted-records.json \
            create/latency-records.json create/regional-records.json \
            delete/origin-records.json delete/weighted-records.json \
            delete/latency-records.json delete/regional-records.json; do
  if [[ ! -f "$OUT/$FILE" ]]; then
    fail "missing output: $FILE"
  fi
  if ! jq . "$OUT/$FILE" > /dev/null 2>&1; then
    fail "invalid JSON: $FILE"
  fi
done
pass "all expected files generated and valid JSON"

# Verify no placeholders in any output
if grep -rqiE 'PLACEHOLDER' "$OUT/"; then
  fail "output contains PLACEHOLDER value"
fi
pass "no PLACEHOLDER values in output"

# Verify no angle-bracket substitutions
if grep -rqE '<[a-zA-Z]' "$OUT/"; then
  fail "output contains angle-bracket substitution"
fi
pass "no angle-bracket substitutions in output"

# Verify correct values appear
if ! grep -q 'router-default.apps.east1.example.com' "$OUT/create/weighted-records.json"; then
  fail "east1 routerCanonicalHostname not in weighted records"
fi
if ! grep -q 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' "$OUT/create/weighted-records.json"; then
  fail "east1 healthCheckId not in weighted records"
fi
pass "runtime values correctly substituted"

# ══════════════════════════════════════════════════════════════════════
# Test 2: CREATE/DELETE consistency
# ══════════════════════════════════════════════════════════════════════

run_test "CREATE/DELETE consistency"

for RECORD_TYPE in origin-records weighted-records latency-records regional-records; do
  CREATE_FILE="$OUT/create/${RECORD_TYPE}.json"
  DELETE_FILE="$OUT/delete/${RECORD_TYPE}.json"

  CREATE_SETS=$(jq -S '[.Changes[].ResourceRecordSet]' "$CREATE_FILE")
  DELETE_SETS=$(jq -S '[.Changes[].ResourceRecordSet]' "$DELETE_FILE")

  if [[ "$CREATE_SETS" != "$DELETE_SETS" ]]; then
    fail "$RECORD_TYPE: CREATE and DELETE ResourceRecordSets differ"
  fi

  CREATE_ACTIONS=$(jq -r '[.Changes[].Action] | unique | .[]' "$CREATE_FILE")
  DELETE_ACTIONS=$(jq -r '[.Changes[].Action] | unique | .[]' "$DELETE_FILE")

  if [[ "$CREATE_ACTIONS" != "CREATE" ]]; then
    fail "$RECORD_TYPE: CREATE file has wrong Action: $CREATE_ACTIONS"
  fi
  if [[ "$DELETE_ACTIONS" != "DELETE" ]]; then
    fail "$RECORD_TYPE: DELETE file has wrong Action: $DELETE_ACTIONS"
  fi
done
pass "all CREATE/DELETE pairs have identical ResourceRecordSets"
pass "all Actions are correct (CREATE vs DELETE)"

# ══════════════════════════════════════════════════════════════════════
# Test 3: Missing mapping
# ══════════════════════════════════════════════════════════════════════

run_test "missing mapping"

cat > "$FIXTURE_DIR/mappings-missing.yaml" << 'YAML'
sites:
  east1:
    routerCanonicalHostname: "router-default.apps.east1.example.com"
    healthCheckId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
YAML

OUT_MISSING="$WORK_DIR/out-missing"
if "$MATERIALIZE" "$FIXTURE_DIR/inventory.yaml" "$FIXTURE_DIR/mappings-missing.yaml" \
    --output-dir "$OUT_MISSING" > /dev/null 2>&1; then
  fail "should have rejected incomplete mappings"
fi
pass "rejected mappings with missing sites"

# ══════════════════════════════════════════════════════════════════════
# Test 4: Placeholder rejection
# ══════════════════════════════════════════════════════════════════════

run_test "placeholder rejection"

cat > "$FIXTURE_DIR/mappings-placeholder.yaml" << 'YAML'
sites:
  east1:
    routerCanonicalHostname: "PLACEHOLDER-ROUTER-HOSTNAME-east1"
    healthCheckId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  east2:
    routerCanonicalHostname: "router-default.apps.east2.example.com"
    healthCheckId: "PLACEHOLDER-HEALTH-CHECK-ID-EAST2"
  west1:
    routerCanonicalHostname: "router-default.apps.west1.example.com"
    healthCheckId: "c3d4e5f6-a7b8-9012-cdef-123456789012"
  west2:
    routerCanonicalHostname: "router-default.apps.west2.example.com"
    healthCheckId: "d4e5f6a7-b8c9-0123-defa-234567890123"
YAML

OUT_PH="$WORK_DIR/out-placeholder"
if "$MATERIALIZE" "$FIXTURE_DIR/inventory.yaml" "$FIXTURE_DIR/mappings-placeholder.yaml" \
    --output-dir "$OUT_PH" > /dev/null 2>&1; then
  fail "should have rejected PLACEHOLDER values"
fi
pass "rejected mappings containing PLACEHOLDER"

# ══════════════════════════════════════════════════════════════════════
# Test 5: Angle-bracket rejection
# ══════════════════════════════════════════════════════════════════════

run_test "angle-bracket rejection"

cat > "$FIXTURE_DIR/mappings-angle.yaml" << 'YAML'
sites:
  east1:
    routerCanonicalHostname: "<routerCanonicalHostname-east1>"
    healthCheckId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  east2:
    routerCanonicalHostname: "router-default.apps.east2.example.com"
    healthCheckId: "<health-check-id>"
  west1:
    routerCanonicalHostname: "router-default.apps.west1.example.com"
    healthCheckId: "c3d4e5f6-a7b8-9012-cdef-123456789012"
  west2:
    routerCanonicalHostname: "router-default.apps.west2.example.com"
    healthCheckId: "d4e5f6a7-b8c9-0123-defa-234567890123"
YAML

OUT_ANGLE="$WORK_DIR/out-angle"
if "$MATERIALIZE" "$FIXTURE_DIR/inventory.yaml" "$FIXTURE_DIR/mappings-angle.yaml" \
    --output-dir "$OUT_ANGLE" > /dev/null 2>&1; then
  fail "should have rejected angle-bracket values"
fi
pass "rejected mappings containing angle-bracket substitutions"

# ══════════════════════════════════════════════════════════════════════
# Test 6: Malformed mapping (empty values)
# ══════════════════════════════════════════════════════════════════════

run_test "malformed mapping (empty values)"

cat > "$FIXTURE_DIR/mappings-empty.yaml" << 'YAML'
sites:
  east1:
    routerCanonicalHostname: ""
    healthCheckId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  east2:
    routerCanonicalHostname: "router-default.apps.east2.example.com"
    healthCheckId: ""
  west1:
    routerCanonicalHostname: "router-default.apps.west1.example.com"
    healthCheckId: "c3d4e5f6-a7b8-9012-cdef-123456789012"
  west2:
    routerCanonicalHostname: "router-default.apps.west2.example.com"
    healthCheckId: "d4e5f6a7-b8c9-0123-defa-234567890123"
YAML

OUT_EMPTY="$WORK_DIR/out-empty"
if "$MATERIALIZE" "$FIXTURE_DIR/inventory.yaml" "$FIXTURE_DIR/mappings-empty.yaml" \
    --output-dir "$OUT_EMPTY" > /dev/null 2>&1; then
  fail "should have rejected empty values"
fi
pass "rejected mappings with empty values"

# ══════════════════════════════════════════════════════════════════════
# Test 7: Duplicate awsRegion rejection
# ══════════════════════════════════════════════════════════════════════

run_test "duplicate awsRegion rejection"

cat > "$FIXTURE_DIR/inventory-dup-region.yaml" << 'YAML'
hostedZoneId: Z0EXAMPLE000000
hostedZoneDomain: grid.example.com
globalHostname: inference.grid.example.com
recordTTL: 60
setIdentifierPrefix: grid-edge
sites:
  east1:
    context: east1-context
    originHostname: east1.origin.grid.example.com
    awsRegion: us-east-1
  east2:
    context: east2-context
    originHostname: east2.origin.grid.example.com
    awsRegion: us-east-1
YAML

cat > "$FIXTURE_DIR/mappings-two.yaml" << 'YAML'
sites:
  east1:
    routerCanonicalHostname: "router-default.apps.east1.example.com"
    healthCheckId: "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
  east2:
    routerCanonicalHostname: "router-default.apps.east2.example.com"
    healthCheckId: "b2c3d4e5-f6a7-8901-bcde-f12345678901"
YAML

OUT_DUP="$WORK_DIR/out-dup-region"
if "$MATERIALIZE" "$FIXTURE_DIR/inventory-dup-region.yaml" "$FIXTURE_DIR/mappings-two.yaml" \
    --output-dir "$OUT_DUP" > /dev/null 2>&1; then
  fail "should have rejected duplicate awsRegion"
fi
pass "rejected inventory with duplicate awsRegion"

# ══════════════════════════════════════════════════════════════════════
# Test 8: Health-check rendering
# ══════════════════════════════════════════════════════════════════════

run_test "health-check rendering (Phase A)"

OUT_HC="$WORK_DIR/out-health-checks"
"$RENDER_HC" "$FIXTURE_DIR/inventory.yaml" "$FIXTURE_DIR/mappings.yaml" \
  --output-dir "$OUT_HC" > /dev/null 2>&1

for SITE in east1 east2 west1 west2; do
  HC_FILE="$OUT_HC/health-check-${SITE}.json"
  if [[ ! -f "$HC_FILE" ]]; then
    fail "missing health-check config: $SITE"
  fi
  if ! jq . "$HC_FILE" > /dev/null 2>&1; then
    fail "invalid JSON: $HC_FILE"
  fi

  HC_TYPE=$(jq -r '.Type' "$HC_FILE")
  HC_PORT=$(jq -r '.Port' "$HC_FILE")
  if [[ "$HC_TYPE" != "TCP" || "$HC_PORT" != "443" ]]; then
    fail "$SITE: expected TCP/443, got $HC_TYPE/$HC_PORT"
  fi

  if jq -e '.EnableSNI' "$HC_FILE" > /dev/null 2>&1; then
    fail "$SITE: EnableSNI present (invalid for TCP type)"
  fi
done
pass "all health-check configs valid (TCP/443, no EnableSNI)"

# Verify health checks use ingress hostnames that resolve before origin records exist
EAST1_FQDN=$(jq -r '.FullyQualifiedDomainName' "$OUT_HC/health-check-east1.json")
if [[ "$EAST1_FQDN" != "router-default.apps.east1.example.com" ]]; then
  fail "east1 FQDN mismatch: $EAST1_FQDN"
fi
pass "health-check FQDNs match captured ingress hostnames"

# ══════════════════════════════════════════════════════════════════════
# Test 9: Weighted record count and structure
# ══════════════════════════════════════════════════════════════════════

run_test "weighted record structure"

WEIGHTED_COUNT=$(jq '.Changes | length' "$OUT/create/weighted-records.json")
if [[ "$WEIGHTED_COUNT" -ne 4 ]]; then
  fail "expected 4 weighted records, got $WEIGHTED_COUNT"
fi
pass "4 weighted records generated"

WEIGHT_SUM=$(jq '[.Changes[].ResourceRecordSet.Weight] | add' "$OUT/create/weighted-records.json")
if [[ "$WEIGHT_SUM" -ne 100 ]]; then
  fail "weight sum is $WEIGHT_SUM, expected 100"
fi
pass "weight sum is 100"

# ══════════════════════════════════════════════════════════════════════
# Test 10: Latency record regions
# ══════════════════════════════════════════════════════════════════════

run_test "latency record regions"

REGIONS=$(jq -r '[.Changes[].ResourceRecordSet.Region] | sort | .[]' "$OUT/create/latency-records.json")
EXPECTED_REGIONS=$(printf 'us-east-1\nus-east-2\nus-west-1\nus-west-2')
if [[ "$REGIONS" != "$EXPECTED_REGIONS" ]]; then
  fail "latency regions mismatch"
fi
pass "latency records have correct and unique AWS regions"

# ══════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  $TESTS_PASSED/$TESTS_RUN tests passed"
echo "═══════════════════════════════════════════════════════════════"
