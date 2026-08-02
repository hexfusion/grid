#!/usr/bin/env bash
# Verify a Grid installation on existing clusters.
# Checks pod health, SWIM membership, overlay convergence, and routing.
#
# Usage: ./verify.sh <inventory.yaml>
#
# Requires: kubectl, yq

set -euo pipefail

INVENTORY="${1:?Usage: $0 <inventory.yaml>}"

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

TOPOLOGY=$(yq '.topology' "$INVENTORY")
SITE_NAMES=$(yq '.sites | keys | .[]' "$INVENTORY")
SITE_COUNT=$(echo "$SITE_NAMES" | wc -w)
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

echo "Verifying Grid installation"
echo "  Topology: $TOPOLOGY"
echo "  Sites:    $SITE_COUNT"
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq ".sites.${SITE}.context" "$INVENTORY")
  echo "--- Site: $SITE ---"

  check "namespace exists" kubectl --context "$CONTEXT" get namespace grid-system

  check "operator pod running" kubectl --context "$CONTEXT" -n grid-system \
    get pods -l app.kubernetes.io/name=grid-operator -o jsonpath='{.items[0].status.phase}' \
    | grep -q Running

  if [[ "$TOPOLOGY" == "dedicated-edge" ]]; then
    ROLE=$(yq ".sites.${SITE}.roles[0]" "$INVENTORY" 2>/dev/null || echo "unknown")
    if [[ "$ROLE" == "consumer" ]]; then
      check "consumer gateway running" kubectl --context "$CONTEXT" -n grid-system \
        get pods -l app.kubernetes.io/instance=consumer-gateway -o jsonpath='{.items[0].status.phase}' \
        | grep -q Running
    elif [[ "$ROLE" == "provider" ]]; then
      check "provider gateway running" kubectl --context "$CONTEXT" -n grid-system \
        get pods -l app.kubernetes.io/instance=provider-gateway -o jsonpath='{.items[0].status.phase}' \
        | grep -q Running
    fi
  elif [[ "$TOPOLOGY" == "combined-site" ]]; then
    check "consumer gateway running" kubectl --context "$CONTEXT" -n grid-system \
      get pods -l app.kubernetes.io/instance=consumer-gateway -o jsonpath='{.items[0].status.phase}' \
      | grep -q Running

    check "provider gateway running" kubectl --context "$CONTEXT" -n grid-system \
      get pods -l app.kubernetes.io/instance=provider-gateway -o jsonpath='{.items[0].status.phase}' \
      | grep -q Running
  fi

  check "SWIM service exists" kubectl --context "$CONTEXT" -n grid-system \
    get service grid-operator-swim

  SWIM_LB=$(kubectl --context "$CONTEXT" -n grid-system \
    get service grid-operator-swim -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  if [[ -n "$SWIM_LB" ]]; then
    echo "  PASS  SWIM LoadBalancer IP assigned: $SWIM_LB"
  else
    echo "  WARN  SWIM LoadBalancer IP not yet assigned (may be pending)" >&2
  fi

  echo ""
done

echo "--- Overlay convergence ---"
for SITE in $SITE_NAMES; do
  CONTEXT=$(yq ".sites.${SITE}.context" "$INVENTORY")

  OVERLAY_EXISTS=$(kubectl --context "$CONTEXT" -n grid-system \
    get configmap grid-routing-overlay -o jsonpath='{.data}' 2>/dev/null || echo "")
  if [[ -n "$OVERLAY_EXISTS" ]]; then
    echo "  PASS  $SITE: routing overlay present"
  else
    echo "  FAIL  $SITE: routing overlay not found" >&2
    ERRORS=$((ERRORS + 1))
  fi
done
echo ""

echo "--- Request test ---"
CONSUMER_SITE=""
for SITE in $SITE_NAMES; do
  if [[ "$TOPOLOGY" == "combined-site" ]]; then
    CONSUMER_SITE="$SITE"
    break
  fi
  ROLE=$(yq ".sites.${SITE}.roles[0]" "$INVENTORY" 2>/dev/null || echo "")
  if [[ "$ROLE" == "consumer" ]]; then
    CONSUMER_SITE="$SITE"
    break
  fi
done

if [[ -n "$CONSUMER_SITE" ]]; then
  CONTEXT=$(yq ".sites.${CONSUMER_SITE}.context" "$INVENTORY")
  echo "  Sending test request from $CONSUMER_SITE..."

  JOB_NAME="grid-verify-$(date +%s)"
  kubectl --context "$CONTEXT" -n grid-system create -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME
  namespace: grid-system
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 60
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: curl
        image: curlimages/curl:8.12.1
        command:
        - curl
        - -sf
        - -X
        - POST
        - -H
        - "Content-Type: application/json"
        - -d
        - '{"model":"test","messages":[{"role":"user","content":"verify"}]}'
        - http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions
        securityContext:
          runAsNonRoot: true
          readOnlyRootFilesystem: true
          allowPrivilegeEscalation: false
          capabilities:
            drop: [ALL]
EOF

  if kubectl --context "$CONTEXT" -n grid-system wait --for=condition=complete \
    "job/$JOB_NAME" --timeout=30s &>/dev/null; then
    echo "  PASS  test request succeeded"
    RESPONSE=$(kubectl --context "$CONTEXT" -n grid-system logs "job/$JOB_NAME" 2>/dev/null || echo "")
    if echo "$RESPONSE" | grep -q "choices"; then
      echo "  PASS  response contains expected inference fields"
    else
      echo "  FAIL  response missing 'choices' field — routing succeeded but did not reach inference" >&2
      ERRORS=$((ERRORS + 1))
    fi
  else
    echo "  FAIL  test request did not complete within 30s" >&2
    ERRORS=$((ERRORS + 1))
  fi

  kubectl --context "$CONTEXT" -n grid-system delete "job/$JOB_NAME" --ignore-not-found &>/dev/null
else
  echo "  SKIP  no consumer site found for request test" >&2
fi
echo ""

if [[ "$ERRORS" -gt 0 ]]; then
  echo "Verification FAILED with $ERRORS error(s)." >&2
  exit 1
fi

echo "Verification PASSED. All $SITE_COUNT sites healthy."
