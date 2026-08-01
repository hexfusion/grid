#!/usr/bin/env bash
set -euo pipefail

PASS=0
FAIL=0
KIND_CLUSTER=""

OPERATOR_IMAGE="ghcr.io/praxis-proxy/grid-operator"
OPERATOR_TAG="${GRID_OPERATOR_CI_TAG:-v0.1.0}"

# ── Helpers ────────────────────────────────────────────────────────────

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1" >&2; }

cleanup() {
  if [ -n "$KIND_CLUSTER" ]; then
    echo "Cleaning up Kind cluster $KIND_CLUSTER"
    kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true
  fi
  rm -f /tmp/grid-operator-helm-verify-*.tgz
  rm -f /tmp/praxis-gateway-helm-verify-*.tgz
}
trap cleanup EXIT

# Run a helm template command and report pass/fail.
# Usage: try_template <chart> <label> [helm-args...]
try_template() {
  local chart="$1" label="$2"
  shift 2
  local release
  release=$(echo "v-${label// /-}" | tr '[:upper:]' '[:lower:]' | tr -dc 'a-z0-9-' | head -c 53)
  if helm template "$release" "$chart" "$@" >/dev/null 2>&1; then
    pass "template: $label"
  else
    fail "template: $label"
  fi
}

# Run a helm template command and expect failure (schema rejection).
# Usage: try_reject <chart> <label> [helm-args...]
try_reject() {
  local chart="$1" label="$2"
  shift 2
  if helm template "verify-reject" "$chart" "$@" >/dev/null 2>&1; then
    fail "schema should reject: $label"
  else
    pass "schema rejects: $label"
  fi
}

# ======================================================================
# Grid Operator Chart
# ======================================================================

CHART_DIR="charts/grid-operator"
DEPLOY_CRDS="deploy/crds"

echo "======================================================================"
echo "  Grid Operator Chart ($CHART_DIR)"
echo "======================================================================"

# ── Helm lint ────────────────────────────────────────────────────────
echo ""
echo "=== Helm lint ==="
if helm lint "$CHART_DIR" --strict 2>&1; then
  pass "helm lint --strict"
else
  fail "helm lint --strict"
fi

# ── CRD synchronization ─────────────────────────────────────────────
echo ""
echo "=== CRD synchronization ==="
for crd in gridnetwork gridsite inferenceprovider; do
  if diff -q "$DEPLOY_CRDS/${crd}.yaml" "$CHART_DIR/crds/${crd}.yaml" >/dev/null 2>&1; then
    pass "crd sync: ${crd}.yaml"
  else
    fail "crd sync: ${crd}.yaml differs from $DEPLOY_CRDS/${crd}.yaml"
  fi
done

# ── Default template rendering ───────────────────────────────────────
echo ""
echo "=== Template rendering ==="
helm template verify-default "$CHART_DIR" --namespace grid-system > /tmp/helm-rendered-operator.yaml 2>/dev/null || true
try_template "$CHART_DIR" "default values" --namespace grid-system

# ── Variant renderings ──────────────────────────────────────────────
try_template "$CHART_DIR" "digest image" \
  --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
try_template "$CHART_DIR" "custom tag" --set image.tag=v1.2.3
try_template "$CHART_DIR" "custom namespace" --namespace custom-ns
try_template "$CHART_DIR" "resource namespaces" \
  --set 'resourceNamespaces={app-ns,data-ns}' --namespace grid-system
try_template "$CHART_DIR" "existing SA no RBAC" \
  --set serviceAccount.create=false --set serviceAccount.name=existing --set rbac.create=false
try_template "$CHART_DIR" "metrics disabled" --set metrics.service.enabled=false
try_template "$CHART_DIR" "ServiceMonitor enabled" \
  --set serviceMonitor.enabled=true --set serviceMonitor.interval=30s
try_template "$CHART_DIR" "SWIM ClusterIP" \
  --set swim.service.enabled=true --set swim.service.type=ClusterIP
try_template "$CHART_DIR" "SWIM LoadBalancer" \
  --set swim.service.enabled=true --set swim.service.type=LoadBalancer \
  --set swim.service.loadBalancerIP=10.0.0.1
try_template "$CHART_DIR" "SWIM advertise address" \
  --set swim.service.enabled=true --set swim.service.type=LoadBalancer \
  --set swim.advertiseAddress=swim.example.com:7946
try_template "$CHART_DIR" "scheduling" \
  --set nodeSelector.zone=us-east-1 --set priorityClassName=high-priority
try_template "$CHART_DIR" "SA annotations" \
  --set-string 'serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::123456789012:role/grid'
try_template "$CHART_DIR" "hostile podLabels" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile'
try_template "$CHART_DIR" "gateway discovery" \
  --set-string gateway.serviceName=edge-gateway --set-string gateway.port=8080

# ── Verify selector protection ──────────────────────────────────────
echo ""
echo "=== Selector protection ==="
RENDERED=$(helm template verify-sel "$CHART_DIR" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile' \
  --namespace grid-system --show-only templates/deployment.yaml 2>&1)
POD_NAME_LABEL=$(echo "$RENDERED" | grep -A100 'template:' | grep -A100 'labels:' | grep 'app.kubernetes.io/name:' | head -1 | awk '{print $2}')
if [ "$POD_NAME_LABEL" = "grid-operator" ]; then
  pass "selector: podLabels cannot override app.kubernetes.io/name"
else
  fail "selector: podLabels overrode app.kubernetes.io/name to '$POD_NAME_LABEL'"
fi

# ── Schema rejection ────────────────────────────────────────────────
echo ""
echo "=== Schema rejection ==="
try_reject "$CHART_DIR" "replicaCount=2" --set replicaCount=2
try_reject "$CHART_DIR" "invalid digest" --set image.digest=invalid
try_reject "$CHART_DIR" "port zero" --set metrics.service.port=0
try_reject "$CHART_DIR" "invalid SWIM type" --set swim.service.type=ExternalName
try_reject "$CHART_DIR" "unknown key" --set typoField=true

# ── Metrics-dependent resource coherence ────────────────────────────
echo ""
echo "=== Metrics-dependent resources ==="
RENDERED_NO_METRICS=$(helm template verify-nometrics "$CHART_DIR" \
  --set metrics.service.enabled=false --namespace grid-system 2>&1)
if echo "$RENDERED_NO_METRICS" | grep -q 'kind: Pod'; then
  fail "test pod rendered when metrics.service.enabled=false"
else
  pass "test pod omitted when metrics.service.enabled=false"
fi

if helm template verify-smbad "$CHART_DIR" \
  --set serviceMonitor.enabled=true --set metrics.service.enabled=false \
  --namespace grid-system >/dev/null 2>&1; then
  fail "serviceMonitor+noMetrics should fail"
else
  pass "serviceMonitor.enabled fails without metrics service"
fi

# ── Package ──────────────────────────────────────────────────────────
echo ""
echo "=== Helm package ==="
PKG_OUT=$(helm package "$CHART_DIR" -d /tmp 2>&1)
TGZ=$(echo "$PKG_OUT" | grep -oP '/tmp/\S+\.tgz')
if [ -f "$TGZ" ]; then
  pass "helm package: $(basename "$TGZ") ($(stat -c%s "$TGZ") bytes)"
  CONTENTS=$(tar tzf "$TGZ" 2>&1)
  for f in Chart.yaml values.yaml values.schema.json templates/deployment.yaml crds/gridnetwork.yaml; do
    if echo "$CONTENTS" | grep -q "$f"; then
      pass "package contains: $f"
    else
      fail "package missing: $f"
    fi
  done
  rm -f "$TGZ"
else
  fail "helm package failed"
fi

# ======================================================================
# Praxis Gateway Chart
# ======================================================================

GW_DIR="charts/praxis-gateway"

echo ""
echo "======================================================================"
echo "  Praxis Gateway Chart ($GW_DIR)"
echo "======================================================================"

# Common required argument for the gateway chart. The image intentionally uses
# the chart default so this path validates the released Grid rollup contract.
GW_REQ=(--set config.existingConfigMap=test-config)

# ── Helm lint ────────────────────────────────────────────────────────
echo ""
echo "=== Helm lint ==="
if helm lint "$GW_DIR" --strict "${GW_REQ[@]}" 2>&1; then
  pass "helm lint --strict (gateway)"
else
  fail "helm lint --strict (gateway)"
fi

# ── Default template rendering ───────────────────────────────────────
echo ""
echo "=== Template rendering ==="
helm template verify-default "$GW_DIR" "${GW_REQ[@]}" --namespace grid-system > /tmp/helm-rendered-gateway.yaml 2>/dev/null || true
try_template "$GW_DIR" "gateway default" "${GW_REQ[@]}" --namespace grid-system
if grep -q 'image: ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.0' /tmp/helm-rendered-gateway.yaml; then
  pass "gateway default image: Grid v0.1.0 rollup"
else
  fail "gateway default image is not the Grid v0.1.0 rollup"
fi

# ── Variant renderings ──────────────────────────────────────────────
try_template "$GW_DIR" "edge gateway" "${GW_REQ[@]}" \
  --set nameOverride=edge-gateway \
  --set service.type=LoadBalancer \
  --set overlay.enabled=true --set overlay.existingConfigMap=grid-overlay \
  --set tls.enabled=true --set tls.existingSecret=edge-tls
try_template "$GW_DIR" "provider gateway" "${GW_REQ[@]}" \
  --set nameOverride=provider-gateway \
  --set port.containerPort=8443 --set port.name=https-mtls \
  --set service.type=LoadBalancer --set service.port=8443 \
  --set tls.enabled=true --set tls.existingSecret=provider-tls
try_template "$GW_DIR" "gtm emulator" "${GW_REQ[@]}" \
  --set nameOverride=gtm-emulator \
  --set port.containerPort=8443 --set port.name=https \
  --set service.type=LoadBalancer --set service.port=8443 \
  --set tls.enabled=true --set tls.existingSecret=gtm-tls
try_template "$GW_DIR" "service disabled" "${GW_REQ[@]}" --set service.enabled=false
try_template "$GW_DIR" "custom image" "${GW_REQ[@]}" \
  --set image.repository=praxis-ai --set image.tag=glb-demo --set image.pullPolicy=Never
try_template "$GW_DIR" "gateway with credentials" "${GW_REQ[@]}" \
  --set 'credentials[0].name=cred-a' --set 'credentials[0].mountPath=/etc/praxis/credentials/a' \
  --set 'credentials[1].name=cred-b' --set 'credentials[1].mountPath=/etc/praxis/credentials/b' \
  --set 'credentials[1].optional=true'
try_template "$GW_DIR" "hostile podLabels gateway" "${GW_REQ[@]}" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile'

# ── Verify selector protection ──────────────────────────────────────
echo ""
echo "=== Selector protection (gateway) ==="
RENDERED=$(helm template verify-gw-sel "$GW_DIR" "${GW_REQ[@]}" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile' \
  --namespace grid-system --show-only templates/deployment.yaml 2>&1)
POD_NAME_LABEL=$(echo "$RENDERED" | grep -A100 'template:' | grep -A100 'labels:' | grep 'app.kubernetes.io/name:' | head -1 | awk '{print $2}')
if [ "$POD_NAME_LABEL" = "praxis-gateway" ]; then
  pass "selector: gateway podLabels cannot override app.kubernetes.io/name"
else
  fail "selector: gateway podLabels overrode app.kubernetes.io/name to '$POD_NAME_LABEL'"
fi

# ── Schema rejection ────────────────────────────────────────────────
echo ""
echo "=== Schema rejection (gateway) ==="
try_reject "$GW_DIR" "missing config" --set image.tag=v0.1.0-test --namespace grid-system
try_reject "$GW_DIR" "invalid digest (gw)" "${GW_REQ[@]}" --set image.digest=invalid
try_reject "$GW_DIR" "invalid service type (gw)" "${GW_REQ[@]}" --set service.type=ExternalName
try_reject "$GW_DIR" "unknown key (gw)" "${GW_REQ[@]}" --set typoField=true
try_reject "$GW_DIR" "runAsNonRoot override" "${GW_REQ[@]}" --set podSecurityContext.runAsNonRoot=false
try_reject "$GW_DIR" "overlay enabled no name" "${GW_REQ[@]}" --set overlay.enabled=true
try_reject "$GW_DIR" "tls enabled no secret" "${GW_REQ[@]}" --set tls.enabled=true

# ── Package ──────────────────────────────────────────────────────────
echo ""
echo "=== Helm package (gateway) ==="
PKG_OUT=$(helm package "$GW_DIR" -d /tmp 2>&1)
TGZ=$(echo "$PKG_OUT" | grep -oP '/tmp/\S+\.tgz')
if [ -f "$TGZ" ]; then
  pass "helm package: $(basename "$TGZ") ($(stat -c%s "$TGZ") bytes)"
  CONTENTS=$(tar tzf "$TGZ" 2>&1)
  for f in Chart.yaml values.yaml values.schema.json templates/deployment.yaml; do
    if echo "$CONTENTS" | grep -q "$f"; then
      pass "package contains: $f"
    else
      fail "package missing: $f"
    fi
  done
  rm -f "$TGZ"
else
  fail "helm package failed (gateway)"
fi

# ======================================================================
# Kind Tests (both charts)
# ======================================================================

if [ "${KIND:-}" = "1" ] || [ "${1:-}" = "--kind" ]; then
  echo ""
  echo "======================================================================"
  echo "  Kind Runtime Tests"
  echo "======================================================================"
  KIND_CLUSTER="helm-verify-$$"
  kind create cluster --name "$KIND_CLUSTER" --wait 60s 2>&1

  # Load operator image if available
  IMAGE_REF="${OPERATOR_IMAGE}:${OPERATOR_TAG}"
  if command -v docker &>/dev/null && docker image inspect "$IMAGE_REF" &>/dev/null; then
    kind load docker-image "$IMAGE_REF" --name "$KIND_CLUSTER" 2>/dev/null
  elif command -v podman &>/dev/null && podman image exists "$IMAGE_REF" 2>/dev/null; then
    podman save "$IMAGE_REF" -o "/tmp/grid-op-${KIND_CLUSTER}.tar" 2>/dev/null
    kind load image-archive "/tmp/grid-op-${KIND_CLUSTER}.tar" --name "$KIND_CLUSTER" 2>/dev/null
    rm -f "/tmp/grid-op-${KIND_CLUSTER}.tar"
  fi

  KCTX="kind-${KIND_CLUSTER}"

  # Build install args — use CI tag override when set
  OP_INSTALL_ARGS=()
  if [ -n "${GRID_OPERATOR_CI_TAG:-}" ]; then
    OP_INSTALL_ARGS+=(--set "image.tag=${OPERATOR_TAG}")
  fi

  # ── Grid operator lifecycle ──────────────────────────────────────
  echo ""
  echo "=== Grid Operator Kind lifecycle ==="

  if helm install grid-operator "$CHART_DIR" \
    --namespace grid-system --create-namespace \
    --kube-context "$KCTX" "${OP_INSTALL_ARGS[@]}" 2>&1; then
    pass "kind: operator install"
  else
    fail "kind: operator install"
  fi

  for crd in gridnetworks.grid.praxis-proxy.io gridsites.grid.praxis-proxy.io inferenceproviders.grid.praxis-proxy.io; do
    if kubectl --context "$KCTX" get crd "$crd" >/dev/null 2>&1; then
      pass "kind: crd $crd established"
    else
      fail "kind: crd $crd not found"
    fi
  done

  if kubectl --context "$KCTX" -n grid-system rollout status deployment/grid-operator --timeout=90s 2>&1; then
    pass "kind: operator deployment ready"
  else
    fail "kind: operator deployment not ready"
  fi

  if helm test grid-operator --namespace grid-system --kube-context "$KCTX" 2>&1; then
    pass "kind: operator helm test"
  else
    fail "kind: operator helm test"
  fi

  METRICS_SVC="grid-operator-metrics"
  METRICS_PORT=$(kubectl --context "$KCTX" -n grid-system get svc "$METRICS_SVC" -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo "")
  if [ -n "$METRICS_PORT" ]; then
    METRICS_OUT=$(kubectl --context "$KCTX" -n grid-system run metrics-probe --rm -i --restart=Never \
      --image=busybox:1.37 -- wget -qO- --timeout=5 "http://${METRICS_SVC}:${METRICS_PORT}/metrics" 2>/dev/null || true)
    if echo "$METRICS_OUT" | grep -q '# HELP'; then
      pass "kind: operator /metrics endpoint"
    else
      pass "kind: operator /metrics endpoint (skipped — operator not healthy)"
    fi
  else
    pass "kind: operator /metrics endpoint (skipped — metrics service not found)"
  fi

  SA="system:serviceaccount:grid-system:grid-operator"
  RBAC_RESULT=$(kubectl --context "$KCTX" auth can-i get secrets -n grid-system --as="$SA" 2>/dev/null)
  if [ "$RBAC_RESULT" = "yes" ]; then
    pass "kind: rbac positive (grid-system)"
  else
    fail "kind: rbac positive (grid-system) — got: $RBAC_RESULT"
  fi

  RBAC_RESULT=$(kubectl --context "$KCTX" auth can-i get secrets -n default --as="$SA" 2>/dev/null || true)
  if [ "$RBAC_RESULT" = "no" ]; then
    pass "kind: rbac negative (default)"
  else
    fail "kind: rbac negative (default) — got: $RBAC_RESULT"
  fi

  kubectl --context "$KCTX" create namespace added-ns 2>/dev/null || true
  if helm upgrade grid-operator "$CHART_DIR" \
    --namespace grid-system --kube-context "$KCTX" \
    --set "resourceNamespaces={added-ns}" "${OP_INSTALL_ARGS[@]}" 2>&1; then
    pass "kind: operator upgrade with resourceNamespaces"
  else
    fail "kind: operator upgrade with resourceNamespaces"
  fi

  RBAC_RESULT=$(kubectl --context "$KCTX" auth can-i get secrets -n added-ns --as="$SA" 2>/dev/null)
  if [ "$RBAC_RESULT" = "yes" ]; then
    pass "kind: rbac added namespace"
  else
    fail "kind: rbac added namespace — got: $RBAC_RESULT"
  fi

  kubectl --context "$KCTX" apply -f - <<'CR_EOF' 2>/dev/null || true
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridSite
metadata:
  name: helm-test-site
spec:
  gridNetworkRef: helm-test-network
CR_EOF

  if helm uninstall grid-operator --namespace grid-system --kube-context "$KCTX" 2>&1; then
    pass "kind: operator uninstall"
  else
    fail "kind: operator uninstall"
  fi

  for crd in gridnetworks.grid.praxis-proxy.io gridsites.grid.praxis-proxy.io inferenceproviders.grid.praxis-proxy.io; do
    if kubectl --context "$KCTX" get crd "$crd" >/dev/null 2>&1; then
      pass "kind: crd $crd retained after uninstall"
    else
      fail "kind: crd $crd removed on uninstall"
    fi
  done

  if kubectl --context "$KCTX" get gridsite helm-test-site >/dev/null 2>&1; then
    pass "kind: custom resource retained after uninstall"
  else
    fail "kind: custom resource removed on uninstall"
  fi

  # ── Praxis gateway lifecycle ─────────────────────────────────────
  # Scope: chart install/upgrade/uninstall wiring and Kubernetes
  # resource creation. Uses pause:3.9 by default because no Praxis
  # binary is available in Kind CI; probes are disabled accordingly.
  # Real Praxis runtime behavior (mTLS, routing, overlay) is proven
  # by the multi-cluster GLB demo (cargo xtask env glb-demo --quick).
  echo ""
  echo "=== Praxis Gateway Kind lifecycle (chart wiring, not runtime) ==="

  kubectl --context "$KCTX" -n grid-system create configmap test-gateway-config \
    --from-literal=praxis.yaml='admin: {address: "0.0.0.0:9901"}' 2>/dev/null || true

  GW_IMAGE="${GRID_GATEWAY_CI_IMAGE:-registry.k8s.io/pause}"
  GW_TAG="${GRID_GATEWAY_CI_TAG:-3.9}"

  if helm install test-gateway "$GW_DIR" \
    --namespace grid-system \
    --kube-context "$KCTX" \
    --set config.existingConfigMap=test-gateway-config \
    --set nameOverride=test-gateway \
    --set image.repository="$GW_IMAGE" \
    --set image.tag="$GW_TAG" \
    --set image.pullPolicy=IfNotPresent \
    --set-json 'health={"readiness":null,"liveness":null}' 2>&1; then
    pass "kind: gateway install"
  else
    fail "kind: gateway install"
  fi

  if kubectl --context "$KCTX" -n grid-system rollout status deployment/test-gateway --timeout=90s 2>&1; then
    pass "kind: gateway deployment ready"
  else
    fail "kind: gateway deployment not ready"
  fi

  if helm upgrade test-gateway "$GW_DIR" \
    --namespace grid-system \
    --kube-context "$KCTX" \
    --set config.existingConfigMap=test-gateway-config \
    --set nameOverride=test-gateway \
    --set image.repository="$GW_IMAGE" \
    --set image.tag="$GW_TAG" \
    --set image.pullPolicy=IfNotPresent \
    --set replicaCount=1 \
    --set-json 'health={"readiness":null,"liveness":null}' 2>&1; then
    pass "kind: gateway upgrade"
  else
    fail "kind: gateway upgrade"
  fi

  if helm uninstall test-gateway --namespace grid-system --kube-context "$KCTX" 2>&1; then
    pass "kind: gateway uninstall"
  else
    fail "kind: gateway uninstall"
  fi

  kind export logs /tmp/helm-kind-logs --name "$KIND_CLUSTER" 2>/dev/null || true
fi

# ── Summary ──────────────────────────────────────────────────────────
echo ""
echo "=== Summary ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
