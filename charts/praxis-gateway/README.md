# Praxis Gateway Helm Chart (Temporary)

Temporary workload chart that deploys the Praxis AI gateway process directly
as a Kubernetes Deployment. This chart exists in the Grid repository because
no Praxis/Gateway Operator or supported Kubernetes installation path exists
yet.

**This chart is not an operator.** It does not define CRDs, controllers, or
dynamic discovery. It mounts a supplied Praxis configuration and optional
TLS, overlay, and credential Secrets.

**Ownership:** Temporary Grid integration asset. Long-term ownership moves to
the future Praxis/Gateway Operator repository when that deployment API and
release ownership exist. Do not treat this as a permanent Grid responsibility.

## Prerequisites

- Kubernetes >= 1.26
- Helm >= 3.12
- A Praxis configuration ConfigMap already created in the target namespace
- A compatible Praxis AI image (the current rollup contains open routing PRs)

## Install

From a local checkout:

```bash
kubectl create configmap edge-gateway-config \
  --from-file=praxis.yaml=path/to/praxis.yaml \
  -n grid-system

helm install edge-gateway charts/praxis-gateway \
  --namespace grid-system \
  --set config.existingConfigMap=edge-gateway-config
```

The default image is the Grid v0.1.0 Praxis AI rollup. Override
`image.repository`, `image.tag`, or `image.digest` to install another compatible
Praxis image. Prefer a digest when reproducing a validated deployment.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `replicaCount` | int | `1` | Gateway replicas. |
| `image.repository` | string | `ghcr.io/praxis-proxy/grid-ai-rollup` | Image repository. |
| `image.tag` | string | `""` | Image tag. Defaults to chart `appVersion` (`v0.1.0`). |
| `image.digest` | string | `""` | Immutable digest (sha256:…). When set, tag is ignored. |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | list | `[]` | Pull secrets for private registries. |
| `nameOverride` | string | `""` | Override chart name. |
| `fullnameOverride` | string | `""` | Override fully qualified app name. |
| `commonLabels` | object | `{}` | Labels added to all resources. |
| `podLabels` | object | `{}` | Additional pod labels. Selector labels cannot be overridden. |
| `podAnnotations` | object | `{}` | Pod annotations. |
| `podSecurityContext` | object | `{}` | Extra pod securityContext (`runAsUser`, `runAsGroup`, `fsGroup`). |
| `args` | list | `["--config", "/etc/praxis/praxis.yaml"]` | Container arguments. |
| `config.existingConfigMap` | string | **required** | Name of an existing ConfigMap with the Praxis config. |
| `config.key` | string | `praxis.yaml` | Key in the ConfigMap. |
| `port.containerPort` | int | `8080` | Container port. |
| `port.name` | string | `http` | Port name. |
| `port.protocol` | string | `TCP` | Port protocol. |
| `service.enabled` | bool | `true` | Create a Service. |
| `service.type` | string | `ClusterIP` | Service type. |
| `service.port` | int | `8080` | Service port. |
| `service.annotations` | object | `{}` | Service annotations. |
| `service.loadBalancerIP` | string | `""` | Static IP for LoadBalancer. |
| `overlay.enabled` | bool | `false` | Mount an overlay ConfigMap. |
| `overlay.existingConfigMap` | string | `""` | Name of the overlay ConfigMap. |
| `overlay.mountPath` | string | `/etc/praxis/routing` | Mount path for overlay files. |
| `overlay.items` | list | routing-config.json, routing-overlay.json | Items to project. |
| `tls.enabled` | bool | `false` | Mount a TLS Secret. |
| `tls.existingSecret` | string | `""` | Name of the TLS Secret. |
| `tls.mountPath` | string | `/etc/praxis/tls` | Mount path for TLS files. |
| `credentials` | list | `[]` | Credential Secret mounts (name, mountPath, optional). |
| `health.readiness` | object | TCP socket on port `http` | Readiness probe. Set to null to disable. |
| `health.liveness` | object | TCP socket on port `http` | Liveness probe. Set to null to disable. |
| `resources` | object | `{}` | Container resource requests and limits. |
| `nodeSelector` | object | `{}` | Node selector. |
| `affinity` | object | `{}` | Pod affinity rules. |
| `tolerations` | list | `[]` | Pod tolerations. |
| `topologySpreadConstraints` | list | `[]` | Topology spread constraints. |
| `priorityClassName` | string | `""` | Pod priority class. |

## Security

The chart enforces OpenShift-compatible restricted security defaults:

- `runAsNonRoot: true` (no fixed UID)
- `readOnlyRootFilesystem: true`
- `allowPrivilegeEscalation: false`
- All Linux capabilities dropped
- `seccompProfile.type: RuntimeDefault`
- `automountServiceAccountToken: false`

## Edge vs Provider Gateway

The chart is role-neutral. Edge and provider gateways use the same chart
with different values:

**Edge gateway:**
- Listens on port 8080 (HTTP)
- Mounts an overlay ConfigMap from the Grid operator
- Mounts a TLS Secret for upstream connections

**Provider gateway:**
- Listens on port 8443 (mTLS)
- Mounts a TLS Secret for client authentication
- Mounts credential Secrets for backend provider access
- Requires the `grid.praxis-proxy.io/backend-access` pod label for
  NetworkPolicy
