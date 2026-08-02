# Existing-Cluster Helm Installation

This Helm-based installation workflow installs the Grid Operator and Praxis
gateways on four existing Kubernetes clusters. Two topology layouts use the
same charts and installer scripts. The workflow has been validated through
chart rendering and Kind; validation on existing clusters remains a separate
deployment gate.

## Topologies

### Dedicated Logical Edge Gateways

Consumer and provider responsibilities on separate clusters. Consumer
clusters run only Praxis consumer gateways; provider clusters run
Praxis provider gateways alongside inference backends.

See `dedicated-edge/` for site-specific Helm values.

### Combined Consumer and Provider Sites

Every cluster runs both consumer and provider gateway roles. Separate Praxis
Deployments, Services, configuration, and credentials preserve the role
separation. The complete provider boundary also depends on the configured
mTLS trust, RBAC, and NetworkPolicy when both roles share a cluster.

See `combined-site/` for site-specific Helm values.

## Inventory

Copy `inventory.example.yaml` to a local, gitignored `inventory.yaml`
and fill in your cluster contexts, reachable gateway addresses, and
SWIM service addresses:

```bash
cp inventory.example.yaml inventory.yaml
# Edit inventory.yaml with your values
```

The inventory is never committed. All scripts read it at runtime.

## Prerequisites

Before running the installer, prepare the following in each cluster's
`grid-system` namespace. These are user-managed and never created by
the installer or committed to Git.

| Resource | Purpose |
|----------|---------|
| `consumer-praxis-config` ConfigMap | Praxis consumer gateway config |
| `provider-praxis-config` ConfigMap | Praxis provider gateway config |
| `consumer-tls` Secret | TLS certificate and key for consumer identity |
| `provider-tls` Secret | TLS certificate and key for provider identity |
| `mock-inference-credential` Secret | Provider backend credentials |
| `GridNetwork`, `GridSite`, `InferenceProvider` CRs | Grid routing topology |

The `grid-routing-overlay` ConfigMap is created automatically by the
Grid Operator once SWIM membership converges.

## Usage

```bash
# Verify prerequisites
./scripts/preflight.sh inventory.yaml

# Install Grid + Praxis on all four clusters
./scripts/install.sh inventory.yaml

# Run verification Jobs
./scripts/verify.sh inventory.yaml

# Clean up (does not delete namespace or CRDs)
./scripts/uninstall.sh inventory.yaml
```

Every script requires the inventory file as the first argument. All
commands use explicit `--kube-context` selection and never modify the
user's current context.

## Charts

Both topologies use the same charts:

- `charts/grid-operator` -- Grid Operator with SWIM, CRD management
- `charts/praxis-gateway` -- Praxis AI Gateway (consumer or provider role)

## Security Boundary

Regardless of topology, the consumer and provider gateway roles maintain
separate trust boundaries:

- Consumer gateways receive routing overlays and client requests
- Provider gateways authenticate consumer peers via mTLS
- Provider credentials are mounted only in provider gateway Deployments
- Provider credentials must never be mounted into consumer gateways

## Requirements

- Helm 3.12+
- Grid operator image v0.1.1+ (the Helm chart requires `/healthz` and `/readyz`
  health endpoints on the metrics port; v0.1.0 images lack these endpoints and
  will fail liveness probes)
- kubectl configured with contexts for all four clusters
- Inter-cluster connectivity between SWIM ports
- TLS certificates and provider credentials prepared out-of-band
- Praxis gateway configuration ConfigMaps created in each cluster
- Grid custom resources applied to each cluster
