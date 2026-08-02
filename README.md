# Grid

Grid is the Kubernetes control plane for multi-site AI routing with
[Praxis](https://github.com/praxis-proxy/praxis) as the request data plane.

## What Grid does

- Reconciles `GridNetwork`, `GridSite`, and provider CRDs.
- Forms site membership with SWIM and propagates provider state with CRDTs.
- Manages Grid trust material for mTLS between sites.
- Scrapes configured provider metrics and scores routing candidates.
- Renders Praxis routing overlay `ConfigMap`s consumed by gateway deployments.
- Projects provider credential references into overlays without writing token
  values into Grid routing data.

## What Grid does not do

Grid does not proxy model traffic, translate provider APIs, or run Praxis HTTP
filters. The Praxis gateway stack handles TLS, proxying, and backend I/O;
Praxis AI supplies the AI-specific routing and credential filters.

## Install

```bash
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --namespace grid-system \
  --create-namespace
```

See the [chart documentation](charts/grid-operator/README.md) for values,
RBAC namespace configuration, CRD upgrade procedures, and SWIM Service
exposure. Install a compatible [Praxis](https://github.com/praxis-proxy/praxis)
gateway separately.

For Kustomize or raw manifests, see [deploy/](deploy/README.md).

## Getting started

Start with the [Global Ingress Demo](demos/grid-glb-demo/README.md).
It provides a copy-and-paste deployment for external client inference through
active Praxis edges, distinguishes that path from cluster-local workload
inference, narrates each request and failure scenario, records runtime evidence,
and tears the environment down when complete.

For cluster-local routing without a traffic manager, see the
[Workload Inference Demo](demos/grid-workload-inference/README.md).

For the compact three-cluster topology in which every site contains separate
consumer and provider gateways, see the
[Combined-Site Demo](demos/grid-combined-site/README.md). This new standalone
demo provides automated runtime proofs for local preference, remote fallback,
provider lifecycle, security boundaries, convergence, and recovery.

For deploying Grid onto existing clusters with Helm, see
[examples/helm/existing-clusters/](examples/helm/existing-clusters/README.md).

```bash
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y
  . "$HOME/.cargo/env"
fi

git clone https://github.com/praxis-proxy/grid.git
cd grid

export GRID_XTASK_GATEWAY_IMAGE=ghcr.io/praxis-proxy/grid-ai-rollup@sha256:95132eb39c0f568b5361a250002979c5063db427ff0fb63b59a93146fcb7ad31
export GRID_XTASK_OPERATOR_IMAGE=ghcr.io/praxis-proxy/grid-operator@sha256:654d9079e13c80e7891dcdd2eed52901ebd733833ae02d776a69a4170c00d9bb
export GRID_XTASK_MOCK_PROVIDER_IMAGE=ghcr.io/praxis-proxy/grid-mock-providers@sha256:60c9ac29782b2ce6c99eb4d82494bd10280ef06b453752486f6933927547d333
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent

cargo build -p forge
cargo xtask env run-grid-glb-demo \
  --forge-config demos/grid-glb-demo/forge.yaml \
  --quick \
  --teardown \
  2>&1 | tee grid-glb-demo-output.txt
```

These immutable validation images form one compatible demo set. The Praxis AI
image rolls up the open intelligent-routing PR stack for review. The detailed
demo guide documents prerequisites, full validation mode, evidence, and
troubleshooting.

The development guide documents focused test and validation commands for
contributors.

## Documentation

- [Documentation index](docs/README.md)
- [Architecture overview](docs/architecture/overview.md)
- [Custom resources](docs/architecture/crds.md)
- [Routing](docs/architecture/routing.md)
- [Scoring](docs/architecture/scoring.md)
- [Auth and policy](docs/architecture/auth.md)
- [Operations](docs/architecture/operations.md)
- [Consumer config](docs/architecture/consumer-config.md)
- [CI Kind E2E strategy](docs/architecture/ci-kind-e2e.md)

## Development

- [Development guide](docs/development.md)
- [Conventions](docs/conventions.md)
