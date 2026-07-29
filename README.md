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

## Getting started

Start with the [Global Ingress Demo](demos/grid-glb-demo/README.md).
It provides a copy-and-paste deployment for external client inference through
active Praxis edges, distinguishes that path from cluster-local workload
inference, narrates each request and failure scenario, records runtime evidence,
and tears the environment down when complete.

```bash
git clone https://github.com/praxis-proxy/grid.git
cd grid

export GRID_XTASK_GATEWAY_IMAGE=ghcr.io/nerdalert/praxis-ai@sha256:52ef822b9b1737979f0b61a570bddad539705456d3cefa94da9fa31d8350c147
export GRID_XTASK_OPERATOR_IMAGE=ghcr.io/nerdalert/grid-operator@sha256:b0aea67f5a534720b1ce98d4af420689e4f4c36ce73d85d1aa867e41f6c32522
export GRID_XTASK_MOCK_PROVIDER_IMAGE=ghcr.io/nerdalert/grid-mock-providers@sha256:2a0f32449ec38575cb2e91a8a5e9c70b4e0a990a219c4480fe364b8a52f21a59
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent

cargo build -p forge
cargo xtask env run-grid-glb-demo \
  --forge-config demos/grid-glb-demo/forge.yaml \
  --quick \
  --teardown \
  2>&1 | tee grid-glb-demo-output.txt
```

These temporary integration images are published under `ghcr.io/nerdalert`
until equivalent project images are available. The detailed demo guide
documents prerequisites, full validation mode, evidence, and troubleshooting.

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
