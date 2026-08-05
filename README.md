# Grid

Grid is a distributed control plane that connects AI
inference backends across Kubernetes clusters, cloud
providers, and third-party APIs into a single routable
mesh. It figures out where models are, which backends
are healthy, and which one should handle the next
request - then tells the
[Praxis](https://github.com/praxis-proxy/praxis)
gateway how to route.

## How It Works

Grid is an orchestrator, not a proxy. It watches
Kubernetes resources, discovers peer sites over a
gossip protocol (SWIM), propagates provider state
with CRDTs, scores candidates, and writes a routing
overlay that Praxis consumes at request time.

```text
+---------------------------+     +---------------------------+
|  Site A (Kubernetes)      |     |  Site B (Kubernetes)      |
|                           |     |                           |
|  +---------------------+ |     | +---------------------+   |
|  | Grid Operator       | |     | | Grid Operator       |   |
|  | - SWIM membership   |<-------->| - SWIM membership   |   |
|  | - CRDT state sync   | |     | | - CRDT state sync   |   |
|  | - scoring engine    | |     | | - scoring engine    |   |
|  | - overlay renderer  | |     | | - overlay renderer  |   |
|  +--------+------------+ |     | +--------+------------+   |
|           |               |     |          |                |
|           | ConfigMap     |     |          | ConfigMap      |
|           v               |     |          v                |
|  +---------------------+ |     | +---------------------+   |
|  | Praxis AI Gateway   | |     | | Praxis AI Gateway   |   |
|  | - request routing   |<-------->| - request routing   |   |
|  | - API translation   | | mTLS| | - API translation   |   |
|  | - credential inject | |     | | - credential inject |   |
|  +--------+------------+ |     | +--------+------------+   |
|           |               |     |          |                |
|           v               |     |          v                |
|  +---------------------+ |     | +---------------------+   |
|  | Inference Backends  | |     | | Inference Backends  |   |
|  | (llm-d, vLLM, etc.) | |     | | (Bedrock, Vertex,   |   |
|  +---------------------+ |     | | OpenAI, Anthropic)  |   |
+---------------------------+     | +---------------------+   |
                                  +---------------------------+
```

Grid handles the **control plane** (what should be
routable). Praxis handles the **data plane** (routing
and proxying actual requests).

## Key Concepts

**GridNetwork** - defines a logical mesh of sites.
Holds SWIM seeds, TLS settings, and gateway
references.

**GridSite** - represents one participating cluster
or location. Created automatically from SWIM
discovery or manually for seed peers.

**InferenceProvider** - declares model capacity at a
site: model name, backend kind (self-hosted,
cloud-managed, or API provider), health config, and
auth strategy.

**Routing overlay** - a versioned ConfigMap that Grid
writes for each gateway. Contains scored candidates,
cluster definitions with mTLS config, and credential
references. Praxis hot-reloads this without restarts.

**Scoring** - Grid scores each candidate using six
weighted signals before writing the overlay:

| Signal | Weight | What it measures |
|--------|-------:|------------------|
| Locality | 3.0 | How close the backend is |
| Queue depth | 3.0 | How busy the backend is |
| KV-cache utilization | 2.0 | Memory pressure |
| Prefix-cache hit ratio | 2.0 | Cache efficiency |
| Latency | 2.0 | Response time |
| Cost | 1.0 | Price per token |

## Request Flow

Once the overlay is loaded, a request flows through
two gateway pipelines:

```text
client request
  -> Praxis consumer/edge gateway
  -> intelligent_route selects a provider from overlay
  -> gateway-to-gateway mTLS
  -> Praxis provider gateway authenticates the peer
  -> provider_route validates the selected candidate
  -> credential_inject adds backend auth
  -> load_balancer picks a backend instance
  -> response returns to the client
```

Grid is never in the request path. All routing
decisions use a pre-computed local overlay file.

## Install

```console
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --namespace grid-system \
  --create-namespace
```

See the
[chart documentation](charts/grid-operator/README.md)
for values, RBAC, CRD upgrades, and SWIM service
exposure. Install a compatible
[Praxis](https://github.com/praxis-proxy/praxis)
gateway separately.

For Kustomize or raw manifests, see
[deploy/](deploy/README.md).

## Getting Started

The fastest way to see Grid working is to run a demo
with the pre-built validation images:

```console
# Install Rust if needed
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf \
    https://sh.rustup.rs | sh -s -- -y
  . "$HOME/.cargo/env"
fi

git clone https://github.com/praxis-proxy/grid.git
cd grid

# Set demo images (one compatible set)
export GRID_XTASK_GATEWAY_IMAGE=\
ghcr.io/praxis-proxy/grid-ai-rollup@sha256:\
95132eb39c0f568b5361a250002979c5063db427\
ff0fb63b59a93146fcb7ad31
export GRID_XTASK_OPERATOR_IMAGE=\
ghcr.io/praxis-proxy/grid-operator@sha256:\
654d9079e13c80e7891dcdd2eed52901ebd733833\
ae02d776a69a4170c00d9bb
export GRID_XTASK_MOCK_PROVIDER_IMAGE=\
ghcr.io/praxis-proxy/grid-mock-providers@sha256:\
60c9ac29782b2ce6c99eb4d82494bd10280ef06b\
453752486f6933927547d333
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent

# Build the test runner and launch the demo
cargo build -p forge
cargo xtask env run-grid-glb-demo \
  --forge-config demos/grid-glb-demo/forge.yaml \
  --quick \
  --teardown \
  2>&1 | tee grid-glb-demo-output.txt
```

Explore the
[deployable demonstrations](demos/README.md)
for automated runtime proofs of routing, failover,
security boundaries, and provider lifecycle.

For deploying onto existing clusters, see
[examples/helm/existing-clusters/](examples/helm/existing-clusters/README.md).

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `operator` | K8s controllers, CRDs, operator binary |
| `scoring` | Six-signal scoring engine and grid state |
| `certs` | Certificate generation and mTLS provider trait |
| `swim` | foca SWIM wrapper and encryption |
| `crdt` | Delta CRDT types (LWW, OR-Set, G-Counter) |
| `overlay-sync` | Sidecar for fast ConfigMap-to-file delivery |
| `mock-providers` | Mock OpenAI, Anthropic, Bedrock, Vertex APIs |
| `forge` | Demo test harness and validation runner |
| `xtask` | Dev task runner for multi-cluster test environments |

## Development

Requires Rust stable 1.96+, Rust nightly (for
rustfmt), and Docker/Podman + kind for integration
tests.

```console
make build          # workspace build
make test           # all tests
make lint           # clippy + fmt check + machete
make audit          # cargo audit + cargo deny check
make all            # build + fmt + lint + test + audit
```

See the [development guide](docs/development.md) and
[conventions](docs/conventions.md) for full details.

## Documentation

- [Architecture overview](docs/architecture/overview.md)
- [Custom resources](docs/architecture/crds.md)
- [Routing](docs/architecture/routing.md)
- [Scoring](docs/architecture/scoring.md)
- [Auth and policy](docs/architecture/auth.md)
- [Operations](docs/architecture/operations.md)
- [Consumer config](docs/architecture/consumer-config.md)
- [Documentation index](docs/README.md)
