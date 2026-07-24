# Grid GLB Demo Environment

Multi-cluster Grid environment demonstrating external ingress and
file-based Grid overlay hot-reload. Three Kind clusters simulate a
realistic topology: one edge-control plane running Praxis AI as the
external entry point, and two provider clusters hosting simulated
inference backends exposed via MetalLB LoadBalancer Services.

## Architecture

```
                   +------------------+
                   |  Client (curl)   |
                   +--------+---------+
                            | :8080
               +------------v------------+
               |   grid-edge-us-east     |
               |   (Praxis AI gateway)   |
               +------------+------------+
                            | overlay routing
            +---------------+---------------+
            v                               v
   +--------------+                +--------------+
   |provider-east |                |provider-west |
   |  (Kind)      |                |  (Kind)      |
   |  GridSite    |                |  GridSite    |
   |  Operator    |                |  Operator    |
   |  mock-provs  |                |  mock-provs  |
   |  LB :8080    |                |  LB :8080    |
   +--------------+                +--------------+
```

Each provider cluster exposes its mock-inference backend through a
MetalLB-backed LoadBalancer Service (`provider-gateway`) on port 8080.
The edge-control cluster's GridNetwork references these LoadBalancer
IPs as `clusterEndpoints`, enabling the Grid overlay to route inference
requests across the cross-cluster Docker network.

Two host services complete the data path:

- **grid-overlay-sync-us-east** watches the operator-generated routing
  overlay ConfigMap (`grid-overlay-glb-demo-consumer-gateway`) on
  edge-control and writes its `grid-config.json` key to a
  shared runtime directory (`.forge/runtime/edge-us-east/` when run
  from the repository root).
- **grid-edge-us-east** runs Praxis AI with file-based Grid config,
  reading from the same runtime directory. Forge renders its
  `praxis.yaml` into `.forge/runtime/edge-us-east/praxis/` after the
  provider gateway IPs have been captured. It listens on
  `127.0.0.1:8080` for local client requests.

## Current Status

### Runnable Now

- Cluster creation with cross-cluster Docker networking
- Gateway API CRDs, MetalLB with auto-configured address pools
- Grid operator (CRDs + deployment) on all clusters
- Per-cluster Grid CRD resources (GridNetwork, GridSites, InferenceProviders)
- Mock inference backends (Deployment + ClusterIP Service) on provider clusters
- Provider gateway LoadBalancer Services exposed via MetalLB
- Automatic capture of provider gateway IPs into Forge state
- Template-manifest rendering of GridNetwork with captured IPs
- Container-reachable kubeconfig export for host services
- Config validation passes with full service definitions

### Current Development Limits

- The demo uses local development images:
  - `grid-overlay-sync:latest`
  - `praxis-ai:hot-reload-grid-route`
- Transport between edge and providers is set to `plaintext` for
  initial development. Production requires `mutual_tls` with proper
  SNI and certificate references.
- Geo/load-aware routing, session affinity, and semantic routing are
  planned follow-up capabilities. This demo proves the external ingress
  data path and hot-reload behavior, not those policy layers.

## Prerequisites

- Docker (required for cross-cluster networking)
- [kind](https://kind.sigs.k8s.io/) v0.20+
- `praxis-forge` binary (built from this repo: `cargo build -p forge`)
- `kubectl`

## Demo Workflow

### 1. Validate the environment config

```console
praxis-forge config validate --config environments/grid-glb-demo/forge.yaml
```

### 2. Create clusters and network

```console
praxis-forge up --config environments/grid-glb-demo/forge.yaml
```

This creates three Kind clusters (`edge-control`, `provider-east`,
`provider-west`) with a shared Docker network (`grid-glb-demo-net`).
The host edge services are marked `autoStart: false`, so `up` creates
the clusters, networking, stacks, and runtime files without starting
the local edge containers.

### 3. Apply provider stacks

Apply stacks to provider clusters first. The `inference-sim` stack
waits for the `provider-gateway` LoadBalancer Service to receive an IP,
then captures it into Forge state automatically:

```console
praxis-forge stack apply provider-east --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply provider-west --config environments/grid-glb-demo/forge.yaml
```

This installs Gateway API CRDs, MetalLB, the Grid operator, provider
Grid CRDs, mock-inference Deployments, and the `provider-gateway`
LoadBalancer Service on each provider cluster. The captured IPs are
stored in `.forge/state.json` for use by downstream stacks.

### 4. Apply edge-control stacks

```console
praxis-forge stack apply edge-control --config environments/grid-glb-demo/forge.yaml
```

The `edge-demo` stack uses `template-manifest` to render
`gridnetwork.yaml` with the captured provider gateway IPs. It also
renders the edge Praxis config to
`.forge/runtime/edge-us-east/praxis/praxis.yaml`. No manual YAML
editing is required.

### 5. Verify cluster status

```console
praxis-forge status --config environments/grid-glb-demo/forge.yaml
```

All three clusters should show `phase=running, live`.

### 6. Verify provider gateway reachability

From the Docker host, confirm the provider gateways respond (IPs
are in `.forge/state.json` under `captures`):

```console
curl -s http://<east-ip>:8080/health
curl -s http://<west-ip>:8080/health
```

Both should return `ok` from the mock-inference health endpoint.

### 7. Start host services

Build the local images and start the two host services:

```console
make overlay-sync-image
# Build praxis-ai:hot-reload-grid-route from the AI hot-reload branch.
```

```console
praxis-forge service start grid-overlay-sync-us-east --config environments/grid-glb-demo/forge.yaml
praxis-forge service start grid-edge-us-east --config environments/grid-glb-demo/forge.yaml
```

The overlay-sync service writes `grid-config.json` to
`.forge/runtime/edge-us-east/`. The edge service mounts that directory
read-only at `/etc/grid` and begins accepting requests on
`127.0.0.1:8080`.

### 8. Send a test request

Once both services are running:

```console
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d @environments/grid-glb-demo/fixtures/requests/shared-model.json
```

### Shared Runtime Layout

Both host services share a runtime directory on the Docker host:

```
.forge/runtime/edge-us-east/
  grid-config.json    # written by overlay-sync, read by edge
  praxis/praxis.yaml  # rendered by Forge from captured provider IPs
  tls/                # reserved for future mTLS certificates

.forge/runtime/kubeconfig/edge-control/
  config              # rewritten kubeconfig mounted into overlay-sync
```

The runtime directories are created by `praxis-forge up` and service
startup. They are gitignored (`.forge/` in root `.gitignore`) and must
not be committed.

## Automated Proof

The GLB ingress verifier asserts:

```console
cargo xtask env verify-grid-glb-ingress \
  --forge-config environments/grid-glb-demo/forge.yaml
```

The proof checks the running Forge environment, verifies provider
gateway reachability, sends an inference request through the edge,
edits the local overlay file to remove one provider, observes a new
`overlay reloaded` log entry, sends a second inference request, and
confirms the edge container ID and restart count remain stable.

## Teardown

```console
praxis-forge down --config environments/grid-glb-demo/forge.yaml
```
