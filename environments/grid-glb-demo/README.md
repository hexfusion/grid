# Grid GLB Demo Environment

Multi-cluster Grid environment demonstrating external ingress and
file-based Grid overlay hot-reload. Three Kind clusters simulate a
peer-site mesh topology. All three are equal peer sites — "edge" and
"provider" are per-request roles, not permanent cluster types. In this
demo flow, `site-us-east` acts as the edge entry point and
`site-us-west` / `site-us-central` host simulated inference backends.

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
   |site-us-west  |                |site-us-central|
   |  (Kind)      |                |  (Kind)       |
   |  GridSite    |                |  GridSite     |
   |  Operator    |                |  Operator     |
   |  mock-provs  |                |  mock-provs   |
   |  LB :8080    |                |  LB :8080     |
   |  SWIM :7946  |                |  SWIM :7946   |
   +--------------+                +--------------+
         ^      SWIM/CRDT gossip          ^
         +--------------------------------+
```

Each provider-role site exposes its mock-inference backend through a
MetalLB-backed LoadBalancer Service (`provider-gateway`) on port 8080.
The `site-us-east` cluster's GridNetwork references these LoadBalancer
IPs as `clusterEndpoints`, enabling the Grid overlay to route inference
requests across the cross-cluster Docker network.

All three sites run Grid operators with SWIM membership enabled. Each
operator advertises via a MetalLB-backed LoadBalancer Service
(`operator-swim-lb`) on UDP port 7946, and seeds are wired through
per-site GridNetwork templates. Operators discover each other through
SWIM, propagate provider state via CRDT, and the `site-us-east`
operator renders a routing overlay ConfigMap with candidates from all
three sites.

Two host services complete the data path:

- **grid-overlay-sync-us-east** watches the operator-generated routing
  overlay ConfigMap (`grid-overlay-glb-demo-consumer-gateway`) on
  `site-us-east` and writes its `grid-config.json` key to a
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

- The demo uses local development images tagged `:glb-demo`:
  - `grid-overlay-sync:glb-demo` (overlay ConfigMap poller)
  - `praxis-ai:glb-demo` (edge gateway with hot-reload + session affinity)
  - `grid-operator:glb-demo` (operator with overlay metadata emission)
  - `grid-mock-providers:glb-demo` (simulated inference backends)
- Transport between edge and providers is set to `plaintext` for
  initial development. Production requires `mutual_tls` with proper
  SNI and certificate references.
- Session affinity is enabled (`X-Session-Id` header) with in-memory
  bindings. This is single-process/demo scope only — bindings do not
  survive restarts or distribute across replicas. The current verifier
  validates that the configuration loads; provider-attributed stickiness
  proof requires backend attribution in responses or logs.
- The operator renderer emits overlay metadata (`stable_id`,
  `admission_state`, `selection_tier`, `rank`, `generated_at`) when it
  has provider candidates. SWIM seed wiring is deterministic: each
  site's GridNetwork seeds reference the other two sites' SWIM LB IPs,
  and operators discover each other through SWIM/CRDT gossip.
- Provider-role sites advertise their data-plane gateway address via
  SWIM state broadcast (`GRID_GATEWAY_ADDRESS` set from Forge-captured
  provider gateway IP). Peer operators create `GridSite` resources with
  `spec.egress.address` and advance them to Active phase, enabling
  CRDT-discovered providers in the routing overlay. This is a
  demo-scoped bridge using Forge captures; the production path is
  operator self-discovery of its own Service IP.
- Geo/load-aware ordering is implemented in the overlay renderer.
  Semantic routing remains a planned follow-up capability. This demo
  proves the external ingress data path, SWIM cross-cluster discovery,
  hot-reload behavior, and edge process stability.

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

This creates three Kind clusters (`site-us-east`, `site-us-west`,
`site-us-central`) with a shared Docker network (`grid-glb-demo-net`).
The host edge services are marked `autoStart: false`, so `up` creates
the clusters, networking, stacks, and runtime files without starting
the local edge containers.

### 3. Apply stacks (Pass 1 — infrastructure + SWIM LB capture)

Apply all stacks. The `swim-lb` stack on each cluster creates a SWIM
LoadBalancer Service and captures its MetalLB-assigned IP. The
`inference-sim` stack captures provider gateway IPs. Per-site
`template-manifest` steps for GridNetwork and operator env patches will
fail on this pass because cross-cluster captures are not yet available —
this is expected.

```console
praxis-forge stack apply --config environments/grid-glb-demo/forge.yaml
```

### 4. Re-apply site-demo stacks (Pass 2 — seed wiring)

With all SWIM LB IPs and provider gateway IPs captured in
`.forge/state.json`, re-apply the site-demo stacks. Templates now
resolve all cross-cluster capture references:

```console
praxis-forge stack apply site-us-east --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply site-us-west --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply site-us-central --config environments/grid-glb-demo/forge.yaml
```

Each site-demo stack renders a per-site GridNetwork with SWIM seeds
pointing to the other two sites, and patches the operator Deployment
with the correct `GRID_SWIM_ADVERTISE_ADDR` (LB IP, not Pod IP) and
`GRID_SWIM_SITE_NAME`. The `site-us-east-demo` stack also renders the
edge Praxis config with captured provider gateway IPs.

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
make overlay-sync-image operator-image
docker tag grid-overlay-sync:latest grid-overlay-sync:glb-demo
docker tag grid-operator:latest grid-operator:glb-demo
# Build praxis-ai:glb-demo from the AI hot-reload branch (ai/ repo).
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

To exercise the session-affinity path, include the `X-Session-Id`
header. Repeated requests with the same session ID should keep the same
binding while the selected provider remains eligible. The mock response
does not currently include provider attribution, so use edge logs or the
automated verifier once attribution support is added to prove stickiness.

```console
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-Session-Id: demo-session-1" \
  -H "X-Model: llama-3.1-8b" \
  -d '{"model":"llama-3.1-8b","messages":[{"role":"user","content":"hello"}]}'
```

### Shared Runtime Layout

Both host services share a runtime directory on the Docker host:

```
.forge/runtime/edge-us-east/
  grid-config.json    # written by overlay-sync, read by edge
  praxis/praxis.yaml  # rendered by Forge from captured provider IPs
  tls/                # reserved for future mTLS certificates

.forge/runtime/kubeconfig/site-us-east/
  config              # rewritten kubeconfig mounted into overlay-sync
```

The runtime directories are prepared by Forge before starting host
services. Writable bind-mount sources under `.forge/runtime/` are
created with world-writable permissions for this local demo because the
containers run as non-root users whose host UID is not known in advance.
These directories are ephemeral, Forge-owned, gitignored (`.forge/` in
root `.gitignore`), and must not be committed.

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
