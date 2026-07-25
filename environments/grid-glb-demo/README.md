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
  survive restarts or distribute across replicas. Each mock provider
  returns an `X-Grid-Demo-Provider` response header containing its
  site name (e.g., `site-us-west`), enabling the automated verifier
  to prove session stickiness and drain behavior end-to-end.
- The operator renderer emits overlay metadata (`stable_id`,
  `admission_state`, `selection_tier`, `rank`, `generated_at`) when it
  has provider candidates. SWIM seed wiring is deterministic: each
  site's GridNetwork seeds reference the other two sites' SWIM LB IPs,
  and operators discover each other through SWIM/CRDT gossip.
- Provider-role sites discover their own data-plane gateway address
  by reading the `provider-gateway` Service LoadBalancer IP from
  Kubernetes (`GRID_GATEWAY_SERVICE_NAME` env var, default
  `provider-gateway`). An initial lookup runs at startup; a background
  poller then retries periodically (default 5 s, configurable via
  `GRID_GATEWAY_DISCOVERY_INTERVAL_MS`) until the address appears and
  continues polling for changes. When a new or changed address is
  discovered, it is pushed to the SWIM runtime via a watch channel and
  broadcast to peers. Peer operators create `GridSite` resources with
  `spec.egress.address` and advance them to Active phase, enabling
  CRDT-discovered providers in the routing overlay. The explicit
  `GRID_GATEWAY_ADDRESS` env var is supported as an override for
  non-standard topologies; when set, the background poller is skipped.
- Geo/load-aware ordering is implemented in the overlay renderer.
  Advanced route classification is deferred to a future iteration —
  this demo proves the external ingress data path, SWIM cross-cluster
  discovery, hot-reload behavior, session affinity, and provider
  attribution without route class propagation.

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

### 3. Build and load local images

Build the Grid-local images and load Kubernetes images into the Kind
clusters before applying stacks. The demo uses `imagePullPolicy:
Never`, so Kind must already have these images locally.

```console
make glb-demo-images
```

This builds three images tagged directly as `:glb-demo` (no `:latest`
intermediate):

- `grid-operator:glb-demo`
- `grid-overlay-sync:glb-demo`
- `grid-mock-providers:glb-demo`

Load the Kubernetes images into the Kind clusters:

```console
praxis-forge cluster load-image site-us-east grid-operator:glb-demo \
  --config environments/grid-glb-demo/forge.yaml
praxis-forge cluster load-image site-us-west grid-operator:glb-demo \
  --config environments/grid-glb-demo/forge.yaml
praxis-forge cluster load-image site-us-central grid-operator:glb-demo \
  --config environments/grid-glb-demo/forge.yaml
praxis-forge cluster load-image site-us-west grid-mock-providers:glb-demo \
  --config environments/grid-glb-demo/forge.yaml
praxis-forge cluster load-image site-us-central grid-mock-providers:glb-demo \
  --config environments/grid-glb-demo/forge.yaml
```

Build `praxis-ai:glb-demo` from the AI repo (hot-reload branch)
before starting the host edge service. The edge gateway requires
file-based overlay hot-reload and session affinity support; advanced
route classification is not required for this demo.

### 4. Apply stacks (Pass 1 — infrastructure + SWIM LB capture)

Apply all stacks. The `swim-lb` stack on each cluster creates a SWIM
LoadBalancer Service and captures its MetalLB-assigned IP. The
`inference-sim` stack captures provider gateway IPs. Per-site
`template-manifest` steps for GridNetwork and operator env patches will
fail on this pass because cross-cluster captures are not yet available —
this is expected.

```console
praxis-forge stack apply site-us-east --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply site-us-west --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply site-us-central --config environments/grid-glb-demo/forge.yaml
```

### 5. Re-apply site-demo stacks (Pass 2 — seed wiring)

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

### 6. Verify cluster status

```console
praxis-forge status --config environments/grid-glb-demo/forge.yaml
```

All three clusters should show `phase=running, live`.

### 7. Verify provider gateway reachability

From the Docker host, confirm the provider gateways respond (IPs
are in `.forge/state.json` under `captures`):

```console
curl -s http://<east-ip>:8080/health
curl -s http://<west-ip>:8080/health
```

Both should return `ok` from the mock-inference health endpoint.

### 8. Start host services

Start the two host services:

```console
praxis-forge service start grid-overlay-sync-us-east --config environments/grid-glb-demo/forge.yaml
praxis-forge service start grid-edge-us-east --config environments/grid-glb-demo/forge.yaml
```

The overlay-sync service writes `grid-config.json` to
`.forge/runtime/edge-us-east/`. The edge service mounts that directory
read-only at `/etc/grid` and begins accepting requests on
`127.0.0.1:8080`.

### 9. Send a test request

Once both services are running:

```console
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d @environments/grid-glb-demo/fixtures/requests/shared-model.json
```

To exercise the session-affinity path, include the `X-Session-Id`
header. Repeated requests with the same session ID keep the same
provider binding while the selected provider remains eligible. The
response `X-Grid-Demo-Provider` header identifies which site served
the request (demo-only attribution, not part of the production API).

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

The 23-step proof checks the running Forge environment, verifies
provider gateway reachability, validates overlay metadata
(`stable_id`, `admission_state`, `selection_tier`, `rank`,
`generated_at` — values checked, not just presence), sends an
inference request through the edge and verifies provider attribution
(`X-Grid-Demo-Provider` header) and model echo, proves session
affinity (binding, reuse, drain with `existing_only`, and drain
verification via provider attribution header), edits the local
overlay file to remove one provider, observes a new `overlay
reloaded` log entry, sends a post-reload inference request with
provider attribution, and confirms the edge container ID and restart
count remain stable.

## Backlog / production gaps

- Plaintext provider transport: production should use mutual TLS.
- Advanced route classification is deferred.

## Teardown

```console
praxis-forge down --config environments/grid-glb-demo/forge.yaml
```
