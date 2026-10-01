# AGN Operations

This guide covers AGN production installation, network formation, site lifecycle,
routing configuration, security, and observability. Development-only
orchestration is isolated under **Development Validation Environments**.

## 1. Deploy the AGN Operator

### Install

AGN provides a Helm chart and Kustomize manifests for the operator and CRDs.

**Option 1: Helm (recommended)**

```console
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --namespace grid-system \
  --create-namespace
```

To grant resource access to additional namespaces:

```console
helm upgrade grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --reuse-values \
  --set "resourceNamespaces={app-ns,data-ns}" \
  --namespace grid-system
```

The chart installs and upgrades the CRDs. A platform that owns them sets
`crds.enabled: false`. The chart README covers adopting CRDs that an older
release installed.

Uninstalling the chart removes namespaced resources. It retains the CRDs while
`crds.keep` is true, which is the default.
Custom resources created by other chart releases (e.g., grid-site) are
not affected. See the [chart README](../../charts/grid-operator/README.md)
for the full values reference.

`enrollment.enabled`, `signals.enabled`, and the SWIM Service default to off.
Turning on enrollment also turns on a LoadBalancer SWIM Service and sets
`gateway.serviceName` to `grid-gateway`. Signals need `swim.service.enabled`
and, on a LoadBalancer, the `Local` external traffic policy. With enrollment,
the `grid-enrollment` chart runs the service that issues site identity, and its
bootstrap Job creates the database credentials Secret. See
[Hub and Site Install](../../examples/helm/hub-site/README.md) for the full
flow.

**Option 2: Kustomize**

```console
# Complete AGN deployment (CRDs + `grid-operator`)
kubectl apply -k deploy/

# Or step-by-step:
kubectl apply -k deploy/crds/
kubectl apply -k deploy/operator/
```

**Option 3: Generated CRDs (development)**

```console
# Generate CRDs from source and apply directly
cargo run -p operator --bin generate_crds | kubectl apply -f -
kubectl apply -k deploy/operator/
```

For regenerating CRDs after schema changes:

```console
make generate-crds
```

This writes `deploy/crds/` and `charts/grid-operator/templates/crds/`. CI runs
`make crds-check`, which fails when the committed CRDs no longer match the
Rust types.

**Container image pattern**: The operator Containerfile uses a
`rust:1.96-alpine` builder with BuildKit cache mounts, and an `alpine:3.23`
runtime with a non-root user and no build toolchain.  The Kubernetes Deployment
adds a restricted security context for OpenShift-style clusters.

**Image availability**: Production deployments pin project-owned release or
commit-digest images. Local image loading is documented under
**Development Validation Environments**.

### Deployment Examples

See sample Custom Resource configurations:
- `config/samples/` - standard operator sample CRs
- `deploy/examples/single-cluster-api-provider/` - minimal example with external API

The install package creates:

| Resource | Name | Scope |
|---|---|---|
| `Namespace` | `grid-system` | cluster |
| `ServiceAccount` | `grid-operator` | `grid-system` |
| `ClusterRole` | `grid-operator-crd` | cluster |
| `ClusterRoleBinding` | `grid-operator-crd` | cluster |
| `ClusterRole` | `grid-operator-resources` | cluster (verb definitions only) |
| `RoleBinding` | `grid-operator-resources` | `default` namespace |
| `RoleBinding` | `grid-operator-resources` | `grid-system` namespace |
| `Deployment` | `grid-operator` | `grid-system` |

The operator runs as a single binary with multiple
controllers (one per CRD type) in the same process. The SWIM
runtime starts with the process when `GRID_SWIM_BIND_ADDR` is set. With
`GRID_SWIM_REQUIRE_KEY`, it holds all traffic until a `GridNetwork` loads its
key or declares none.

**Important**: AGN deploys only the operator and CRDs. Cluster lifecycle,
Praxis AI gateways, inference runtimes, load-balancer integrations, and DNS
remain deployment-platform responsibilities.

Praxis AI gateway deployment is separate and requires:
1. Praxis AI image with required filters (`intelligent_route`, `credential_inject`)
2. Consumer gateway configuration referencing AGN-generated ConfigMaps
3. Provider gateway deployment with AGN-compatible endpoints

### Operator image

The project-owned operator image path is:

```
ghcr.io/praxis-proxy/grid-operator
```

Repository CI publishes source-SHA images, and the release workflow publishes
versioned images with SBOM and provenance attestations.

**Tag policy:**

| Tag | Mutability | When pushed |
|---|---|---|
| `sha-<7-char-commit>` | Immutable | Pushed for every commit to `main` |
| `v<version>` | Immutable release tag | Pushed for each release tag |

Deployments should pin an immutable digest:

```yaml
image: ghcr.io/praxis-proxy/grid-operator@sha256:8c8271aa589fbd81e346b75ae580be9e8085c3b283b4e6a99e2b9adcea73e12d
```

**Override:** replace the `image:` field in
`deploy/operator/deployment.yaml` or patch the
Deployment after apply:

```console
kubectl set image deployment/grid-operator \
  -n grid-system \
  operator=ghcr.io/praxis-proxy/grid-operator:sha-<commit>
```

**Registry namespace:** the image is published under `ghcr.io/praxis-proxy/`.

**CI publishing setup:** the `publish` job in
`.github/workflows/helm.yaml` publishes source-SHA images from `main`, using
the images the e2e jobs tested. `.github/workflows/release.yaml` publishes version and source-SHA tags
after the tagged source passes release gates.

**Security:** the operator image contains only the
statically linked operator binary.  No secrets, tokens,
SWIM encryption keys, or credentials are baked into the
image. Release images carry an SBOM and provenance attestations.
Images are not signed yet.

### RBAC permissions

RBAC is split into two `ClusterRoles`:

1. **`grid-operator-crd`** — cluster-scoped CRD access,
   bound via a `ClusterRoleBinding`.
2. **`grid-operator-resources`** — namespaced `Secret` and
   `ConfigMap` access, bound via per-namespace
   `RoleBindings`.

The Kustomize install includes a `RoleBinding` in each of the `default` and
`grid-system` namespaces. The Helm chart binds the release namespace plus each
entry in `resourceNamespaces`. Generated `ConfigMaps` use server-side
apply (`patch`). SSA on a non-existent resource requires `create` permission,
so both `create` and `patch` are granted for `configmaps`. The SWIM revision
`ConfigMap` is read with `get` and written with `replace`, which needs
`update`. `Secrets` are only created, never patched. `delete` is granted only
on `gridsites`.

**AGN CRDs (cluster-scoped, `grid-operator-crd`):**

| Resource | Verbs | Why |
|---|---|---|
| `gridnetworks` | `get`, `list`, `watch`, `patch` | Controller watch loop. The operator never writes a `GridNetwork` spec |
| `gridnetworks/status` | `get`, `patch` | Phase, conditions, connectedSites, distributedProviderCount |
| `gridsites` | `get`, `list`, `watch`, `patch`, `create`, `update`, `delete` | Controller watch, `siteDiscovery.mode: auto` creation from SWIM Alive members, and collection of stale auto-created sites |
| `gridsites/status` | `get`, `patch` | Phase, conditions, discovered, observedGeneration |
| `inferenceproviders` | `get`, `list`, `watch`, `patch` | Controller watch and `hostSelector` matching |
| `inferenceproviders/status` | `get`, `patch` | Phase, conditions, matchingSites, observedGeneration |
| `agenttoolproviders` | `get`, `list`, `watch`, `patch` | Controller watch |
| `agenttoolproviders/status` | `get`, `patch` | Status writes |

**Events (`events.k8s.io`, `grid-operator-resources`):**

| Resource | Verbs | Why |
|---|---|---|
| `events` | `create`, `patch` | Published on `GridSite` phase/reason transitions with action `GatewayProbe` |

`GridSite` is cluster-scoped, so its events land in the operator namespace:
`kubectl -n <operator namespace> get events --field-selector involvedObject.kind=GridSite`.

**Core resources (namespaced, `grid-operator-resources`):**

| Resource | Verbs | Why |
|---|---|---|
| `secrets` | `get`, `create` | Read TLS certs, SWIM key, credential refs. Create the enrolled site identity, and a dev CA only with `GRID_DEV_SELF_SIGNED_CA`. Never patch an existing `Secret` |
| `configmaps` | `get`, `create`, `patch`, `update` | SSA-create routing overlay, serving config, and consumer config `ConfigMaps`. Read and replace the SWIM revision `ConfigMap` |
| `services` | `get` | Read the gateway and SWIM `Services` for address discovery, and each `gatewayRef` Service for the consumer listener port |

The `grid-operator-resources` `ClusterRole` is never bound
cluster-wide.  It takes effect only in namespaces where a
`RoleBinding` references it.

### Secret access rules

The operator reads `Secrets` in the namespace declared by
each `SecretRef` in the CRD spec.  It does not search
across namespaces or list `Secrets`.

| Secret path | Keys read | Keys written |
|---|---|---|
| `spec.tls.siteSecretRef` | `tls.crt`, `tls.key` (client cert + private key for mTLS gateway probes, with key bytes wrapped in `Zeroizing`) | `tls.crt`, `tls.key` (created only by enrollment or the dev CA path) |
| `spec.tls.caSecretRef` | `ca.crt` (existence check) | `ca.crt` (created by enrollment), or `ca.crt` and `ca.key` (created by the dev CA path) |
| `spec.tls.swimKeyRef` | `key` (or custom key field) | — |
| `spec.auth.secretRef` | existence + UTF-8 validation | — |

Secret writes use `create` and never replace an existing `Secret`. Without
enrollment or `GRID_DEV_SELF_SIGNED_CA`, the operator writes no `Secret`.

Credential token bytes are never written to `ConfigMaps`,
overlays, status fields, or logs.

### ConfigMap write scope

| `ConfigMap` | Naming | Data key | Namespace |
|---|---|---|---|
| Routing overlay | `grid-overlay-{network}-{gateway}` | `routing-overlay.json`, `routing-config.json` | `GatewayRef.namespace` |
| Consumer config | `grid-consumer-<network>-<gateway>` | `praxis.yaml` | `GatewayRef.namespace` |
| Serving config | `grid-serving-{network}-{gateway}` | `serving-config.json` | `GatewayRef.namespace` |
| SWIM revision high-water mark | `grid-swim-revision-hwm-<site>` | Revision and node generation | Operator namespace |

The operator writes the serving config only when `signalTransport` is `poll`.

The consumer config listener port comes from a `get` on the gateway Service
`GatewayRef.name` in `GatewayRef.namespace`. The resources `ClusterRole` grants
`get` on any Service in the namespaces it is bound to, so any release name works.

### What is not granted

Neither `ClusterRole` grants:

- `pods`, `pods/exec`, `pods/log`, `pods/portforward`
- `deployments`, `ingresses`
- `services` `list` or `watch`, or any `services` write
- `secrets` `patch`, `update`, `delete`, `list`, `watch`
- `configmaps` `delete`, `list`, `watch`
- `update` on anything except `configmaps` and `gridsites`
- `delete` on anything except `gridsites`

### Adding namespaces

The Kustomize install grants `Secret` and `ConfigMap` access
only in the `default` and `grid-system` namespaces. The Helm chart grants it in
the release namespace and each entry in `resourceNamespaces`. To grant access in
additional namespaces (e.g. the gateway namespace
referenced by `GatewayRef`, or the namespace holding TLS
`Secrets`) with Kustomize, create a `RoleBinding` in each:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: grid-operator-resources
  namespace: praxis-system          # the target namespace
subjects:
  - kind: ServiceAccount
    name: grid-operator
    namespace: grid-system
roleRef:
  kind: ClusterRole
  name: grid-operator-resources
  apiGroup: rbac.authorization.k8s.io
```

Add one `RoleBinding` per namespace referenced by
`GatewayRef.namespace`, `tls.caSecretRef.namespace`,
`tls.siteSecretRef.namespace`, `tls.swimKeyRef.namespace`,
and `auth.secretRef.namespace` in your CRD specs.

### Deployment configuration

The operator binary reads the environment variables below. The `Deployment` in
`deploy/operator/deployment.yaml` sets only the SWIM bind and advertise
addresses, the advertise fallback, the gateway namespace and port, and the
metrics address. The Helm
chart sets more from its values.

SWIM endpoint values accept all of these forms:

```text
10.0.0.4:7946
[2001:db8::4]:7946
grid-swim.example.internal:7946
```

DNS names are resolved before the SWIM runtime starts. All usable seed
addresses are retained, sorted, and deduplicated. For an advertised hostname,
the first address in that deterministic ordering becomes the concrete address
published to SWIM. Resolution is bounded; an invalid or unresolvable
advertise endpoint prevents SWIM startup rather than silently advertising an
unrelated address.

| Variable | Purpose |
|---|---|
| `GRID_SWIM_BIND_ADDR` | UDP address to bind the SWIM listener |
| `GRID_SWIM_ADVERTISE_ADDR` | Advertised SWIM endpoint. Accepts `ip:port`, `[ipv6]:port`, or `hostname:port`. Unset, the operator advertises the bind address and refuses to start if that address is unspecified. The Kustomize `Deployment` sets it to `$(POD_IP):7946` |
| `GRID_SWIM_SERVICE_NAME` | SWIM Service in the operator namespace, which must be type LoadBalancer. The operator advertises its LoadBalancer address, waiting for it without a fallback and reporting not ready until then. Controllers run during the wait, and writes derived from membership wait for a first peer or a 15 second grace. When three polls in a row no longer list the advertised hostname or IP, the operator leaves the cluster and exits, so the restarted pod advertises the new one |
| `GRID_SWIM_ADVERTISE_FALLBACK` | Marks a `GRID_SWIM_ADVERTISE_ADDR` equal to it as the Pod IP default. The operator then builds the Pod IP endpoint itself, bracketing IPv6, or discovers the LoadBalancer address when `GRID_SWIM_SERVICE_NAME` names a Service. An older operator ignores it and advertises the Pod IP |
| `GRID_SWIM_REQUIRE_KEY` | Hold all SWIM traffic until the `GridNetwork` key loads or the network declares none. Defaults to `true`. With `false`, SWIM is plaintext while no network exists |
| `GRID_SWIM_SITE_NAME` | Unique site identity for this operator instance |
| `GRID_SWIM_SEEDS` | Comma-separated SWIM seed endpoints; accepts `ip:port`, `[ipv6]:port`, or `hostname:port` |
| `GRID_SIGNALS_ADDR` | Signals listener address. Defaults to `[::]:9091`, or `0.0.0.0:9091` on a host without IPv6 |
| `GRID_SIGNALS_ADVERTISE_ADDR` | Signals endpoint gossiped to peers, as `ip:port`, `[ipv6]:port`, or `hostname:port`. Without it, a site advertising the SWIM LoadBalancer gossips that address on the Service port named `signals`, and any other site gossips none. A LoadBalancer site needs one of the two, and the Service port needs a LoadBalancer that serves UDP and TCP on one Service |
| `GRID_SIGNALS_PEER_PORT` | Port dialed on a peer's SWIM host when the peer gossips no signals endpoint. Defaults to `9091` |
| `GRID_SIGNALS_MAX_PER_PEER` | Concurrent authenticated signals connections one peer site may hold. Defaults to `8` |
| `GRID_GATEWAY_ADDRESS` | Explicit gateway address override (skips self-discovery) |
| `GRID_GATEWAY_SERVICE_NAME` | Service name for gateway self-discovery (default: `provider-gateway`) |
| `GRID_GATEWAY_NAMESPACE` | Namespace for gateway Service lookup (default: `grid-system`) |
| `GRID_GATEWAY_PORT` | Port appended to discovered address (default: `8080`) |
| `GRID_GATEWAY_DISCOVERY_INTERVAL_MS` | Polling interval for gateway discovery (default: `5000`) |
| `GRID_METRICS_ADDR` | Address serving `/metrics`, `/healthz`, and `/readyz` (default: `0.0.0.0:9090`) |
| `GRID_SIGNALS_LOCAL_ADDR` | This site's own signals endpoint, passed to its gateway in the serving config. Blank leaves it unset |
| `GRID_ENROLL_ENABLED` | Enroll on startup when the site identity Secret is absent |
| `GRID_ENROLL_URL` | Enrollment service base URL, which must be https |
| `GRID_ENROLL_CA_FILE` | PEM bundle pinning the enrollment server |
| `GRID_ENROLL_GRID_CA_FILE` | Grid CA anchors the returned CA must match. Defaults to `GRID_ENROLL_CA_FILE` |
| `GRID_ENROLL_SITE_NAME` | Site name the token pins |
| `GRID_ENROLL_TOKEN_SECRET` | Secret in the operator namespace holding the site token |
| `GRID_ENROLL_TOKEN_SECRET_KEY` | Key of the token within that Secret (default: `token`) |
| `GRID_DEV_SELF_SIGNED_CA` | Mint a self-signed grid CA and site certificate when both referenced Secrets are absent and in the operator namespace. Dev only (default: `false`, chart value `devSelfSignedCa`) |
| `GRID_CONSUMER_LISTENER_PORT` | Consumer config listener port when the gateway Service cannot be read (default: `8080`, chart value `consumer.listenerPort`) |
| `GRID_CONSUMER_CREDENTIAL_MOUNT_BASE` | Directory the consumer pod mounts credential Secrets under (default: `/run/secrets/grid-credentials`, chart value `consumer.credentialMountBase`) |
| `GRID_CONSUMER_TLS_CERT_MOUNT_PATH` | Directory the consumer pod mounts its grid TLS Secret at (default: `/etc/praxis/tls`, chart value `consumer.tlsCertMountPath`) |

The signals listener and peer poller run only when `signalTransport` is `poll`.
The operator reads `signalTransport` and `peerTrust` once at startup, so a
change needs an operator restart.

The signals listener caps handshakes per client source address. Behind a
LoadBalancer, set `externalTrafficPolicy: Local` on the Service that carries
the signals port. With `Cluster`, node SNAT gives many clients one source
address, and they share its cap.

`GRID_SWIM_ENCRYPT_KEY` is intentionally omitted from the
`Deployment`.  Production SWIM encryption uses
`GridNetwork.spec.tls.swimKeyRef` to reference a
Kubernetes `Secret`.  The env var exists for local
development and testing only.

### Validate the install

```console
cargo xtask env verify-operator-install-rbac \
  -c tests/env/operator-routing.toml
```

This command builds the operator image, loads it into a
Kind cluster, applies the install manifests, runs positive
and negative `kubectl auth can-i` checks (including
namespace-scope proofs), then waits for the
in-cluster `Deployment` to reconcile a test `GridNetwork`
using only the installed `ServiceAccount`.

## 2. Create a GridNetwork

```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: GridNetwork
metadata:
  name: production
spec:
  siteDiscovery:
    mode: auto
  seeds:
    - "10.0.0.5:7946"
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
  tls:
    caSecretRef:
      name: grid-ca
      namespace: praxis-system
    siteSecretRef:
      name: grid-site-cert
      namespace: praxis-system
```

The GridNetwork controller:
1. Reads both Secrets. If either is missing and step 2 does not apply, it
   reports `Ready=False` with reason `TrustMaterialMissing` and a Warning event,
   and generates nothing.
2. Only when the operator runs with `GRID_DEV_SELF_SIGNED_CA=true` (chart value
   `devSelfSignedCa`), both Secrets are absent, and both are in the operator
   namespace, it creates a self-signed dev CA and this site's certificate (DNS
   SAN `{site-name}.grid.internal`, dual EKU for mTLS). It never overwrites a
   `Secret` that exists.
3. Announces the seeds to the SWIM runtime
4. Sets `status.phase: Initializing`. With `caSecretRef` set and no seeds, a
   running SWIM runtime reports `Active` instead

### CRD-driven seeds

`spec.seeds` is **operator-consumed**: on every `GridNetwork` reconcile the
controller parses and resolves each entry independently, logs invalid or
temporarily unresolvable entries at `warn`, retains successful addresses,
removes the local advertise address to prevent self-announce noise,
deduplicates, and calls `SwimHandle::announce_seeds` to deliver the batch to
the running SWIM event loop. Re-announcing to already-connected peers is
idempotent — foca ignores redundant joins. If every non-empty configured seed
fails, the controller retains the last-known-good resolved set; if none exists,
SWIM remains active with a seedless bootstrap and a degradation warning.

Startup seeds from `GRID_SWIM_SEEDS` (env var) and CRD seeds are additive.
The env var seeds are applied once at startup; CRD seeds are applied on every
reconcile, so dynamically added addresses take effect without an operator restart.

**Runtime update contract**

| Change to `spec.seeds` | Effect |
|---|---|
| Seed added | Announced to SWIM on the next reconcile; join initiated |
| Seed removed | Logged; no active disconnect — SWIM failure detection ages the peer out naturally |
| Seeds unchanged | Re-announced idempotently; no side effects |

Adding a seed requires no operator restart.  The new address is SWIM-joined within
one reconcile cycle. The requeue interval is 300 s by default, 60 s when a provider
uses metrics TLS, or `spec.metricsRefreshInterval` when set (capped at 60 s with
metrics TLS). A watch event triggers one sooner.

Removing a seed does not disconnect the peer.  The removed peer remains in SWIM
membership until it stops responding to probes and is declared `Suspect` then
`Dead` by the SWIM protocol.

**Global-runtime semantics**

The operator supports one `GridNetwork` per cluster. If more than one exists at
startup, it logs an error and exits. The SWIM runtime is process-global, with
one UDP listener per operator process. Seeds in `GridNetwork.spec.seeds` are
announced to that membership node. This is site-membership bootstrap, not
per-network membership isolation.
CRDT provider records remain network-scoped separately.

Each membership identity carries a restart-scale `u64` generation. A
replacement process at the same advertised address presents a greater
generation and supersedes the retained process identity; in-process renewals
increment it without wrapping. All peers in one SWIM membership domain must
run a wire-compatible identity schema. A release that changes that schema
must be coordinated across the membership domain rather than deployed as an
unqualified rolling update.

**Transport-security contract**

SWIM is the AGN control-plane membership and state broadcast channel. When
`spec.tls.swimKeyRef` is configured and the referenced Secret resolves to a
valid 32-byte key, reconcile applies the key before announcing CRD seeds or
publishing provider state. From that point, outgoing SWIM UDP
packets are encrypted and authenticated with AES-256-GCM.  Incoming packets
that fail authentication are silently dropped; the foca membership state
machine never sees them.

When `swimKeyRef` is absent, SWIM traffic is sent and received as cleartext
(backward-compatible local and development behavior).

If `swimKeyRef` is configured but the Secret is missing, unreadable, or not a
valid 32-byte key, the reconcile fails before CRD seed announcement and
provider broadcasts for that `GridNetwork`.  The SWIM runtime is
process-global, so a previously loaded key remains active until restart; the
operator does not switch to plaintext for that configured reconcile.

`GRID_SWIM_ENCRYPT_KEY` is the local and Kind validation path for startup-time
enforcement because it is available before the UDP socket starts.  It is
process environment material and should not be treated as the production Secret
delivery mechanism. With CRD-backed `swimKeyRef` and no environment key, the
operator reads the key from the declared Secret at startup and holds SWIM
traffic until it loads.

**SWIM encryption protects:** gossip membership packets, gateway address
broadcasts, public certificate PEM broadcasts, and CRDT provider state broadcasts.

**SWIM encryption does not protect:** data-plane request traffic.  Gateway
request-time authentication and authorization are enforced by Praxis/Praxis AI
gateway TLS and peer identity filters, not by SWIM membership.

Routing eligibility remains fail-closed at the `GridSite` layer independently of
SWIM encryption: remote CRDT provider records are rendered only for peers whose
`GridSite` is `Active`. Active indicates control-plane eligibility — sufficient
trust information to include the site in routing overlays. Data-plane readiness
is verified separately. Both layers are required for production deployments.

**Channel-full retry**

If the seed announce channel is full (capacity 16 batches), the announce is
skipped for the current reconcile and retried on the next
(300 s by default, as above). Seeds are not guaranteed to be applied
immediately under heavy broadcast load.

**Seed format**
Seeds accept `IP:port`, `[IPv6]:port`, and `hostname:port`, for example
`10.0.0.2:7946` or `grid-swim.example.internal:7946`. DNS is resolved during
each reconciliation with per-endpoint and aggregate bounds; results are sorted
and deduplicated. Invalid or failed entries are skipped with source and
endpoint diagnostics. A partial result is announced, while a wholly failed
non-empty list does not replace a working last-known-good set.

**Troubleshooting seed changes**

*New seed not joining:*
- Verify the address is a valid `IP:port`, `[IPv6]:port`, or `hostname:port`.
- Check the operator log for `new CRD seeds added` at `info` or
  `announcing CRD seeds to SWIM runtime` at `debug`. If both are absent, the
  reconcile may not have fired yet.
- Check for `failed to queue CRD seeds for SWIM announcement` at `warn` level,
  indicating a channel-full retry.
- Verify the remote operator is running with `GRID_SWIM_BIND_ADDR` set to the
  expected address.

*Removed seed still shows as connected:*
- Expected behavior.  SWIM does not actively disconnect on seed removal.
- Wait for the WAN probe and suspicion window. The runtime probes every five
  seconds, allows three seconds for a direct response before indirect probes,
  and retains a suspect member for 30 seconds before
  declaring it `Dead`. `GridNetwork.status.connectedSites` then decreases.
- If the remote operator is still running, it will rejoin as `Alive` again because
  SWIM membership is peer-to-peer and periodically announces to live and down
  members. Seeds bootstrap discovery; they are not an allowlist.

**Phase progression:** `GridNetwork Active` is set when
the SWIM runtime reports at least one `Alive` peer in
its `MembershipSnapshot`.  `Degraded` is set when peers
are known but all are `Suspect` or `Dead`, or when the SWIM runtime has
stopped.
`connectedSites` reflects the live SWIM `Alive` peer
count; `distributedProviderCount` reflects remote
`InferenceProvider` records received via SWIM CRDT
broadcast.

Both fields are `0` and the phase remains `Pending` or
`Initializing` when SWIM is disabled (i.e. the operator
is started without `GRID_SWIM_BIND_ADDR`).

## 3. Sites Discover Each Other

Each site takes its `gridId` from `spec.gridId`, then `status.gridId`, and
otherwise generates a UUIDv4. Sites do not negotiate it. Set the same
`spec.gridId` on every site, or each site generates its own.

Each site has one `GridSite`, named by its bare site name. When SWIM
reports a peer Alive, the `GridNetwork` controller fills that peer's `GridSite`
`status.discovered` and moves it from `Pending` to `Discovered`. It never
writes the spec of an existing `GridSite`. `spec.siteDiscovery.mode` decides
what happens when no `GridSite` exists for the peer:

- `manual`, the default: the operator creates none. Declare each peer, for
  example with `peers` in the grid-site chart.
- `auto`: the operator creates one with only `gridNetworkRef` and the label
  `grid.praxis.fast/auto-discovered: "true"`, up to 256 per network. Only one
  auto-mode `GridNetwork` runs discovery per operator.

A `GridSite` without `spec.egress` probes with `siteDiscovery.defaultEgressTls`,
`mutualTls` by default. Members the operator cannot adopt are reported on the
`GridNetwork` `DiscoveryConflict` condition, never as a spec rejection:

| Reason | Meaning |
|---|---|
| `SiteIdInvalid` | The member's site ID is not a DNS-1123 label |
| `SiteIdDuplicated` | Two live addresses claim the site ID. Its gossiped egress address is withdrawn |
| `FieldConflict` | A `GridSite` of that name belongs to another network |
| `StaleAutoDiscoveredEgress` | An auto-created `GridSite` carries `spec.egress`, which overrides gossip |
| `AutoDiscoveryCapReached` | The 256 auto-created `GridSites` limit is reached |
| `UnadoptableMembers` | The conflicts have more than one reason. The message lists each |

A second auto-mode `GridNetwork` reports `Accepted=False` with reason
`AutoDiscoveryClaimed`.

In `auto` mode the operator records `status.discovered.absentSince` when gossip
loses a member and clears it when the member returns. When
`spec.staleCandidateTtlSeconds` is set, it deletes an auto-created `GridSite`
absent longer than that whose spec nobody edited, under a uid and
resourceVersion precondition. Unset, it deletes none.

`GridSite` status: `phase: Discovered`

## 4. Gateway Address and Trust Bootstrap

SWIM discovery proves that a peer is participating in gossip.  It does not
authorize that peer for request routing.

### SWIM bootstrap phases

The trust bootstrap for a remote site progresses through these steps:

1. **SWIM discovery** — the peer is observed as Alive in SWIM membership.
   Phase: `Discovered`.  No trust established.

2. **Gateway address known** — the remote operator advertises its resolved gateway
   address via SWIM state broadcast.  The address is resolved by the self-discovery
   poller (Service LoadBalancer lookup) or from the `GRID_GATEWAY_ADDRESS` override.
   The local operator stores it in `GridSite.status.discovered.egressAddress`
   and probes it only when it is a literal, dialable `IP:port`. A hostname, or
   an address such as loopback or link-local, needs `spec.egress.address`
   instead. Phase: `Connecting`. No trust established.

3. **Identity policy configured**: set `spec.egress.tls.serverName` to the
   expected DNS SAN, or leave it to default to `<site>.grid.internal`, the
   name enrollment issues. Under the default `pin` peer trust, set
   `spec.trust.canonicalFingerprints` to one or two independently verified
   DER-certificate SHA-256 pins. Configure `GridNetwork.spec.tls.caSecretRef`
   and `siteSecretRef` for server and client authentication. Under
   `GridNetwork.spec.peerTrust.mode: spiffe`, the probe verifies the peer's
   exact SPIFFE ID instead, and a `GridSite` that sets pins reports
   `Accepted=False` with reason `TrustConflictsWithPeerTrust`. The provider
   gateway's `gatewayConfig.peerTrust.mode` must match the network.

4. **Identity-aware gateway probe passes**: the `GridSite` controller performs
   a bounded mTLS handshake. It verifies the CA chain, DNS SAN, client
   authentication, and the canonical pin or SPIFFE ID against the live leaf
   certificate. Success promotes the site to `Active` with reason `TlsVerified`.

   ```yaml
   spec:
     egress:
       address: provider.example.com:8443
       tls:
         mode: mutualTls
         serverName: provider.example.com
     trust:
       canonicalFingerprints:
         - "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
   ```

   See [Authentication and Access Policy](auth.md) for the trust contract.

5. **Data-plane mTLS enforced**: a provider Praxis gateway validates peer
   identity over mTLS on every request, independent of the control-plane
   phase. Deployment acceptance requires positive and negative runtime probes;
   manifest inspection alone is not evidence.

### Authentication vs authorization

| Concept | Question it answers | AGN mechanism |
|---|---|---|
| Authentication | "Is this peer really the site it claims to be?" | Identity-aware AGN health probe plus gateway mTLS peer validation on each request |
| Authorization | "Is this authenticated peer allowed to participate in this AI Grid Network or receive/send this traffic?" | Local AGN policy plus destination gateway enforcement |

A peer must satisfy both.  A SWIM peer must never become routable solely because
it gossiped successfully.

### Security rules

- SWIM membership is discovery, not authorization.
- TCP reachability proves an address accepts connections, not identity.
- Gossip carries no certificate. Identity comes only from the live handshake.
- Private keys, credential tokens, and Secret data must never be written to
  `GridSite` status, `GridNetwork` status, overlays, generated ConfigMaps, or logs.
- The operator does not copy Kubernetes Secrets across clusters as part of site discovery.
- The provider gateway still enforces peer identity on every request with mTLS.

### Routing eligibility

`GridSite.status.phase == Active` is the control-plane eligibility gate for remote CRDT provider records.
Provider records advertised by a SWIM peer are included in the routing overlay only when
the corresponding `GridSite` is `Active`.  Peers in `Discovered`, `Connecting`, or any
other phase are excluded.  Peers with no matching `GridSite` are also excluded (fail-closed).

GridSite Active is a control-plane eligibility signal. It proves the configured
gateway health probe succeeded. It does not prove that Praxis loaded the latest
routing config or authorized a particular request.

Setting `Active` in Mutual mode requires the configured CA, client identity,
server name, canonical pin or SPIFFE ID, and live gateway certificate to agree. A provider
gateway independently authorizes peer identity on every data-plane request.
`Active` alone is not evidence that request authorization succeeded.

## 5. Connectivity Verification

The `GridSite` controller verifies gateway reachability and identity against
`spec.egress.address`, or `status.discovered.egressAddress` when the spec sets none.

The `GridSite` controller reports the reason of its last probe on the
`Connected` condition, and on `Ready` while the site is not `Active`.

| Reason | Current check |
|--------|---------------|
| `AwaitingDiscovery` | The site is `Pending` until SWIM reports the peer Alive and the `GridNetwork` controller moves it to `Discovered` |
| `GatewayAddressMissing` | No egress address yet, so the site stays `Discovered` |
| `GossipedAddressRefused` | The only address is a gossiped one that is not a dialable literal `IP:port`, so the site stays `Discovered`. Set `spec.egress.address` |
| `GatewayAddressKnown` | `spec.egress.address` or a dialable `status.discovered.egressAddress` is non-empty, and the site moves to `Connecting` |
| `TlsVerified` | Mutual TLS handshake, chain, SAN, and live-leaf pin all verify |
| `PlaintextIneligible` | Egress is plaintext, so `Connected=False` whether or not TCP connects. Set by `spec.egress.tls.mode` or the GridNetwork `siteDiscovery.defaultEgressTls` (default `mutualTls`) |

Request-time authorization remains enforced by the provider gateway after the
control-plane health evaluation succeeds.

## 6. Capability Negotiation

Sites publish capability and provider state through AGN control-plane records
and CRDT-over-SWIM propagation.  Capability information can include models,
tools, agents, and provider availability signals.

The `GridSite` status `capabilities` field records broad site capability
classes.  A site should only be treated as fully usable after discovery,
gateway reachability, trust establishment, and data-plane readiness are all
satisfied.

## 7. Register Providers

Users create provider resources.
See the [CRDs doc](crds.md) for full specs.

Example — an API provider:
```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: InferenceProvider
metadata:
  name: anthropic-api
spec:
  gridNetworkRef: production
  hostSelector: {}
  providerKind: anthropic
  backendKind: apiProvider
  endpoint: https://api.anthropic.com
  models:
    - name: claude-sonnet-4
  auth:
    strategy: bearerToken
    secretRef:
      name: anthropic-token
      namespace: praxis-system
      key: token
  accessPolicy:
    siteSelector: {}
```

Example — a local llm-d cluster:
```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: InferenceProvider
metadata:
  name: local-vllm
spec:
  gridNetworkRef: production
  hostSelector: {}
  providerKind: openAi
  backendKind: local
  endpoint: http://vllm-service.inference:8000
  models:
    - name: llama-3.2-8b
```

## 8. Routing Configuration

The `GridNetwork` controller renders routing overlay
`ConfigMap`s from CRD data. For each `gatewayRef` in the
`GridNetwork`, it server-side applies a `ConfigMap`
named `grid-overlay-{network}-{gateway}` containing:

- **`routing-overlay.json`**: the versioned, content-addressed envelope consumed
  by Praxis AI. It includes scope, provenance, revision, digest, and the routing
  payload.
- **`routing-config.json`**: JSON-serialised
  legacy `RoutingOverlay` payload with one `RoutingCandidate` per
  model per `InferenceProvider` in the network.  When
  `spec.auth.secretRef` is set, candidates carry only
  the credential reference, never token bytes.

The overlay shape is compatible with the Praxis
`intelligent_route` filter:

```json
{
  "network": "production",
  "local_site": "production",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "claude-sonnet-4",
      "site": "anthropic-api",
      "cluster": "anthropic-api",
      "fresh": true,
      "credential": {
        "strategy": "bearer_token",
        "secretRef": {
          "name": "anthropic-token",
          "namespace": "praxis-system",
          "key": "token"
        }
      }
    }
  ]
}
```

**Cluster naming:** `candidate.cluster` uses
`spec.routingClusterRef` when set, otherwise the
`InferenceProvider` metadata name.  The Praxis
`load_balancer` cluster serving that provider must use
the same identity.

Local development with `xtask env` maps overlay site
identities to generated `gateway-{site}` load-balancer
entries; see `xtask/src/env/operator_overlay.rs`.

### Routing overlay delivery

For gateways that must react promptly to provider health, capacity, or score
changes, enable the overlay-sync delivery path in the `praxis-gateway` chart.

```text
operator reconcile
  -> overlay ConfigMap applied
  -> overlay-sync Kubernetes API watch
  -> envelope validated
  -> atomic write to shared emptyDir
  -> Praxis file watcher hot-reloads
```

A direct ConfigMap volume is eventually refreshed by the kubelet. That is
adequate for static or slowly changing configuration, but its refresh delay is
not appropriate as the delivery mechanism for short-lived routing changes.
The sidecar watches the API directly, so a published revision does not wait for
the kubelet's projected-volume polling cycle.

The sidecar adds correctness controls as well as lower latency:

- an init container blocks Praxis startup until the first valid overlay exists;
- schema version, destination scope, content digest, and maximum size are
  checked before publication;
- a temporary file plus `fsync` and rename prevents partial reads;
- invalid updates, source deletion, and temporary API loss retain the
  last-known-good file;
- readiness, degraded state, accepted/rejected counters, write counters, and
  timestamps make the delivery boundary observable; and
- a dedicated ServiceAccount token is mounted only into the init and sidecar
  containers. Praxis has no Kubernetes API access.

The sidecar does not change the metrics scrape interval or force the AGN
operator to reconcile. End-to-end convergence is still:

```text
metrics become visible
  + operator scrape/reconcile
  + ConfigMap apply
  + sidecar API-watch delivery
  + Praxis file-watch reload
```

The sidecar is off by default. Set `overlay.enabled=true` and
`overlay.sidecar.enabled=true` to use it. With `overlay.sidecar.enabled=false`,
the chart projects the ConfigMap directly. Do not use that mode when a demo or
production SLO assumes prompt metrics-driven route changes.

`grid-gateway` reads the file named by `GRID_SERVING_CONFIG` once at start, so
a changed serving config takes effect after a pod restart. The
`grid.praxis.fast/serving-digest` annotation on the `grid-serving-*`
ConfigMap changes when the rendered content does.

## 9. Workloads Consume Providers

Workloads send requests to the Praxis Gateway.
The gateway's grid scoring filter selects the optimal
backend. Praxis AI handles API translation and credential
injection transparently.

For API-provider routes, the request-time path is:

```text
intelligent_route
  -> writes intelligent_route.credential.* metadata from the selected candidate
credential_inject
  -> reads the matching token from a mounted Secret file
  -> injects Authorization: Bearer <token>
load_balancer
  -> forwards to the selected provider cluster
```

The token is not stored in the AGN overlay or consumer
Praxis `ConfigMap`.

For direct API-provider and cloud-provider fallback, the
consumer gateway is often also the final-hop gateway, so the
credential Secret is mounted there. For remote AGN sites,
provider credentials should live only in the remote provider
site or provider-side component that makes the final backend
call. AGN carries the reference needed for routing and
configuration; it does not copy Secret values between
clusters.

The native path requires Praxis AI v0.4.1 or later, which includes the
`credential_inject` filter. The `grid-gateway` image links v0.4.1.

See [Auth & Policy](auth.md) for workload access
patterns and authentication strategies.

## GridSite trust bootstrap

### Peer identity

Gossip carries no certificate. Under `pin` peer trust, take the peer's pin from
the peer itself, through a channel you trust. On the peer cluster, the pin is
the SHA-256 of its site certificate's DER bytes:

```console
kubectl -n <namespace> get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' \
  | base64 -d | openssl x509 -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1
```

To advance a Mutual TLS site to `Active`, configure
the CA and local client identity on the `GridNetwork`, then configure
`canonicalFingerprints`, and `serverName` when it is not `<site>.grid.internal`,
on the `GridSite`. Under `peerTrust.mode: spiffe` the probe verifies the peer's
SPIFFE ID and the `GridSite` sets no pins. The gateway `peerTrust.mode` must
match the network.

A site in `TrustMaterialMissing` lacks at least one required CA, client
certificate, client key, or, under `pin` peer trust, canonical pin.

### Security rules

SWIM broadcasts carry no certificates and no keys. The provider gateway enforces
mTLS peer identity and certificate validation on every request.

## GridSite gateway address configuration

The operator resolves and advertises its data-plane gateway address to SWIM
peers. This address is propagated through SWIM state broadcasts and used by
receiving operators to populate `GridSite.status.discovered.egressAddress` on
the peer's `GridSite`.

**Self-discovery (default):** A background poller periodically looks up the
`provider-gateway` LoadBalancer Service and extracts its external IP.  The
poller runs only when the SWIM runtime does. It retries every 5 seconds
(configurable via `GRID_GATEWAY_DISCOVERY_INTERVAL_MS`) until the address appears, then
continues watching for changes.  Discovered addresses are pushed to the SWIM
runtime via a watch channel.

**Explicit override:** Set `GRID_GATEWAY_ADDRESS` to skip the self-discovery
poller entirely.

```bash
# Self-discovery (default): operator discovers from provider-gateway Service
GRID_SWIM_BIND_ADDR=10.0.0.4:7946 GRID_GATEWAY_SERVICE_NAME=provider-gateway ./operator

# Explicit override: skip discovery poller
GRID_SWIM_BIND_ADDR=10.0.0.4:7946 GRID_GATEWAY_ADDRESS=10.0.0.4:8080 ./operator
```

The binary looks for the Service in namespace `grid-system`. The Helm chart
uses the release namespace instead, and with enrollment it sets the Service
name to `grid-gateway`.

**Requirements:**
- Format: `host:port` or `IP:port`. The remote operator stores it verbatim in
  `GridSite.status.discovered.egressAddress`, but probes it only when it is a
  literal, dialable `IP:port`. Otherwise the site stays `Discovered` with reason
  `GossipedAddressRefused` until `spec.egress.address` is set
- When absent or empty and no LoadBalancer Service exists: peer `GridSite`
  records have no egress address and stay in `Discovered`
  phase until the Service appears
- When the Service is missing or has no LoadBalancer address, including right
  after an operator restart, the operator gossips an empty address once per
  change, and peers clear `status.discovered.egressAddress` instead of probing
  an old one
- This address is separate from `GRID_SWIM_BIND_ADDR` — the SWIM gossip endpoint
  and the data-plane gateway address are distinct. The signals endpoint is a
  third address. It is gossiped from the SWIM LoadBalancer Service
  (`grid-operator-swim`) unless `GRID_SIGNALS_ADVERTISE_ADDR` is set

Discovery takes the first LoadBalancer ingress entry's IP, or its hostname when
it has no IP, and appends `GRID_GATEWAY_PORT`. A hostname ingress therefore
needs `spec.egress.address` on each peer's `GridSite`.

**Probe behavior:** In Mutual mode, the `GridSite` controller performs a bounded
mTLS connection to the egress address. It verifies the configured CA,
`serverName`, and the canonical live-certificate pin, or the SPIFFE ID under
`spiffe` peer trust. A successful probe reports reason `TlsVerified` on the
`Connected` condition. Connection failures move an Active site to `Unreachable`.
Identity or trust failures move it to `Connecting`.

A pin mismatch on the live leaf is `PinMismatch` and demotes the site.

Explicit `plaintext` mode performs only a bounded TCP connection for
diagnostics. It reports reason `PlaintextIneligible` with `Connected=False`,
never promotes a site to `Active`, and is never selected as a fallback when
Mutual TLS configuration is incomplete or invalid.

## GridSite Lifecycle Diagnostics

Use `kubectl get gridsites` to inspect current lifecycle phases:

```console
kubectl get gridsites
```

Example output:

```
NAME                              PHASE        NETWORK
grid-site-b       Connecting   op-e2e-sjd-net
```

To see the reason and diagnostic message:

```console
kubectl get gridsite <name> -o jsonpath='{.status.phase}/{.status.conditions[?(@.type=="Connected")].reason}: {.status.conditions[?(@.type=="Connected")].message}'
```

### Phase transitions and their cause

| From | To | Trigger |
|---|---|---|
| (new) | Pending | Resource created |
| Pending | Discovered | `GridNetwork` controller observes SWIM Alive member |
| Discovered | Connecting | `GridSite` controller: egress address known |
| Connecting | Active | `GridSite` controller: configured Mutual TLS identity probe succeeds |
| Active | Connecting | TLS identity or trust verification fails, a TLS handshake times out, or the endpoint is changed to plaintext and its TCP probe succeeds |
| Active | Unreachable | Gateway address is missing, the TCP connect times out or fails, or the plaintext TCP probe fails |
| Unreachable | Active | The configured identity probe succeeds |
| Unreachable | Connecting | Trust verification fails, as for Active to Connecting |
| Active or Unreachable | Connecting | The spec is rejected (`Accepted=False`), for example `TrustConflictsWithPeerTrust` or `ServerNameInvalid` |

Security invariant: a SWIM peer must never become routable solely because it
gossiped successfully.  Discovery, authentication, and authorization are
separate steps.

### Troubleshooting

**Phase stays Pending after SWIM convergence**

- Check the `GridNetwork` sets `spec.siteDiscovery.mode: auto`, or declare the `GridSite` under the bare site name.
- Check that the `GridNetwork` controller has SWIM running (`GRID_SWIM_BIND_ADDR` env var set).
- Check `kubectl get gridnetwork <name> -o jsonpath='{.status.connectedSites}'` — must be > 0.

**Phase stays Discovered (not advancing to Connecting)**

- The site has no egress address in spec or `status.discovered`.  Verify the remote operator's
  `provider-gateway` LoadBalancer Service exists and has an external IP assigned,
  or set `GRID_GATEWAY_ADDRESS` as an explicit override.  The self-discovery poller
  will propagate the address through SWIM once discovered.
- Reason will be `GatewayAddressMissing`. With reason `GossipedAddressRefused`,
  the gossiped address is a hostname or not dialable. Set `spec.egress.address`.

**Phase stays Connecting**

- Check the `Connected` condition reason:
  - `TrustMaterialMissing`: configure the CA Secret, local client identity,
    and, under `pin` peer trust, canonical pin policy.
  - `PlaintextIneligible`: the egress mode is `plaintext`, which never routes.
    Set `spec.egress.tls.mode` or `siteDiscovery.defaultEgressTls` to
    `mutualTls`.
  - `TrustConflictsWithPeerTrust`, `ServerNameForbidden`, or
    `ServerNameInvalid`: the spec is rejected and `Accepted` is `False`. Fix the
    spec. A rejected site is not probed.
  - `TrustMaterialInvalid`: trust material is malformed or oversized.
  - `UntrustedIssuer`, `IdentityMismatch`, `CertificateExpired`, or
    `CertificateNotYetValid`: inspect the live gateway certificate and
    configured CA/server name.
  - `PinMismatch`: the live leaf certificate does not match either configured
    canonical pin.
  - `HandshakeTimeout` or `TlsProtocolError`: the TCP endpoint answered but did
    not complete the expected TLS protocol.

**Phase is Active, site became Unreachable**

- The connection to the egress address failed. When connectivity returns,
  the complete configured identity probe must pass before the site returns to
  Active.

**RBAC for GridSite status updates**

The `GridSite` and `GridNetwork` controllers both write to `GridSite` status.
The `grid-operator-crd` `ClusterRole` in `deploy/operator/cluster-role-crd.yaml`
(`<fullname>-crd` in the Helm chart) includes `gridsites/status` with verbs
`get` and `patch`.

## Consumer Config

When `GatewayRef.consumerConfig.enabled: true`, the AGN Operator applies a
`ConfigMap` in the gateway's namespace on every reconcile.  The
`grid-operator-resources` `ClusterRole` includes `configmaps` with verbs
`get`, `create`, `patch`, and `update`. A `RoleBinding` in the gateway's
namespace is required for the operator `ServiceAccount` to write the `ConfigMap`
there. The Helm chart creates one for each entry in `resourceNamespaces`.

Every `clusterEndpoints[]` entry must declare explicit transport intent via the
`transport` field.  Remote/provider-gateway clusters should use
`transport.mode: mutualTls` with a non-blank `transport.sni` matching the
provider certificate SAN.  Local dev-only clusters may use
`transport.mode: plaintext`.  Missing transport fails closed with status reason
`MissingTransport`; missing SNI on `mutualTls` fails with `MissingSni`.
`transport.mode` is the security switch — not the presence of `sni`.

### Credential Secret access

The generated `ConfigMap` references credential Secrets by name, namespace, and
key and does not contain Secret values. Provider admission reads each
credential Secret to check that it exists, the key is present, and the value is
UTF-8. The operator therefore needs `get` on it through a `RoleBinding` in the
Secret's namespace.

The final-hop gateway or provider-side component making the final backend call
needs the credential Secret mounted.  Secret provisioning in that cluster is
the responsibility of external tooling (platform automation, External Secrets,
Vault, or a manual process). The AGN Operator does not copy Secrets across
clusters.

### Cross-cluster limitations

The operator's RBAC controls access within its own cluster.  When the consumer
gateway runs in a different cluster, the generated `ConfigMap` must be delivered
externally — the operator cannot write to a remote cluster's API server directly.
The Kind validation harness (`verify-api-fallback-native`) bridges this gap for
local testing by reading the generated YAML and re-applying it as
`praxis-consumer-config` in the consumer cluster.  Production cross-cluster
delivery requires GitOps, External Secrets, or a similar mechanism.

## Site Departure

The running SWIM implementation detects an abrupt loss through direct and
indirect probes. Remote provider records are treated as degraded when the peer
is `Suspect` or `Dead`, and stale-candidate retention follows the configured
overlay TTL policy.

The current operator does not garbage-collect the CRDT record or complete a
`Left` transition on process shutdown. It deletes a `GridSite` only when
`siteDiscovery.mode` is `auto`, the operator created it, nobody edited its
spec, and the member stayed absent past `spec.staleCandidateTtlSeconds`.
Departure otherwise preserves control-plane evidence and requires explicit
site lifecycle cleanup by the deployment owner.

## Adding a New Site to an Existing AI Grid Network

1. Deploy the AGN Operator on the new cluster
2. Create a `GridNetwork` with any existing cluster as a seed, and either
   declare a `GridSite` for each peer or set `spec.siteDiscovery.mode: auto`
3. SWIM discovers the existing cluster, which shares
   the membership list of all other sites
4. The new site automatically discovers all grid
   members within seconds
5. Under `pin` peer trust, an auto-discovered `GridSite` has no pins, so it stops at
   `TrustMaterialMissing`. Set `spec.trust.canonicalFingerprints` on it, or
   declare the peer before it joins with `peers.<name>.address` and `digest`
   in the grid-site chart. The operator then verifies the fingerprint and
   advances matching sites to `Active`
6. Once `Active`, the new site's providers are visible
   to all other sites through the routing overlay

## External Ingress Operations

External ingress operates as a two-stage service:

```text
managed GTM -> Praxis AI edge -> AGN-selected Praxis provider gateway
```

The production deployment inventory contains:

| Layer | Operational inventory |
|---|---|
| GTM | Public service name, public TLS ownership, edge origins, health probes, steering policy, drain/failback policy, DDoS/WAF controls. |
| Edge | At least two failure domains, pinned AGN/AI/Praxis compatibility set, caller authentication, tenant/model authorization, request limits, accepted overlay status. |
| AGN | One edge-specific `GatewayRef` and routing perspective per edge location, authenticated site state, bounded admission policy, versioned overlay revisions. |
| Provider | Private backend, provider Praxis gateway, mTLS listener, trusted edge identities, local authorization, local limits, and final-hop credentials. |

### Edge Readiness

A production GTM integration uses a route-aware readiness signal. The edge is
ready when:

- its public listener and external authentication dependencies are healthy;
- a supported overlay version has been accepted;
- the accepted overlay age is within policy;
- endpoint/TLS configuration exists for every usable selected cluster; and
- the public offer has its configured minimum authorized route coverage.

One unavailable optional provider does not withdraw the edge. Loss of all
required routes, a hard-expired snapshot, or a failed security dependency does.
Liveness remains a process/listener signal and is not used as route coverage.

During drain, readiness is withdrawn before shutdown. New requests stop while
existing SSE streams receive the configured completion interval.

### Overlay Rollout Evidence

A production external-edge rollout tracks four distinct states:

```text
desired revision
  -> rendered ConfigMap revision
  -> distributed file revision
  -> Praxis accepted/serving revision
```

`GridNetwork.status.consumerConfigStatus` is a per-gateway list, and an entry
with `phase: Rendered` reports successful desired config rendering and apply.
It does not report that the gateway loaded the overlay. `status.overlayStatus`
already reports the rendered and distributed revisions for each gateway. The
production contract requires gateway status for the accepted
revision, digest, acceptance time, age, and last rejection reason. That
contract is not satisfied by compatibility profiles that expose only
`Rendered` status.

Operational checks compare the same revision across all four states and send a
request whose internal route-decision record contains that revision.

An invalid overlay is expected to:

1. fail strict parse or semantic validation;
2. leave the previous accepted snapshot serving;
3. increment a bounded rejection metric;
4. expose the rejection reason without content or secrets; and
5. affect readiness only according to accepted-snapshot age policy.

### Provider Trust Verification

Control-plane `GridSite Active` is necessary but not sufficient. The data-plane
verification uses the actual edge-to-provider path:

```text
known edge certificate -> accepted
unknown/revoked certificate -> rejected
wrong SNI/server identity -> rejected
direct public client -> rejected
authorized peer but denied provider policy -> rejected
```

The provider gateway's backend remains `ClusterIP`-only or otherwise private.
Customer `Authorization` values are absent from the provider request. Provider
credentials exist only at the final hop.

## Development Validation Environments

The `xtask env` commands provide a local development and
integration-validation path using Kind clusters. They are not the production
reconciliation model. The Kubernetes-native global-ingress scenario is
documented in
the [Praxis demos repository](https://github.com/praxis-proxy/demos).

This path is intended for:

- Local development iteration against a multi-cluster
  topology
- Integration validation before pushing to a real cluster
- CI pipelines that require a running kind environment

### What `xtask env` does

`xtask env` commands are imperative and config-driven.
They operate from `tests/env/config.toml` (or a supplied
`--config` path), which declares clusters, their roles,
and the models each provider cluster exposes.

Available commands:

| Command | What it does |
|---|---|
| `cargo xtask env up` | Creates kind clusters, deploys the configured provider backend, generates local test certificates |
| `cargo xtask env down` | Tears down kind clusters and removes generated certs |
| `cargo xtask env status` | Reports cluster, provider, and cert readiness |
| `cargo xtask env verify-providers` | Probes Chat Completions endpoints against the configured provider backend in all provider clusters |
| `cargo xtask env build-gateway-images` | Builds the Praxis AI gateway and mock EPP container images |
| `cargo xtask env load-gateway-images` | Loads locally-built images into kind cluster nodes |
| `cargo xtask env deploy-provider-gateways` | Applies generated Praxis AI gateway resources to provider clusters |
| `cargo xtask env verify-provider-gateways` | Runs end-to-end probes through the provider gateway request path |
| `cargo xtask env deploy-consumer-gateway` | Deploys a consumer Praxis AI gateway with a generated static `intelligent_route` config |
| `cargo xtask env deploy-consumer-gateway --overlay-config <path>` | Deploys the consumer gateway using a `routing-config.json` routing overlay file |
| `cargo xtask env verify-gateway-e2e` | Verifies consumer-to-provider routing end-to-end |
| `cargo xtask env verify-mtls-trust` | Verifies provider gateway mTLS enforcement (positive + negative cases) |
| `cargo xtask env verify-api-fallback-native` | Verifies native `intelligent_route` → `credential_inject` credential injection with token bytes absent from overlay and consumer ConfigMap |
| `cargo xtask env verify-stale-gc-ttl` | Verifies `GridNetwork.spec.staleCandidateTtlSeconds` evicts stale remote candidates from the rendered overlay |
| `cargo xtask env verify-responses-routing` | Verifies `/v1/responses` request parsing and AGN overlay routing using `openai_responses_format` -> `intelligent_route` filter chain |
| `cargo xtask env verify-crd-schema` | Verifies required generated CRD schema fields without requiring kind clusters |
| `cargo xtask env verify-swim-dns-hostnames` | Verifies hostname-based SWIM advertise/seeds, membership convergence, and CRDT provider-state propagation |
| `cargo xtask env verify-operator-install-rbac` | Applies install manifests, runs positive/negative RBAC checks, proves minimal reconcile succeeds |
| `cargo xtask env validate-all` | Runs the local validation suite and prints a Markdown result table |

### Operator and SWIM local validation

The operator is **not** running inside kind; it connects
to the kind cluster via the local kubeconfig.  SWIM
runtimes use localhost UDP sockets between local operator
processes.  This avoids requiring an operator container
image or in-cluster RBAC for local validation.

#### Setup (one-time per machine)

```console
cargo xtask env up -c tests/env/operator-routing.toml
cargo xtask env load-gateway-images -c tests/env/operator-routing.toml
```

Creates `grid-site-a` (provider, mock-openai backend)
and `grid-consumer` kind clusters, generates local mTLS
certificates, and loads Praxis AI gateway images.

#### CRD schema validation

```console
cargo xtask env verify-crd-schema
```

This command runs the CRD generator and verifies the
generated schema contains required AGN status and
InferenceProvider routing and metrics fields. It does
not require kind clusters.

#### Routing validation

```console
cargo xtask env validate-operator-routing -c tests/env/operator-routing.toml
```

This command deploys the Praxis provider gateway, spawns
the operator out of cluster, applies `GridNetwork` and
`InferenceProvider` fixtures, waits for reconciliation,
exports the operator overlay, deploys the consumer
gateway from that overlay, and sends live HTTP requests
through the consumer gateway.

The validation covers provider health classification,
candidate ordering, metrics-aware ordering,
`routingClusterRef` identity mapping, overlay export,
consumer gateway deployment, successful routing for a
known model, and clean failure for an unknown model.

#### SWIM membership

```console
cargo xtask env verify-swim-membership -c tests/env/operator-routing.toml
```

This command starts two out-of-cluster operator
processes with distinct localhost UDP ports. The
secondary seeds on the primary. After a convergence
window, the command applies a `GridNetwork` fixture and
polls `GridNetwork.status` for SWIM-derived membership
state.

#### CRDT-over-SWIM state

```console
cargo xtask env verify-swim-state -c tests/env/operator-routing.toml
```

To qualify hostname-based SWIM endpoints without relying on public DNS:

```console
GRID_SWIM_DNS_EVIDENCE_DIR=/tmp/grid-swim-dns-run-1 \
  cargo xtask env verify-swim-dns-hostnames -c tests/env/operator-routing.toml
```

The qualification uses two distinct loopback ports with `localhost:port`
advertise and seed endpoints, then proves membership and remote provider-state
propagation before cleaning its resources.

This command starts two SWIM-enabled operator processes,
waits for gossip convergence, then applies a
`GridNetwork` and an `InferenceProvider`. Each operator
maps the `InferenceProvider` CRD to a
`crdt::ProviderState` and publishes it as a
`StateBroadcast` over foca's custom-broadcast path. The
receiver merges the `GridStateSnapshot`, and subsequent
status reconciliation reflects remote provider state in
`GridNetwork.status.distributedProviderCount`.

**Provider fields propagated over SWIM:**

| CRDT field | Source |
|---|---|
| `network_id` | owning `GridNetwork.metadata.name` |
| `site_id` | local SWIM site identity |
| `provider_id` | `metadata.name` |
| `routing_cluster` | `spec.routingClusterRef` or `metadata.name` |
| `models` | `spec.models[*].name` |
| `backend_kind` | `spec.backendKind` |
| `capacity_weight` | `spec.capacityWeight` |
| `phase` | `status.phase` (including `Unavailable`) |
| `access_policy` | `spec.accessPolicy` |
| `metrics` | `metricsConfig` scrape results, or defaults |
| `revision` | `metadata.resourceVersion`, falling back to `metadata.generation` |
| `writer_id` | local SWIM site identity |

`distributedProviderCount` in `GridNetworkStatus`
reflects received remote provider records for the
current `GridNetwork`; local records and records from
other `GridNetwork`s are excluded. The local validation
fixture expects exactly one remote provider record; zero
means state did not arrive, and more than one indicates
cross-network leakage or stale test state.

#### Three-node SWIM mesh

```console
cargo xtask env verify-swim-mesh-three-node -c tests/env/operator-routing-multisite.toml
```

This command starts three SWIM-enabled operator processes in a linear topology:
node A (no seeds), node B (seeds A), and node C (seeds B only — not A).  It proves:

1. **Transitive discovery** — A learns about C through B.  After gossip convergence,
   `GridNetwork.status.distributedProviderCount >= 2` on A, confirming CRDT state from
   both B and C reached A transitively.

2. **Routing eligibility before Active** — C's CRDT provider is present in A's
   SWIM state but absent from A's routing overlay because C's `GridSite` is not yet
   `Active`.  Both B and C are excluded.

3. **Routing eligibility after Active** — After C's `GridSite` is set to `Active`
   (with a reachable egress address), A's overlay is re-rendered and C's provider
   candidate appears.

4. **Cross-network isolation** — A wrong-network `GridNetwork` and `InferenceProvider`
   are applied alongside the main network.  The wrong-network model is confirmed absent
   from A's correct-network overlay, proving providers cannot cross network scopes.

This validation proves that SWIM gossip alone is not sufficient for routing; explicit
`Active` phase assignment is required.

#### Full local validation suite

```console
cargo xtask env validate-all -c tests/env/operator-routing.toml
```

This command runs the local status check, operator
routing validation, SWIM membership validation,
CRDT-over-SWIM state validation, and mTLS trust
validation in sequence. It continues after individual
step failures and prints a Markdown summary table at the
end so CI logs and manual runs show the complete state
of the environment.

### Required local images

Before running `load-gateway-images`, the following
images must exist in the local container daemon:

| Image | Built from | Required for |
|---|---|---|
| `localhost/praxis-ai:llmd-ext-proc` | AI repository external checkout | All provider and consumer gateways |
| `localhost/praxis-ai-mock-epp:latest` | AI repository external checkout | All provider gateways |
| `grid-mock-providers:latest` | This repository, `mock-providers/Containerfile` | Provider clusters with `backend = "mock-openai"` only |

This table applies to the generic `xtask env` harness above
(`validate-all`, `verify-swim-mesh-three-node`,
`verify-failover-under-lost-peer`, etc.), which is the only
path that consumes these two locally-built defaults directly.
The named demos (`grid-glb-demo`, `grid-combined-site`,
`grid-llmd-pool-metrics`) do **not** need them — they override
`GRID_XTASK_GATEWAY_IMAGE`/`GRID_XTASK_MOCK_EPP_IMAGE`
(see `xtask/src/env/image_overrides.rs`) with published
`ghcr.io/praxis-proxy/ai` images and never build
from an AI repository checkout.

As of this writing, neither `Containerfile.composed` nor a mock
llm-d Endpoint Picker server implementation exists in the AI
repository (tracked in
[`ai#716`](https://github.com/praxis-proxy/ai/issues/716)), so
`build-gateway-images --ai-repo <path>` cannot currently produce
either of the first two images. That gap blocks only the
generic-harness path, concretely `verify-failover-under-lost-peer`
today, and not any of the three named demos above.
[`ai#334`](https://github.com/praxis-proxy/ai/pull/334) moved `ext_proc`
compatibility into the AI repository and merged on 2026-08-24.

Use `build-gateway-images --ai-repo <path>` to build the first two images from
the AI repository source tree. Build `grid-mock-providers:latest` separately
from this repository:

```bash
docker build -t grid-mock-providers:latest -f mock-providers/Containerfile .
```

### What `xtask env` does NOT do

The `xtask env up/down/status/deploy-*` commands are
not the production operator:

- They do not reconcile Kubernetes resources
  continuously
- They do not manage `GridNetwork`, `GridSite`, or
  `InferenceProvider` CRDs in a watch loop
- They do not perform live config reload against
  a running gateway

The `verify-swim-membership` and `verify-swim-state`
commands do spawn out-of-cluster operator processes that
run real SWIM and CRDT reconciliation, but they use
localhost UDP sockets and ephemeral fixtures — they are
not a substitute for in-cluster production deployment.

In the production architecture, continuous reconciliation
is the responsibility of the AGN Operator and its
controllers. `xtask env` commands are a validation
convenience layer, not a production orchestrator.

### Routing overlay file input

`deploy-consumer-gateway --overlay-config <path>`
accepts a `routing-config.json` routing overlay file. This
allows local validation of the overlay wire format and
consumer gateway config generation without running a
full production operator reconcile loop. The overlay
file format is:

```json
{
  "network": "<grid-network-name>",
  "local_site": "<gateway-local-site-name>",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "<model-name>",
      "site": "<provider-site-name>",
      "cluster": "<overlay-cluster-name>",
      "fresh": true
    }
  ]
}
```

When an overlay is supplied, the candidates come from the overlay.
`intelligent_route.local_site` is always the consumer site. The
`load_balancer` section is still generated from the
provider endpoints in the environment config.

### Separation from production reconciliation

The production architecture is operator-driven. The
AGN Operator reconciliation path owns long-lived
management of:

- `GridNetwork`, `GridSite`, and `InferenceProvider`
  CRD reconciliation
- SWIM mesh formation and certificate lifecycle
- Routing overlay ConfigMap generation and application

`xtask env` is a development convenience layer that
uses the same config and cert infrastructure, not a
production orchestrator. Production reconciliation
semantics are defined by the AGN Operator controllers,
not by the imperative `xtask env` command flow.

### Opinionated walkthroughs and topology fixtures

Scripts, static manifests, and walkthrough
documentation for specific gateway-to-gateway
topologies are maintained outside this repository
in the accompanying research-spikes repository.

AGN keeps generic, config-driven, reusable commands.
Topology-specific fixtures, static manifests, and
presentation walkthroughs belong outside the AGN
repository.

## References

- [HashiCorp memberlist](https://github.com/hashicorp/memberlist) — reference
  design for SWIM-style membership, gossip transport encryption, key rotation,
  and join/admission behavior. AGN uses foca rather than memberlist; foca used
  the Go-based memberlist implementation as a reference architecture, and AGN
  uses memberlist the same way: as an architectural reference for control-plane
  gossip hardening.
