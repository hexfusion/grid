# Custom Resource Definitions

API group: `grid.praxis.fast/v1beta1`

The AI Grid Network (AGN) Operator defines these resources to describe sites,
provider capacity, and routing policy.

All CRDs are cluster-scoped and in the `grid` category, so `kubectl get grid`
lists them.

## GridNetwork

The AGN logical network and top-level tenancy scope. A single
cluster can host multiple `GridNetworks` for
multi-tenancy.

```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: GridNetwork
metadata:
  name: production
spec:
  gridId: ""                    # empty: the operator generates one into status.gridId, immutable once set
  seeds:
    - "10.0.0.5:7946"
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
      localSiteName: cluster-east   # optional; defaults to network name
      consumerConfig:               # optional; opt-in consumer Praxis config generation
        enabled: true
        clusterEndpoints:           # endpoint topology for load_balancer
          - cluster: site-a
            address: "10.0.0.4:30080"
            transport:
              mode: mutualTls         # mTLS with CA verification and client cert
              sni: site-a.grid.internal
          - cluster: api-provider
            address: "mock-api.default.svc:8080"
            transport:
              mode: plaintext          # explicit insecure/dev-only — no TLS
  region: us-east-1
  zone: us-east-1a
  swim:
    probeInterval: 5s           # WAN probe interval
    suspicionTimeout: 10s       # before declaring dead
    gossipNodes: 3              # indirect probe fanout
  tls:
    caSecretRef:
      name: grid-ca
      namespace: praxis-system
    siteSecretRef:
      name: grid-site-cert
      namespace: praxis-system
    swimKeyRef:
      name: swim-key
      namespace: praxis-system
  budgetPolicy:                   # optional; absent means no tenants are tracked
    tenants:
      - tenantId: tenant-a
        capUsd: 100.0
      - tenantId: tenant-b
        capUsd: 250.0
```

### Routing policy fields

These optional `GridNetwork.spec` fields control provider ordering, admission,
and request selection. For guidance on composing them to achieve a routing
behavior, see the [AGN Routing Guide](../routing.md).

| Field | Supported values / shape | Default and interaction |
|---|---|---|
| `routingPolicy` | `geographyFirst`, `scoreFirst` | `geographyFirst`. Controls candidate ordering and selection-group boundaries. |
| `scoringPolicy.strategy` | `noMetrics`, `queueDepth`, `kvCachePressure` | `noMetrics`; when `scoringPolicy` is present, `strategy` is required. |
| `selectionPolicy.mode` | `deterministic`, `roundRobin`, `random`, `weightedRandom` | Omitted from the overlay when unset, and Praxis then uses deterministic selection. The grid-site chart sets `roundRobin`. |
| `placementPolicy.strategy` | `static` | Required if and only if selection mode is `weightedRandom`; rejected with other modes. |
| `admissionPolicy.mode` | `instantaneous`, `stabilized` | With `admissionPolicy` omitted, admission is instantaneous. The grid-site chart sets `stabilized` with the default `pressure` values and `missingMetrics: existingOnly`. Stabilized behavior requires repeated pressure/recovery observations. |
| `admissionPolicy.missingMetrics` | `existingOnly`, `excluded` | `existingOnly` when admission policy is configured. Applies when an active scoring strategy and provider signal are configured but the signal is missing or expired; it does not activate metrics observation. |
| `admissionPolicy.pressure` | Six fields: `enterThreshold`, `exitThreshold`, `failureThreshold`, `successThreshold`, `minimumStateDuration`, `recoveryHoldDown` | When `pressure` is omitted, defaults are `0.85`, `0.70`, `2`, `3`, `10s`, and `30s`, respectively. When supplied, all six fields are required. Exit must be lower than enter; durations must be positive whole seconds (for example, `10s`). |
| `metricsRefreshInterval` | Seconds or milliseconds, at least one second | `300s`. TLS-protected metrics cap the effective interval at `60s`. Controls metric refresh/re-ranking, not request-time selection. The operator ignores a value it cannot parse and uses the default. |

`geographyFirst` and `scoreFirst` are the only current routing-policy values.
Explicit grouping fields such as `selectionPolicy.grouping.localityScope` are
not part of this CRD.

### Discovery and trust fields

| Field | Supported values | Default and interaction |
|---|---|---|
| `siteDiscovery.mode` | `manual`, `auto` | `manual`. Gossip fills declared GridSites in both modes. `auto` also creates a GridSite for each undeclared member. See [Field ownership](#field-ownership). |
| `siteDiscovery.defaultEgressTls` | `mutualTls`, `plaintext` | `mutualTls`. The egress TLS mode of a GridSite with no `spec.egress`. A `plaintext` site never becomes routing-eligible. |
| `peerTrust.mode` | `pin`, `spiffe` | `pin`. Read at operator start, so a change needs an operator restart. |
| `signalTransport.mode` | `gossip`, `poll` | `gossip`. Read at operator start, so a change needs an operator restart. |

**Phases**: Pending → Initializing → Active → Degraded

**Status fields**: `conditions`, `gridId`, `connectedSites`, `distributedProviderCount`,
`observedGeneration`, `phase`, `consumerConfigStatus[]`, `overlayStatus[]`, `budgetStatus[]`

Each `overlayStatus[]` entry reports one gateway's routing overlay:

| Phase | Reason | Meaning |
|---|---|---|
| `Distributed` | _(empty)_ | The overlay `ConfigMap` holds the current render. |
| `Pending` | `EmptyCandidates` | The gateway has no routing candidate yet, so no overlay or serving config is written. Normal on a grid with no providers. |
| `Retained` | `EmptyCandidates`, `OverlayApplyFailed`, `OverlayRenderFailed` | The last distributed overlay stays in place. |
| `Error` | `OverlayApplyFailed`, `OverlayRenderFailed` | No overlay was ever distributed and the latest attempt failed. |

When `spec.tls` names both `caSecretRef` and `siteSecretRef` and either Secret is
missing, the network reports `Ready=False` with reason `TrustMaterialMissing`. The
operator mints a self-signed dev CA and site certificate only with
`GRID_DEV_SELF_SIGNED_CA` (grid-operator chart value `devSelfSignedCa`). Even then,
both Secrets must be missing and both must live in the operator namespace. It
creates them and never replaces an existing Secret.

`distributedProviderCount` reflects the number of remote `InferenceProvider`
records received from peer sites via CRDT broadcast.  Local providers and records
from other `GridNetwork`s are excluded from the count.

`consumerConfigStatus[]` is populated for each gateway with
`consumerConfig.enabled: true`, reporting the outcome of the most recent
render/apply attempt.

### Tenant budget tracking

`budgetPolicy.tenants[]` opts individual tenants into cumulative spend
tracking. AGN merges each site's locally recorded spend for a tenant into a
per-tenant CRDT counter (a `GCounter`, one slot per originating site) that is
gossiped over SWIM alongside provider state, so the reported total reflects
spend recorded anywhere in the grid, not just the local site.

For every tenant declared in `budgetPolicy`, `budgetStatus[]` reports:

- `tenantId` — matches `budgetPolicy.tenants[].tenantId`
- `capUsd` — copied from the policy, in USD
- `spendUsd` — the converged cross-site total, in USD
- `spendRatio` — `spendUsd / capUsd`, for at-a-glance dashboarding

`budgetStatus[]` is a status **signal only**. AGN does not itself degrade or
reject traffic when a tenant's `spendRatio` reaches or exceeds `1.0` — that
enforcement decision is expected to live in a gateway-side policy filter
(cross-repo, `praxis-ai`), the same split used for `provider_route`
authorization. Real per-request tenant attribution also depends on
upstream work (`praxis-ai#130`/`praxis-ai#104`) and does not exist yet.

Because `budgetStatus[]` is visible to any caller with read access to the
`GridNetwork` resource, and Kubernetes RBAC is not field-level, a reader
authorized to view one tenant's status can see every other tracked tenant's
spend on the same `GridNetwork`. See
[`grid#48`](https://github.com/praxis-proxy/grid/issues/48) for the
options under consideration if per-tenant confidentiality is required.

| Field | Type | Meaning |
|---|---|---|
| `gatewayName` | string | Name of the gateway reference |
| `namespace` | string | Namespace of the gateway and generated `ConfigMap` |
| `configMapName` | string | Operator-owned name of the generated `ConfigMap`, `grid-consumer-<network>-<gateway>` |
| `phase` | enum | `Rendered` \| `Pending` \| `Error` \| `Disabled` |
| `reason` | string | Machine-readable reason. `Pending`: `EmptyCandidates`, while the gateway has no routing candidate. `Error`: `MissingClusterEndpoint`, `MissingTransport`, `MissingSni`, `PlaintextWithSni`, `ConsumerConfigRenderFailed`, `ConsumerConfigApplyFailed`, or `ConsumerConfigError`. `Rendered`: empty, or the warning `RoutedAddressMismatch` or `ListenerPortUnresolved`, which outranks it. `Disabled`: `ConsumerConfigDisabled` |
| `message` | string | Human-readable diagnostic; never contains token bytes |
| `observedGeneration` | integer | `GridNetwork` generation when this entry was last updated |

Example status output:

```yaml
status:
  phase: Active
  gridId: grid-abc123
  connectedSites: 2
  consumerConfigStatus:
    - gatewayName: inference-gw
      namespace: praxis-system
      configMapName: grid-consumer-production-inference-gw
      phase: Rendered
      reason: ""
      message: "consumer config rendered and applied to praxis-system/grid-consumer-production-inference-gw"
      observedGeneration: 7
    - gatewayName: fallback-gw
      namespace: default
      configMapName: grid-consumer-op-e2e-net-op-e2e-gw
      phase: Error
      reason: ConsumerConfigRenderFailed
      message: "consumer config render: overlay local_site must not be blank"
      observedGeneration: 7
```

### CRD-driven SWIM seeds

`spec.seeds` is a list of SWIM endpoints (`host:port`) used to bootstrap
SWIM mesh formation. Each entry may be a literal IPv4 address, a bracketed IPv6
address, or a DNS hostname. Hostnames are resolved with a bounded lookup before
they are announced to the running SWIM runtime on every
`GridNetwork` reconcile.  Re-announcing to an existing peer is idempotent — foca
ignores redundant joins.

**Runtime update behavior:**

| Change | Effect |
|---|---|
| Seed added to `spec.seeds` | Announced to the SWIM runtime on the next reconcile (~300 s default); SWIM join initiated |
| Seed removed from `spec.seeds` | No active disconnect; SWIM failure detection ages out the peer naturally |
| `spec.seeds` unchanged | Re-announced on every reconcile; idempotent, no side effects |

**Channel-full behavior:** If the SWIM announce channel is temporarily full
(capacity 16 batches), the announce is silently retried on the next reconcile.
Seeds are not guaranteed to be joined within one reconcile cycle under heavy
broadcast load, but the retry is automatic.

**Scope:** `spec.seeds` targets the SWIM site-membership layer.  The SWIM runtime
is process-global — all `GridNetwork` resources in the operator process share the
same SWIM node.  Seeds from any `GridNetwork` reach the shared SWIM membership
table.  Provider CRDT state remains scoped per network.

**Self-filtering:** The operator removes its own SWIM bind address from
`spec.seeds` before announcing, preventing self-join loops.

**`spec.tls.swimKeyRef`:** References a Kubernetes Secret containing the 32-byte
AES-256-GCM key for SWIM transport authentication.  When configured, the
`GridNetwork` controller reads the key from the Secret and configures the
SWIM runtime to encrypt all outgoing UDP packets and reject incoming packets
that fail authentication before it announces CRD seeds or publishes
certificate/provider state for that reconcile.

The Secret must contain a `"key"` field (or the field named by `swimKeyRef.key`)
with exactly 32 bytes.  If the Secret is absent, unreadable, or has the wrong
length, the reconcile fails before CRD seed announcement and state broadcast.
The process-global SWIM runtime keeps any previously loaded key until restart;
it does not switch to plaintext for that configured reconcile.
With `GRID_SWIM_REQUIRE_KEY` (default `true`), the operator holds all SWIM
traffic until a `GridNetwork` loads the key or declares none.

For local development and testing, the `GRID_SWIM_ENCRYPT_KEY` environment
variable (64-character hex) provides an alternative key injection path without
requiring a Kubernetes Secret.  Because environment variables are visible to
same-host process inspectors, this is not the production Secret delivery path.

Routing eligibility remains gated separately by `GridSite.status.phase == Active`
regardless of SWIM encryption configuration. Active status indicates control-plane
eligibility only: sufficient trust information exists to include the site in routing
overlays. Data-plane security readiness (mTLS handshake completion, client certificate
validation, routing config loading) is verified separately at request time.

### Stale candidate TTL

`spec.staleCandidateTtlSeconds` controls when stale (fresh=false) remote
candidates are evicted from the rendered overlay. In auto discovery it also
collects unedited auto-created GridSites, as described under GridSite.

| Value | Behaviour |
|---|---|
| Absent (default) | Stale candidates are retained indefinitely in the overlay. |
| `0` | Rejected by the CRD schema (`minimum: 1`). |
| `N >= 1` | Remote candidates with SWIM member age `>= N` seconds are omitted from the overlay. |

Local and healthy remote candidates are never evicted.  CRDT storage records
are not deleted by this mechanism.

### GatewayRef.consumerConfig

`spec.gatewayRefs[].consumerConfig` opts a gateway into operator-managed consumer
Praxis `ConfigMap` generation.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Set to `true` to enable consumer config generation for this gateway. |
| `clusterEndpoints[]` | `[]` | Endpoint topology for `load_balancer` clusters. Each entry maps a candidate cluster name to an address with explicit `transport` configuration. Missing transport fails closed. |
| `clusterEndpoints[].transport.mode` | _(required)_ | `mutualTls` (mTLS with CA/client cert/SNI/verify) or `plaintext` (no TLS, insecure/dev-only). |
| `clusterEndpoints[].transport.sni` | _(required for `mutualTls`)_ | TLS Server Name Indication; must match the provider certificate SAN. |

The operator names the generated `ConfigMap` `grid-consumer-<network>-<gateway>`.
The credential mount base, TLS mount path, and fallback listener port are operator
deployment settings (grid-operator chart `consumer.*` values), not grid intent.

When `enabled: true`, the `GridNetwork` controller renders a `praxis.yaml`-keyed
`ConfigMap` in the gateway namespace on each reconcile.  The generated config is a
complete, runnable Praxis config containing:

- `listeners:`: one public listener on the numeric `targetPort` of the gateway Service named by the gateway ref (its only port, or the port named `http`), falling back to `GRID_CONSUMER_LISTENER_PORT` with reason `ListenerPortUnresolved`
- `filter_chains:` — the consumer chain with:
  - `intelligent_route` candidates from the routing overlay (with `credential.secretRef` for
    credential-bearing candidates)
  - `credential_inject` entries (one per unique credential reference) using
    `file:` sources — token bytes are never written to the `ConfigMap`
  - `load_balancer` entries (one per unique candidate cluster). Every referenced
    cluster must have a matching `clusterEndpoints[]` entry with endpoint address
    and explicit `transport` configuration.  `transport.mode` is the security
    switch — not `sni` presence.  Missing transport fails closed
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

The generated credential-injection config assumes the gateway is the egress
component for the selected backend.  This is correct for direct API-provider and
cloud-provider fallback routes.  For remote provider sites, provider credentials
should be mounted only in the remote site or provider-side component that makes
the final backend call.

The `credential_inject` filter is a Praxis AI runtime dependency. The AGN
operator can render the config shape, but the deployed Praxis AI image must
include that filter for the generated config to start successfully.

When `enabled: false` or `consumerConfig` is absent, this gateway behaves as before
— only the routing overlay `ConfigMap` is applied.

## GridSite

Represents another site in the grid. Declared by the user, for example one per
`peers` entry in the grid-site chart, or created by SWIM discovery when
`siteDiscovery.mode` is `auto`.

```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: GridSite
metadata:
  name: cluster-b
  labels:
    grid.praxis.fast/network: production
spec:
  gridNetworkRef: production
  egress:
    address: egress.cluster-b.example.com:8443
    tls:
      mode: mutualTls
      serverName: egress.cluster-b.example.com
  trust:
    canonicalFingerprints:
      - "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  region: us-east-1
  zone: us-east-1a
  sovereigntyZone: us
```

**Phases**: Pending → Discovered → Connecting → Active → Unreachable → Left

**Status fields**: `conditions`, `phase`, `observedGeneration`, `discovered`
(`egressAddress`, `absentSince`), `capabilities`
(inference, agentTools, agentToAgent), and `lastProbeTime`. The reason and
message for the current phase live on the `Connected` condition.

The GridSite name is the SWIM site ID, with no network prefix, so a declared
GridSite and the site SWIM discovers are the same object. The ID must already be
a DNS-1123 label. Discovery refuses any other ID rather than rewriting it.

### Field ownership

Spec is user intent, written by Helm, Argo CD, or `kubectl`. The operator writes
status. It writes the spec of a GridSite only when it creates that GridSite, and
it creates one only when `spec.siteDiscovery.mode` on the GridNetwork is `auto`
and no GridSite with that name exists. Created GridSites carry the label
`grid.praxis.fast/auto-discovered=true`, and the operator owns them end to
end.

Gossip fills `status.discovered` on the matching GridSite in both modes. In
`manual` mode, the default, the operator still observes declared GridSites and
advances them from `Pending` to `Discovered`. It only skips creating the missing
ones.

Discovery names a GridSite after a SWIM site ID only when the ID is already a
DNS-1123 label. It skips any other ID rather than normalizing it, so `HUB` never
lands on GridSite `hub`. Auto mode creates at most 256 GridSites per network.
Past that, the GridNetwork reports `DiscoveryConflict` with reason
`AutoDiscoveryCapReached`. With `spec.staleCandidateTtlSeconds` set, the operator
deletes an auto-created GridSite whose member has been absent or Dead that long,
timed from `status.discovered.absentSince`, unless someone edited its spec.
When a peer withdraws its gateway address, the operator clears
`status.discovered.egressAddress`. The operator probes a gossiped address only
when it is a literal `IP:port` outside loopback, link-local, unspecified, and
cloud metadata ranges. Otherwise the site holds at Discovered with reason
`GossipedAddressRefused`. A declared `spec.egress.address` may name a host, and
the operator trusts cluster DNS to resolve it. In
poll signal mode the operator dials a member's signals endpoint only when a
GridSite exists for it: one with pins under `peerTrust.mode: pin`, any GridSite
under `spiffe`, auto-created ones included. Under `spiffe` a gossiped signals
endpoint must also be a literal IP, because auto discovery creates the GridSite
from gossip.

When a SWIM member's name belongs to a GridSite of another network, the operator
leaves that GridSite untouched. The discovering GridNetwork reports
`DiscoveryConflict=True` with reason `FieldConflict` and names the member and the
owning network. The same condition reports a member whose ID is not a DNS-1123
label (`SiteIdInvalid`), and an auto-created GridSite that carries
`spec.egress` (`StaleAutoDiscoveredEgress`), which the operator never writes, so
it can only come from re-applying an old export. Two live SWIM addresses claiming
one ID past a five-minute restart grace report `SiteIdDuplicated`, and discovery
clears that site's gossiped egress. Mixed problems report
`UnadoptableMembers`. None of these changes `Accepted`, which stays about the
network's own spec, or `Ready`, because the network still serves every site it
did adopt.

Only the oldest GridNetwork with `siteDiscovery.mode: auto` runs discovery on an
operator. Another one reports `Accepted=False` with reason `AutoDiscoveryClaimed`
and is not reconciled until its spec changes.

With Argo CD, ignore operator-written state:

```yaml
ignoreDifferences:
  - group: grid.praxis.fast
    kind: GridNetwork
    jsonPointers: [/status]
  - group: grid.praxis.fast
    kind: GridSite
    jsonPointers: [/status]
  - group: grid.praxis.fast
    kind: InferenceProvider
    jsonPointers: [/status]
  - group: grid.praxis.fast
    kind: AgentToolProvider
    jsonPointers: [/status]
```

Argo CD does not manage auto-created GridSites. Exclude them from the
Application, for example with a resource exclusion on the
`grid.praxis.fast/auto-discovered=true` label, so a prune never deletes
them.

### Conditions

Every grid resource reports `metav1.Condition` entries in `status.conditions`.
Each entry carries `observedGeneration`, and `lastTransitionTime` changes only
when the condition's status changes.

| Resource | Conditions |
|---|---|
| GridNetwork | `Accepted`, `Ready`, `DiscoveryConflict` |
| GridSite | `Accepted`, `Discovered`, `Connected`, `Ready` |
| InferenceProvider | `Accepted`, `Available`, and `MetricsSignals` when `metricsConfig` is set |
| AgentToolProvider | `Accepted`, `Available` |

### GridSite lifecycle

SWIM discovery, authentication, and authorization are separate concerns:

- SWIM discovery identifies a peer and records liveness.
- Authentication proves the peer gateway identity, normally through mTLS
  certificate validation.
- Authorization decides whether that authenticated peer is allowed to
  participate in the AI Grid Network or carry traffic for a given policy scope.

A discovered SWIM peer is not automatically authorized for routing.

| Phase | How entered | Transition driver |
|---|---|---|
| `Pending` | Resource created (declared or by auto-discovery) | Initial default |
| `Discovered` | SWIM peer observed as Alive | `GridNetwork` controller writes on first observation. The `Discovered` condition reason is `MemberKnown` from here on |
| `Connecting` | Gateway address known (`spec.egress.address` or `status.discovered.egressAddress`) | `GridSite` controller advances from Discovered and runs an identity-aware probe |
| `Active` | `TlsVerified` | `GridSite` controller promotes from Connecting only after identity-verified TLS succeeds |
| `Unreachable` | Connectivity failure while Active | `GridSite` controller moves Active → Unreachable when the endpoint cannot be reached |
| `Left` | No operator path enters it | Preserved by operator once set |

**Reason codes** (the `Connected` condition reason):

| Reason | Phase | Meaning |
|---|---|---|
| `AwaitingDiscovery` | Pending | Site record exists; SWIM has not yet observed the peer as Alive |
| `GatewayAddressKnown` | Connecting | Gateway address received; advancing to Connecting |
| `GatewayAddressMissing` | Discovered | No gateway address known. The remote operator advertises one from `GRID_GATEWAY_ADDRESS` or its gateway Service LoadBalancer |
| `GossipedAddressRefused` | Discovered | The only address is a gossiped one that is not a dialable literal `IP:port`. Set `spec.egress.address` |
| `EgressMissing` | Connecting or Unreachable | A previously probed site has no egress address |
| `TlsVerified` | Active | The handshake verified the chain to the Grid CA and the identity. The message names it: the SPIFFE ID under `spiffe`, the pinned leaf digest under `pin` |
| `PlaintextIneligible` | Connecting or Unreachable | Egress is plaintext, which never routes. `Connected=False`, and the message says whether TCP reached |
| `ConnectTimeout` / `ConnectionFailed` | Connecting or Unreachable | TCP connection timed out or failed |
| `HandshakeTimeout` / `TlsProtocolError` | Connecting | TLS handshake timed out or failed |
| `UntrustedIssuer` | Connecting | Server certificate does not chain to the configured AGN trust root |
| `IdentityMismatch` | Connecting | Server SAN does not match the `serverName`, or, under `spiffe`, the leaf lacks the expected SPIFFE ID |
| `CertificateExpired` / `CertificateNotYetValid` | Connecting | Server certificate is outside its validity period |
| `PinMismatch` | Connecting | Under `pin`, the live leaf digest matches no configured pin |
| `TrustMaterialMissing` | Connecting | CA, client certificate, key, or pin policy is absent. An empty `serverName` defaults to `<name>.grid.internal` |
| `TrustMaterialInvalid` | Connecting | The probe found malformed or oversized trust material in the grid Secrets. Reported on `Connected` and `Ready`, like `TrustMaterialMissing`, not on `Accepted` |
| `TrustConflictsWithPeerTrust` | Held, never Active | `spec.trust` sets pins while the GridNetwork `peerTrust.mode` is `spiffe` |
| `ServerNameForbidden` | Held, never Active | Declared `spec.egress` uses `plaintext` with a `serverName` |
| `ServerNameInvalid` | Held, never Active | `spec.egress.tls.serverName` is not a valid DNS name |

**GridSite phase transitions:**

- Pending → Discovered: the `GridNetwork` controller writes `Discovered` when a remote SWIM
  peer is first observed as Alive, in either `siteDiscovery.mode`.
- Discovered → Connecting: the `GridSite` controller advances automatically when
  `spec.egress.address` or a dialable `status.discovered.egressAddress` is non-empty. The
  discovered address comes from the remote operator, propagated via SWIM state broadcast. That
  operator advertises `GRID_GATEWAY_ADDRESS` when set, and otherwise the LoadBalancer address of
  its gateway Service (`GRID_GATEWAY_SERVICE_NAME` in `GRID_GATEWAY_NAMESPACE`) with
  `GRID_GATEWAY_PORT`. With no address, the site stays Discovered with reason
  `GatewayAddressMissing`.
- Connecting: the `GridSite` controller probes the egress gateway on each reconcile.  For
  `mutualTls` mode, the probe performs a bounded TLS handshake verifying the CA chain and the
  `serverName` SAN. It then checks the leaf against the `canonicalFingerprints` pins when the
  GridNetwork `peerTrust.mode` is `pin`, or against the exact SPIFFE ID
  `spiffe://grid.internal/site/<name>` when it is `spiffe`. For explicit
  `Plaintext` mode, it performs a bounded TCP connect for diagnostics but keeps
  the site in `Connecting`; TCP reachability alone cannot make a site
  routing-eligible. Active also requires mTLS client credentials
  (`siteSecretRef` must be configured on the `GridNetwork`).
- Active → Unreachable: the `GridSite` controller demotes Active to Unreachable when the probe
  cannot connect. Probes use a 2 second connect timeout, and an Unreachable site is probed
  again after half its time down, between 30 seconds and 5 minutes plus jitter. Identity or trust failures demote Active to Connecting, distinguishing a
  reachable but unverified endpoint from an unreachable endpoint.

**Egress address source:** `spec.egress.address` is an override. When it is empty, the probe
uses `status.discovered.egressAddress`, which the remote operator advertises through the SWIM
state broadcast. With neither set, the site stays
Discovered. `spec.egress.tls` applies to either address, so a GridSite can pin `serverName`
without pinning the address. With no `spec.egress`, the TLS mode is the GridNetwork's
`spec.siteDiscovery.defaultEgressTls` (`mutualTls` by default, or `plaintext`), which auto-created
GridSites inherit. To change one discovered site, declare its GridSite with `spec.egress.tls.mode`. `Active` proves the address the probe
dialed, not the consumer `clusterEndpoints` a gateway routes to.

Gossip carries no certificate. Trust comes only from the live handshake: the
pins under `peerTrust.mode: pin`, or the CA chain and the exact SPIFFE ID under
`spiffe`.

Private keys, bearer tokens, provider credentials, and Kubernetes Secret contents must never
be written to status.

**`spec.egress.tls` fields:**

| Field | Meaning |
|---|---|
| `mode` | `mutualTls` (default): TLS handshake with CA verification and client auth. `plaintext`: TCP-only diagnostics that never become routing-eligible |
| `serverName` | Expected DNS identity for TLS SNI and SAN verification. Defaults to `<name>.grid.internal`, the DNS name enrollment issues. Must be absent for `plaintext` |

**`spec.trust` fields:**

| Field | Meaning |
|---|---|
| `canonicalFingerprints` | Required DER-certificate SHA-256 pins (`hex(sha256(der_bytes))`), one or two entries for bounded rotation overlap |

**Routing eligibility:** `GridSite.status.phase == Active` is the control-plane eligibility
gate for remote CRDT provider records. Active means the control plane has
verified the remote site's identity through TLS, which is sufficient to include
the site's providers in routing overlays for consideration.

Provider records from a peer whose `GridSite` is in `Discovered`, `Connecting`, `Pending`,
`Unreachable`, or `Left` are excluded from the routing overlay. Records from a peer with
no matching `GridSite` are also excluded (fail-closed).

GridSite Active is a control-plane eligibility signal. It means AGN has enough
site/trust information to consider the site for overlay generation. It does not
prove that a Praxis gateway has loaded the latest routing config or authorized
provider-side traffic. Data-plane readiness is verified separately at request
time by provider gateway filters.

**Single-site / combined-site deployments:** Site discovery is driven by *remote*
SWIM peers - the `Pending -> Discovered` transition above requires a remote peer
observed Alive. A single or combined cluster has no peers, so its own `GridSite`
stays `Pending` with reason `AwaitingDiscovery`. **This is expected and does not
block local serving:** local `InferenceProvider`s are eligible regardless of
`GridSite.status.phase`; only *remote* CRDT provider records are phase-gated (see
"Routing eligibility" above). Do not add SWIM seeds or extra operator replicas to
try to force the site `Active` - there is no second site to discover, and a lone
operator legitimately runs a single-node mesh with zero peers. The `Active` phase
and its mTLS gateway probe (`spec.egress` + `spec.trust`) apply to reaching
*remote* sites, or a manually-configured peer gateway endpoint. See
[Architecture Overview -> Single-Site and Combined Deployments](overview.md#single-site-and-combined-deployments).

See [Routing eligibility](routing.md#routing-eligibility) for the full gating rule.

Example status — Mutual TLS verified:

```yaml
status:
  phase: Active
  observedGeneration: 5
  lastProbeTime: "2026-07-30T12:00:00Z"
  conditions:
    - type: Connected
      status: "True"
      reason: TlsVerified
      message: "TLS handshake verified: chain to the Grid CA and pinned leaf digest 3f2a91c0d4e7"
      observedGeneration: 5
      lastTransitionTime: "2026-07-30T11:55:00Z"
```

Example status — trust material missing:

```yaml
status:
  phase: Connecting
  observedGeneration: 3
  conditions:
    - type: Connected
      status: Unknown
      reason: TrustMaterialMissing
      message: "required trust material not available"
```

Example status — gateway address not configured on remote operator:

```yaml
status:
  phase: Discovered
  observedGeneration: 2
  conditions:
    - type: Connected
      status: Unknown
      reason: GatewayAddressMissing
      message: "gateway address not yet available; cannot advance to Connecting"
```

### Spec validation

The operator validates every business rule in Rust and reports a failure as
`Accepted=False` with a stable reason, plus a Warning event the first time that
reason appears. A rejected spec never fails a reconcile or restarts the operator.

| Resource | Reasons | Effect |
|---|---|---|
| GridNetwork | `BudgetPolicyInvalid`, `AutoDiscoveryClaimed` | The operator stops reconciling the network until the spec changes. |
| GridSite | `TrustConflictsWithPeerTrust`, `ServerNameForbidden`, `ServerNameInvalid` | The site is not probed and never holds `Active`. |
| InferenceProvider | `UnsupportedAuthStrategy`, `CredentialSecretRefInvalid`, `EndpointInvalid`, `MetricsEndpointInvalid`, `ModelNameInvalid` | The provider is `Unavailable`. |
| AgentToolProvider | `EndpointInvalid`, `GridNetworkRefInvalid`, `UnsupportedAuthStrategy`, `CredentialSecretRefInvalid` | The provider is `Unavailable`. |

A consumer cluster endpoint with a missing transport or a wrong SNI fails only
its own gateway. That gateway's `consumerConfigStatus` entry reports `Error`
with `MissingTransport`, `MissingSni`, or `PlaintextWithSni`. The network keeps
serving its other gateways. See [Consumer config](consumer-config.md).

An endpoint must be an http or https URL with a host. An InferenceProvider
`spec.endpoint` may also be a bare `host:port`. `metricsEndpoint` must be a URL.
The schema carries matching patterns, so a malformed endpoint or a blank model
name fails at apply time as well. The GridNetwork controller routes and scrapes
an InferenceProvider only once its `Accepted` condition is `True`.

The CRD schema stays structural: types, required fields, enums, lengths, ranges,
and three CEL rules on GridNetwork: `exitThreshold` below `enterThreshold`,
`placementPolicy` set only with `weightedRandom`, and `gridId` immutable once set. A GitOps apply
therefore succeeds even when a business rule fails, and the failure shows in
`status.conditions` and events, not at apply time.

Once these rules are stable, the side-effect-free ones move into
`x-kubernetes-validations`, so an apply reports them directly. Those are the SNI
rules for GridSite egress and consumer cluster endpoints, the endpoint URL
rules, and the budget tenant rules. Rules that read another object stay in Rust, because CEL
cannot read other objects. Those are the GridSite trust rule against the
GridNetwork `peerTrust.mode`, and the provider `gridNetworkRef` lookup. The Rust
checks stay in place after the move, as defense against objects admitted before
a rule existed.

## InferenceProvider

Represents an inference backend available over the
grid.

```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: InferenceProvider
metadata:
  name: openai-api
spec:
  gridNetworkRef: production
  providerKind: openAi          # openAi | anthropic | bedrock | vertex
  backendKind: apiProvider     # local | remote | cloudManaged | apiProvider
  endpoint: https://api.openai.com
  models:
    - name: gpt-5-mini
      capabilities: [text_generation]
  auth:
    strategy: bearerToken      # current native path; see Auth doc
    secretRef:
      name: openai-token
      namespace: praxis-system
      key: token
  accessPolicy:
    siteSelector:
      matchLabels: {}           # empty = all sites
  hostSelector:
    matchLabels: {}             # empty = every site; omitted = no site
```

`spec.hostSelector` names the GridSites that host the provider, on
InferenceProvider and AgentToolProvider alike. An empty selector matches every
site. An omitted one matches no site: the provider stays `Pending` with the
`Available` reason `HostSelectorMissing` and yields no routing candidate. The
API server prunes unknown fields, so a manifest that still says `siteSelector`
lands here instead of on every site.

This external API example intentionally omits `healthCheck`: AGN probes health
with an unauthenticated HTTP `GET`, which is not the provider's authenticated
inference API. It also omits metrics scraping because the external API does not
provide the provider-pool metrics used by AGN scoring. The referenced
`openai-token` Secret must exist in `praxis-system` before controller-managed
credential projection can become available.

**Phases**: Pending → Available → Degraded → Unavailable

**Status fields**: `conditions`, `phase`, `matchingSites`, `metricsSignals`,
`modelDiscoveryUrl`, `modelDiscoveryError`, `observedGeneration`. The reason for
the current phase is on the `Available` condition.

`spec.capacityWeight` is an optional positive relative provider capacity from
`1` through `1000`, used only with `GridNetwork.spec.selectionPolicy.mode:
weightedRandom` and `placementPolicy.strategy: static`. If omitted, the
effective weight is `1`. AGN copies this value to the overlay's
`traffic_weight`; it does not represent a percentage.

### Provider kind

`spec.providerKind` is the wire API the provider speaks: `openAi`,
`anthropic`, `bedrock`, or `vertex`. Self-hosted vLLM and llm-d serve the
OpenAI API, so they use `openAi`.

### Backend kind

`spec.backendKind` describes the provider's placement and policy category:

| Value | Meaning |
|-------|---------|
| `local` | Self-hosted capacity in the local site. |
| `remote` | Self-hosted capacity in another AGN site. |
| `cloudManaged` | Managed cloud capacity controlled by the operator's cloud account. |
| `apiProvider` | External API/SaaS provider used as fallback or explicit API route. |

The value influences scoring and routing policy. It does not require a specific
transport implementation; for example, a `cloudManaged` backend can still be
fronted by Praxis.

The CRD schema enumerates these four values, so the API server rejects any
other `backendKind`.

### Credential projection

`spec.auth.secretRef` points to a Kubernetes Secret that contains provider
credential bytes.  For the current native `bearer_token` path:

1. The operator validates that the Secret exists and contains the referenced key.
2. The routing overlay candidate receives only:

   ```json
   {
     "credential": {
       "strategy": "bearer_token",
       "secretRef": {
         "name": "anthropic-token",
         "namespace": "praxis-system",
         "key": "token"
       }
     }
   }
   ```

3. The consumer Praxis config uses `credential_inject` with a `file:` source
   pointing at a mounted Secret file.

Token bytes do not appear in the overlay `ConfigMap`, `intelligent_route` candidates,
filter metadata, or the consumer Praxis `ConfigMap`.

### Metrics configuration

`spec.metricsConfig` configures the operator-side Prometheus scrape used during
`GridNetwork` reconciliation. When present, the operator scrapes
`{spec.endpoint}{metricsConfig.path}`, parses the configured signal names, and
feeds the resulting `BackendMetrics` into overlay scoring.

| Field | Default | Meaning |
|-------|---------|---------|
| `metricsEndpoint` | absent | Optional metrics-service base URL. When set, it replaces `spec.endpoint` as the scrape base; `path` is appended to the selected base. |
| `path` | `/metrics` | HTTP path, relative to `metricsEndpoint` when set, otherwise `spec.endpoint`. |
| `timeout` | `2s` | Scrape timeout. `s` and `ms` suffixes are recognized. |
| `preset` | absent | `vllm` or `llmdEpp`. Fills `queueDepth` and `kvCacheUtilization` with the exporter's metric names. Each `signalNames` entry overrides the preset for that signal. |
| `poolName` | absent | Selects samples whose Prometheus `name` label matches this pool. When set, the scrape must contain at least one configured signal for that pool. |
| `queueCapacity` | absent | For raw queue-depth counts, divide by this positive capacity and clamp the normalized value to `0.0..1.0`. Without it, queue depth must already be normalized. |
| `signalNames` | absent | Mapping from scoring signals to Prometheus metric names. |
| `staleMetricsSeconds` | absent | Maximum age in seconds for reusing the last successful sample after a failed scrape. Minimum: `1`. For plaintext metrics, absence means immediate neutral fallback; when TLS is configured, an expired/absent sample makes the provider unhealthy and excluded. |
| `tls` | absent | TLS configuration for metrics scraping.  See [TLS and mTLS](#tls-and-mtls). |

| Preset | `queueDepth` | `kvCacheUtilization` |
|---|---|---|
| `vllm` | `vllm:num_requests_waiting` | `vllm:kv_cache_usage_perc` |
| `llmdEpp` | `llm_d_epp_average_queue_size` | `llm_d_epp_average_kv_cache_utilization` |

Both preset queue metrics are raw counts, so a preset needs `queueCapacity`. The
operator writes the names in effect to `status.metricsSignals` and reports the
`MetricsSignals` condition. It is `False` with reason `NoSignalNames` when
neither a preset nor `signalNames` names a signal. It is `False` with reason
`MissingQueueCapacity` when the preset queue metric has no `queueCapacity`. The
scrape then leaves the queue metric out, so queue depth scores neutrally instead
of reading a raw count as full saturation.

Providers without `metricsConfig` and signals without configured metric names
use neutral metric scores. For scrape failures, plaintext configuration retains
the neutral-scoring compatibility behavior; TLS-configured metrics fail closed
after any configured stale-sample grace period expires. See
[Stale metrics grace period](routing.md#stale-metrics-grace-period) in the
routing architecture for full semantics.

#### Signal names

| Field | Expected value |
|-------|----------------|
| `queueDepth` | Normalized queue depth from `0.0` to `1.0`. |
| `kvCacheUtilization` | KV-cache utilization from `0.0` to `1.0`. |
| `latencyP99Ms` | P99 request latency in milliseconds. |
| `prefixCacheHitRatio` | Prefix-cache hit ratio from `0.0` to `1.0`. |
| `errorRate` | Error rate from `0.0` to `1.0`. |
| `healthy` | Health gauge interpreted by the metrics parser. |

#### TLS and mTLS

`metricsConfig.tls` enables Secret-backed TLS (and optionally mutual TLS)
for metrics scraping.  When configured, the operator resolves PEM material
from Kubernetes Secrets at reconcile time and uses it for all scrape
requests to this provider's metrics endpoint.

| Field | Required | Meaning |
|-------|----------|---------|
| `tls.caSecretRef` | yes | Secret containing the CA certificate for server verification. Default key: `ca.crt`. |
| `tls.clientCertificateSecretRef` | no | Secret containing client certificate and private key for mTLS. |
| `tls.clientCertificateSecretRef.certificateKey` | no | Key within `Secret.data` for the certificate PEM. Default: `tls.crt`. |
| `tls.clientCertificateSecretRef.privateKeyKey` | no | Key within `Secret.data` for the private key PEM. Default: `tls.key`. |

`caSecretRef` follows the same [`SecretRef`](#credential-projection) schema used by
`spec.auth.secretRef`.  `clientCertificateSecretRef` adds explicit
`certificateKey` and `privateKeyKey` fields with serde defaults.

**Failure behavior**: when TLS material cannot be resolved or parsed, the
provider phase is downgraded from `Available` to `Degraded`, and the
`Available` condition carries one of these reasons:

| Reason | Category | Meaning |
|--------|----------|---------|
| `MetricsTlsSecretMissing` | reconcile-time | A referenced Secret does not exist. |
| `MetricsTlsKeyMissing` | reconcile-time | The expected key is absent from `Secret.data`. |
| `MetricsTlsMaterialInvalid` | reconcile-time | PEM material could not be parsed. |
| `MetricsTlsIdentityMismatch` | reconcile-time | Client certificate and private key do not match. |

Scrape-time handshake, authorization, timeout, and transport failures are
classified in operator logs, not surfaced as an `InferenceProvider` condition reason.
The last successful sample is reused only within `staleMetricsSeconds`; after
that, a TLS-configured provider is marked unhealthy (`healthy: false`) and
excluded from routing. Without TLS, scrape failures retain the neutral-scoring
compatibility behavior.

There is no `insecureSkipVerify` option.

Example (one-way TLS):

```yaml
metricsConfig:
  path: /metrics
  timeout: 2s
  signalNames:
    queueDepth: vllm:num_requests_waiting
  tls:
    caSecretRef:
      name: metrics-ca
      namespace: grid-system
```

Example (mTLS):

```yaml
metricsConfig:
  path: /metrics
  timeout: 2s
  signalNames:
    queueDepth: vllm:num_requests_waiting
  tls:
    caSecretRef:
      name: metrics-ca
      namespace: grid-system
    clientCertificateSecretRef:
      name: metrics-client-cert
      namespace: grid-system
      certificateKey: tls.crt
      privateKeyKey: tls.key
```

#### Queue depth normalization

Without `queueCapacity`, AGN does not normalize raw queue counts, and exporters
should publish `queueDepth` as a normalized gauge from `0.0` to `1.0`. With it, the
operator divides the raw value by `queueCapacity` and clamps the result.

### Model discovery

`spec.modelDiscovery` makes the operator poll the backend for the models it
serves. Discovery does not yet affect routing or gossip; `spec.models`
remains the routing source.

```yaml
modelDiscovery:
  openAiModels:                 # GET {endpoint}{path}, reads data[].id
    endpoint: http://vllm:8000  # optional; defaults to spec.endpoint
    path: /v1/models            # default
    tls: {...}                  # optional; same shape as metricsConfig.tls
```

The effective request URL appears in `status.modelDiscoveryUrl`. It reflects
the configured endpoint and path, regardless of whether a poll succeeds, and
is absent when model discovery is not configured.

A failed poll sets `status.modelDiscoveryError` to the bounded failure category
used by `grid_model_discovery_total`. A successful poll clears it. The discovery
loop owns this field separately from the provider's routing phase and clears
it when discovery is disabled. The field reports the latest poll error; the
held model set still follows its TTL independently.

The bearer token comes from `spec.auth`. A model-discovery URL must use HTTPS
when a bearer token is configured. With `auth.manual`, requests carry no
credentials, so plain HTTP remains available.

Discovery runs in its own loop, like the signals scraper, not in reconcile.
Discovered sets are held in memory per provider:

- A successful poll replaces the set and renews its deadline; an empty list is
  a valid result.
- A failed poll keeps the set; its deadline is not renewed.
- A set not renewed within the TTL expires. Absence is the staleness signal,
  so a stale set fails closed and no clocks are compared.
- A provider that is deleted or drops `modelDiscovery` loses its set on the
  next round.
- The response is rejected as a whole, never partially applied, when it:
  - is not a 2xx, or exceeds 1 MiB;
  - is not JSON with `data[].id`;
  - has a blank id, an id over 256 bytes, or an id with control characters;
  - repeats an id (duplicates are treated as malformed, not deduplicated);
  - lists more than 256 models.

| Variable | Default | Meaning |
|----------|---------|---------|
| `GRID_MODEL_DISCOVERY_INTERVAL_SECS` | `60` | time between rounds |
| `GRID_MODEL_DISCOVERY_TIMEOUT_SECS` | `5` | bound on one provider poll |
| `GRID_MODEL_DISCOVERY_TTL_SECS` | `180` | how long a set survives without a successful poll; at least interval + timeout |
| `GRID_MODEL_DISCOVERY_CONCURRENCY` | `8` | providers polled at once |

Each poll increments `grid_model_discovery_total{provider,outcome}`, where
`outcome` is one of `ok`, `credential`, `tls`, `config`, `unreachable`,
`timeout`, `http_status`, `invalid_response`. Failures are also logged.

## AgentToolProvider

Represents MCP tool servers available over the grid.

```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: AgentToolProvider
metadata:
  name: db-tools
spec:
  gridNetworkRef: production
  hostSelector: {}
  protocol: mcp
  endpoint: http://db-tools.tools:8080
  tools:
    - name: database-query
      description: "Query the database"
  auth:
    strategy: bearerToken
    secretRef:
      name: tool-token
      namespace: praxis-system
  accessPolicy:
    siteSelector:
      matchLabels:
        grid.praxis.fast/site: cluster-a
```

**Phases**: Pending → Available → Unavailable

`Degraded` is not currently reachable for this CRD: unlike
`InferenceProvider`'s metrics-scrape path, the MCP `tools/list` probe has no
partial-success state to represent — it either succeeds (`Available`) or
fails outright (`Unavailable`), mirroring `phase_and_reason_from_probe`'s and
`phase_from_matching`'s explicit design (both are tested to never emit
`Degraded`).

**Status fields**: `conditions`, `phase`, `discoveredTools` (auto-populated
from MCP `tools/list`; a failed probe preserves the previous list rather than
clearing it), `matchingSites`, `observedGeneration`. The reason for the current
phase is on the `Available` condition.

#### AgentToolProvider reason codes

These are `Available` condition reasons.

| Reason | Phase | Meaning |
|---|---|---|
| `EndpointInvalid` | Unavailable | `spec.endpoint` is not an http or https URL with a host. Reported as `Accepted=False`. |
| `GridNetworkRefInvalid` | Unavailable | `spec.gridNetworkRef` is blank. Reported as `Accepted=False`. |
| `GridNetworkNotFound` | Pending | `spec.gridNetworkRef` does not resolve to an existing `GridNetwork` yet. `Available` stays `Unknown`, since this is often just apply order. |
| `HostSelectorMissing` | Pending | `spec.hostSelector` is omitted, which places the provider on no site. |
| `UnsupportedAuthStrategy` | Unavailable | `spec.auth.strategy` is not one the controller supports yet. Reported as `Accepted=False`. |
| `CredentialSecretRefInvalid` | Unavailable | `spec.auth.secretRef` is absent or has a blank field. Reported as `Accepted=False`. |
| `CredentialSecretMissing` | Unavailable | `spec.auth.secretRef` does not resolve to an accessible Secret. |
| `CredentialSecretKeyMissing` | Unavailable | The referenced key is absent from the Secret. |
| `CredentialSecretValueInvalid` | Unavailable | The Secret value is not valid UTF-8. |
| `McpEndpointUnreachable` | Unavailable | The MCP endpoint could not be reached: transport failure, DNS error, timeout, or a blocked (SSRF-sensitive) address. |
| `McpToolsListInvalidResponse` | Unavailable | The endpoint was reached but the `tools/list` exchange failed or returned an unparseable response. |
| `McpAuthRejected` | Unavailable | The MCP server rejected the configured `spec.auth` credentials (HTTP 401/403). |
| `McpAuthTokenInvalid` | Unavailable | The resolved `spec.auth` bearer token contains characters that cannot be sent as an HTTP header value; the probe fails closed rather than proceeding unauthenticated. |
| `EndpointTlsSecretMissing` | Unavailable | A referenced TLS Secret does not exist (or the requested key is absent — see [`grid#58`](https://github.com/praxis-proxy/grid/issues/58) for a known misclassification of the latter). |
| `EndpointTlsKeyMissing` | Unavailable | The expected key exists in the Secret but its value is empty. |
| `EndpointTlsMaterialInvalid` | Unavailable | CA certificate PEM material could not be parsed. |
| `EndpointTlsIdentityMismatch` | Unavailable | Client certificate or private key PEM material could not be parsed. |

`SitesMatched` (for `Available`) and `AwaitingSiteMatch` (for `Pending`) are
telemetry-only labels for `grid_mcp_probe_total` and Events. The `Available`
condition reason is `Available` or `Pending` in those phases.

## AgentToAgentProvider

Represents A2A agents available over the grid.

```yaml
apiVersion: grid.praxis.fast/v1beta1
kind: AgentToAgentProvider
metadata:
  name: claims-agent
spec:
  gridNetworkRef: production
  hostSelector: {}
  protocol: a2a
  endpoint: http://claims-agent.agents:8080
  agentCard:
    skills: [claims-processing, document-review]
    modalities: [text]
  auth:
    strategy: mtlsOnly
  accessPolicy:
    siteSelector:
      matchLabels:
        grid.praxis.fast/site: cluster-a
```

The operator defines this type but runs no controller for it, and neither the
grid-operator chart nor `deploy/crds` installs its CRD.

**Phases**: Pending → Available → Degraded → Unavailable
