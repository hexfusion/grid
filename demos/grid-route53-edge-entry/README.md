# Grid Route 53 Edge Entry Demo

A narrated demonstration in which Amazon Route 53 provides the complete
public DNS layer and sends clients to healthy regional Praxis edge
gateways. After a request reaches an edge, Grid and Praxis select an
eligible private inference provider.

## What This Demo Shows

- **Route 53 is the complete public DNS implementation.** It owns global
  and regional DNS records, applies weighted or latency routing among
  public Praxis edge gateways, performs synthetic health checks,
  withdraws unhealthy edges while another record remains healthy, and
  restores recovered edges.
- **DNS edge selection and Grid provider selection are independent
  decisions.** Route 53 selects a healthy public edge. After the request
  arrives, Grid and Praxis select an eligible private provider.
- **Only Praxis consumer/edge gateways are publicly reachable.** Provider
  gateways, inference backends, the Grid operator, metrics endpoints, and
  SWIM are never exposed through Route 53 or OpenShift Routes.
- **Health checks have known limitations.** TCP 443 checks verify only
  that the OpenShift router's ingress endpoint accepts connections. They
  cannot detect Route admission, consumer gateway readiness, overlay
  health, or inference availability.
- **Regional DNS entry does not enforce provider residency alone.** DNS
  edge membership restricts which edges answer, but Grid/Praxis
  request-time policy must also exclude out-of-region providers.

## Topology

```text
                         Amazon Route 53
              inference.grid.example.com
                    /          |          \
                   v           v           v
          logical region east     logical region west
             /          \             /          \
            v            v           v            v
        east1 edge    east2 edge   west1 edge   west2 edge
        (OpenShift    (OpenShift   (OpenShift   (OpenShift
         Route)        Route)       Route)       Route)
             \           |           |           /
              +----------+-----------+----------+
                             |
                    private Grid provider paths
                             |
                    private inference backends
```

Route 53 owns the top layer. OpenShift owns the regional edge endpoints
and their TLS termination. Grid and Praxis own the request-time provider
selection after DNS has delivered the client to an edge.

The four standalone OpenShift clusters serve as independently reachable
edge origins. Each cluster's consumer gateway represents its regional
edge. This is the demo topology — production can deploy dedicated
regional edge clusters.

### Portability

The Route 53 record model, health associations, verification sequence,
and cleanup contract are platform-neutral. The tracked OpenShift Route
template is this demo's ingress adapter, not a Grid requirement.

To use another Kubernetes distribution or cloud environment, replace the
OpenShift rendering and admission steps with that platform's supported
public ingress mechanism. Each site must still provide:

- a stable public IP address or DNS target for the Praxis edge;
- HTTPS for the global, regional, and direct-origin names;
- one independently testable origin name per edge;
- a health target reachable by Route 53 health checkers;
- response attribution identifying both the edge and serving provider;
- private connectivity from the edge to authorized provider gateways; and
- an exact, workflow-owned cleanup procedure for its ingress resources.

For example, a managed Kubernetes environment may use a cloud
`LoadBalancer` Service or an ingress controller instead of an OpenShift
Route. Put the resulting public address or canonical hostname in the
runtime mappings consumed by the Route 53 record workflow. Do not expose
provider gateways, inference backends, operator APIs, metrics endpoints,
or SWIM to satisfy the public-edge requirement.

The included record materializer emits CNAME records because OpenShift
reports a canonical router hostname. Environments with stable public IP
addresses must adapt that rendering step to emit `A` or `AAAA` records;
the verification and cleanup requirements remain the same.

## Prerequisites

- Working Grid installation with consumer gateways deployed on four
  OpenShift clusters
- AWS CLI configured with Route 53 permissions (see [IAM
  Permissions](#iam-permissions))
- Public DNS domain with an existing Route 53 hosted zone
- TLS certificate covering the global, regional, and all origin hostnames
- `kubectl`, `yq` (mikefarah/yq >= 4.18), `envsubst`, `jq`
- `dig`, `curl`, `openssl` (for verification)
- `aws` CLI (for Route 53 operations)

### IAM Permissions

The following IAM actions are required. Restrict the Route 53 actions to
the specific hosted zone ARN (`arn:aws:route53:::hostedzone/<ZONE-ID>`).

| Action | Used For |
|--------|----------|
| `sts:GetCallerIdentity` | Pre-flight identity verification |
| `route53:ListResourceRecordSets` | Baseline snapshot, cleanup verification |
| `route53:ChangeResourceRecordSets` | Create and delete DNS records |
| `route53:GetChange` | Wait for change propagation |
| `route53:CreateHealthCheck` | Create TCP 443 health checks |
| `route53:GetHealthCheck` | Inspect health check configuration |
| `route53:GetHealthCheckStatus` | Verify health check state |
| `route53:ListHealthChecks` | Audit existing health checks |
| `route53:UpdateHealthCheck` | Invert/restore for failure simulation |
| `route53:DeleteHealthCheck` | Cleanup |

Do not use `route53:*`. The minimum set above covers the full
create/verify/cleanup lifecycle for this demo.

## Inventory

Copy `inventory.example.yaml` to a private working directory and fill in
real values. Do not commit real hostnames, kubeconfig paths, AWS IDs, or
credentials.

```bash
cp inventory.example.yaml /secure/path/inventory.yaml
# edit with real values
```

### Fields

| Field | Required | Default | Description |
|-------|:--------:|---------|-------------|
| `hostedZoneId` | Yes | — | Route 53 hosted zone ID for the domain |
| `hostedZoneDomain` | Yes | — | Domain name of the hosted zone |
| `globalHostname` | Yes | — | Global entry point; all edges are eligible |
| `regionalHostname` | No | — | Regional entry point; only `regionalSites` edges |
| `regionalSites` | When regional | — | List of site names in the regional edge pool |
| `namespace` | No | `grid-system` | Namespace where consumer gateway runs |
| `serviceName` | No | `consumer-gateway` | Consumer gateway Service name |
| `servicePort` | No | `8080` | Consumer gateway Service port |
| `tlsTermination` | No | `edge` | OpenShift Route TLS termination mode |
| `recordTTL` | No | `60` | Route 53 record TTL in seconds |
| `setIdentifierPrefix` | No | `grid-edge` | Unique prefix for Route 53 set identifiers |
| `sites.<name>.context` | Yes | — | kubectl context for the cluster |
| `sites.<name>.originHostname` | Yes | — | Direct per-site hostname |
| `sites.<name>.awsRegion` | Yes | — | AWS region for latency-based routing |

The DNS CNAME target for Route 53 records is **not** in the inventory. It
is obtained from the admitted Route's
`.status.ingress[].routerCanonicalHostname` after apply.

## Hostname Contract

All records are created directly in the existing hosted zone. No child
zone or NS delegation is used.

```text
inference.grid.example.com
    Global entry point. Route 53 selects a healthy regional edge
    using weighted or latency routing policy.

inference-east.grid.example.com
    Regional entry point. Route 53 returns only edges assigned to
    the east region. Grid/Praxis request-time policy must also exclude
    out-of-region providers for this hostname to enforce residency.

east1.origin.grid.example.com
east2.origin.grid.example.com
west1.origin.grid.example.com
west2.origin.grid.example.com
    Direct origin names used for client verification and troubleshooting.
    Each is a CNAME pointing to its cluster's routerCanonicalHostname.
```

## Workflow

The operational workflow proceeds through these phases:

1. **Health checks / configuration** — render health-check configs,
   create in AWS, apply OpenShift Routes
2. **Runtime mapping capture** — record `routerCanonicalHostname` and
   `healthCheckId` per site into a private mappings file
3. **Payload materialization** — generate complete, validated CREATE and
   DELETE payloads from inventory + mappings
4. **Review** — inspect generated payloads before AWS mutation
5. **AWS mutation** — submit record batches to Route 53
6. **Verification** — prove DNS routing, inference, and health withdrawal
7. **Exact cleanup** — delete records using generated DELETE payloads in
   reverse order

### 1. Pre-flight

```bash
demos/grid-route53-edge-entry/scripts/preflight.sh \
  /secure/path/inventory.yaml \
  --tls-cert /secure/path/fullchain.pem
```

Validates: required tools, inventory structure, cluster access, consumer
gateway Service existence, TLS certificate SANs, AWS caller identity.

### 2. Baseline Snapshot

Before any AWS mutation, export the hosted zone record sets and record
the caller identity:

```bash
aws sts get-caller-identity
aws route53 list-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  > /secure/path/before/record-sets.json
```

### 3. TLS Certificate

Issue a certificate covering all hostnames. The demo uses certbot with
the Route 53 DNS plugin:

```bash
certbot certonly \
  --dns-route53 \
  --non-interactive \
  --agree-tos \
  --email operator@example.com \
  --config-dir /secure/path/tls/certbot-config \
  --work-dir /secure/path/tls/certbot-work \
  --logs-dir /secure/path/tls/certbot-logs \
  -d 'inference.grid.example.com' \
  -d 'inference-east.grid.example.com' \
  -d 'east1.origin.grid.example.com' \
  -d 'east2.origin.grid.example.com' \
  -d 'west1.origin.grid.example.com' \
  -d 'west2.origin.grid.example.com'
```

Certificate paths and private keys must remain outside the repository
with restrictive permissions.

### 4. Render Routes

```bash
demos/grid-route53-edge-entry/scripts/render-routes.sh \
  /secure/path/inventory.yaml \
  --output-dir /secure/path/rendered \
  --tls-cert /secure/path/tls/fullchain.pem \
  --tls-key /secure/path/tls/privkey.pem
```

Renders per site:
- **Origin Route** (`consumer-edge-origin-<site>`): direct hostname for
  health checks and independent verification.
- **Global Route** (`consumer-edge-global-<site>`): shared public hostname
  accepted on every cluster.
- **Regional Route** (`consumer-edge-regional-<site>`): regional hostname
  accepted only on sites in `regionalSites`.

Rendered manifests are written outside the repository with `umask 077`
because they contain TLS private key material.

### 5. Apply Routes

Apply per-site with explicit context:

```bash
for SITE in east1 east2 west1 west2; do
  kubectl --context "${SITE}-context" apply \
    -f "/secure/path/rendered/${SITE}-origin-route.yaml" \
    -f "/secure/path/rendered/${SITE}-global-route.yaml"
done

# Regional Routes (only for sites in the regional pool)
for SITE in east1 east2; do
  kubectl --context "${SITE}-context" apply \
    -f "/secure/path/rendered/${SITE}-regional-route.yaml"
done
```

### 6. Verify Route Admission and Capture Router Hostnames

```bash
demos/grid-route53-edge-entry/scripts/verify-origins.sh \
  /secure/path/inventory.yaml
```

Read the `routerCanonicalHostname` from each admitted Route — this is
the CNAME target for Route 53 records:

```bash
kubectl --context <context> -n grid-system get route consumer-edge-origin-<site> \
  -o jsonpath='{.status.ingress[0].routerCanonicalHostname}'
```

Copy the mappings example outside the repository and record each site's
`routerCanonicalHostname` now:

```bash
cp demos/grid-route53-edge-entry/mappings.example.yaml \
  /secure/path/mappings.yaml
```

The health checks use these already-resolvable ingress hostnames. They do
not depend on the demo origin records, which are created later.

### 7. Phase A: Render Health-Check Configs

Generate one TCP 443 health-check configuration per site from the
inventory and captured runtime mappings:

```bash
demos/grid-route53-edge-entry/scripts/render-health-checks.sh \
  /secure/path/inventory.yaml \
  /secure/path/mappings.yaml \
  --output-dir /secure/path/health-checks
```

Output files are directly usable with the AWS CLI:

```bash
aws route53 create-health-check \
  --caller-reference "${SET_ID_PREFIX}-<site>-$(date +%s)" \
  --health-check-config file:///secure/path/health-checks/health-check-<site>.json
```

Save the returned `HealthCheck.Id` for each site. Repeat for all sites.

> **`EnableSNI` is NOT valid for the TCP health check type.** Route 53
> rejects the API call if `EnableSNI` is included in a TCP health check
> configuration. The rendered configs omit it.

See `route53/examples/health-check.example.json` for the full
configuration reference and documented limitations.

### 8. Verify Health Checks

After creating all health checks, verify they report healthy:

```bash
aws route53 get-health-check-status --health-check-id <id>
```

All health checks must report `Healthy` before proceeding to record
creation.

### 9. Complete Runtime Mappings

Add each `healthCheckId` returned in step 7 to the mappings file that
already contains the `routerCanonicalHostname` values from step 6.

See `mappings.example.yaml` for the expected structure. Do not commit
mappings files containing real values.

### 10. Phase B: Materialize Record Payloads

Generate complete, AWS CLI-ready payloads from the inventory and runtime
mappings:

```bash
demos/grid-route53-edge-entry/scripts/materialize-records.sh \
  /secure/path/inventory.yaml \
  /secure/path/mappings.yaml \
  --output-dir /secure/path/records
```

The script validates all inputs and rejects:
- `PLACEHOLDER` values (case-insensitive)
- Angle-bracket substitutions (`<routerHostname>`)
- Empty or null values
- Duplicate `awsRegion` values (latency routing constraint)
- Duplicate `SetIdentifier` values

On success, generates:

```text
/secure/path/records/
  create/
    origin-records.json          # Origin CNAME per site
    weighted-records.json        # Weighted CNAME at global hostname
    latency-records.json         # Latency CNAME at global hostname
    regional-records.json        # Weighted CNAME at regional hostname
  delete/
    origin-records.json          # Exact matching DELETE payload
    weighted-records.json        # Exact matching DELETE payload
    latency-records.json         # Exact matching DELETE payload
    regional-records.json        # Exact matching DELETE payload
```

Every CREATE payload has an exact matching DELETE payload built from the
same resolved values. The DELETE payloads are used for cleanup.

### 11. Review Generated Payloads

Before submitting to AWS, review each CREATE payload:

```bash
jq . /secure/path/records/create/origin-records.json
jq . /secure/path/records/create/weighted-records.json
```

Verify:
- Correct hostnames and CNAME targets
- Correct health-check IDs associated with the right sites
- Weight distribution matches intent
- Latency regions match inventory
- No placeholder or template values remain

### 12. Submit Origin CNAME Records

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/create/origin-records.json
```

Save the returned change ID and wait for propagation:

```bash
aws route53 wait resource-record-sets-changed --id /change/<CHANGE_ID>
```

### 13. Submit Weighted Records

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/create/weighted-records.json
```

See `route53/examples/weighted-record.example.json` for the reference
structure. Equal weights give approximately equal distribution — results
are statistical, not exact counts.

### 14. Verify Weighted Distribution

```bash
# Authoritative (query the zone's nameserver directly)
for i in $(seq 1 20); do
  dig @<nameserver> inference.grid.example.com CNAME +short
done | sort | uniq -c | sort -rn

# Recursive
dig +short inference.grid.example.com CNAME
```

### 15. Prove Origin Endpoints Independently

```bash
demos/grid-route53-edge-entry/scripts/verify-origins.sh \
  /secure/path/inventory.yaml --test-inference
```

Each origin must independently complete TLS and serve inference.
The helper asserts HTTP success; capture and validate the environment's
edge/provider attribution separately as described in step 20.

### 16. Prove Global Inference Through Route 53

```bash
curl -s -X POST "https://inference.grid.example.com/v1/chat/completions" \
  -H 'Content-Type: application/json' \
  -d '{"model":"sim-model-v1","messages":[{"role":"user","content":"test"}]}' \
  -w '\nHTTP: %{http_code}\n'
```

The response must identify both the Route 53-selected edge and the
Grid/Praxis-selected serving provider. The exact headers or response fields
depend on the deployed Praxis configuration. Capture them with `curl -D -`
or the environment's equivalent and retain the result as evidence.

### 17. Replace Weighted Records with Latency Records

Weighted and latency routing policies cannot coexist at the same
Name+Type. Delete the weighted records first, then create latency
records:

```bash
# Delete weighted records using the generated DELETE payload
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/delete/weighted-records.json

# Create latency records
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/create/latency-records.json
```

See `route53/examples/latency-record.example.json` for the record
structure.

Latency routing selects the origin with the lowest estimated network
latency from the querying resolver to the specified AWS region. Results
are **observational**: recursive resolver location, EDNS Client Subnet
support, and DNS caching all affect Route 53's selection. Do not claim
deterministic geographic routing from a single resolver location.

### 18. Prove Regional Entry

Submit regional records at the regional hostname:

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/create/regional-records.json
```

SetIdentifiers follow the pattern `<prefix>-regional-<site>` (e.g.,
`grid-edge-regional-east1` with the default prefix).

Verify that only east edges answer:

```bash
for i in $(seq 1 20); do
  dig @<nameserver> inference-east.grid.example.com CNAME +short
done | sort | uniq -c | sort -rn
```

Regional provider restriction also requires Grid/Praxis request-time
policy. DNS edge membership alone does not prevent an edge from
forwarding to an out-of-region provider.

### 19. Prove Health Withdrawal and Restoration

#### Simulated failure (health-check inversion)

```bash
# Invert east1 health check to simulate failure
aws route53 update-health-check \
  --health-check-id <east1-health-check-id> \
  --inverted

# Verify east1 withdrawn from authoritative DNS
for i in $(seq 1 20); do
  dig @<nameserver> inference.grid.example.com CNAME +short
done | sort | uniq -c | sort -rn
# east1 should be absent from authoritative responses

# Verify inference continues through remaining edges
curl -s -X POST "https://inference.grid.example.com/v1/chat/completions" \
  -H 'Content-Type: application/json' \
  -d '{"model":"sim-model-v1","messages":[{"role":"user","content":"test"}]}' \
  -w '\nHTTP: %{http_code}\n'

# Restore east1 health check
aws route53 update-health-check \
  --health-check-id <east1-health-check-id> \
  --no-inverted
```

#### Known limitation: TCP 443 does not detect Route or gateway failure

TCP 443 checks the OpenShift router's shared ingress endpoint, not the
individual Route or consumer gateway. Deleting the Route or scaling the
consumer gateway to zero does **not** cause the health check to fail,
because the router continues accepting TCP connections on port 443.

| Failure | TCP 443 Detects? |
|---------|:----------------:|
| Cluster node unreachable | Yes |
| Ingress load balancer down | Yes |
| TLS port closed | Yes |
| Route deleted | No |
| Route not admitted | No |
| Consumer gateway crash | No |
| Consumer gateway scaled to 0 | No |
| Overlay mesh missing | No |
| Provider unavailable | No |
| Credential/auth failure | No |

For this reason, the failure injection proof uses health-check inversion,
which simulates a failure that the TCP health check **can** detect. The
demo must not claim gateway-aware failover from a TCP check that only
reaches a shared OpenShift router.

### 20. Narration Output

The narrated proof distinguishes DNS edge selection from Grid provider
selection:

```text
DNS name:                 inference.grid.example.com
Route 53 policy:          weighted (or latency)
Resolver/client location: us-east
Selected edge:            east1
Edge health:              healthy
Grid-selected provider:   <provider-name>
Inference status:         HTTP 200
Attribution:              edge=east1 provider=<provider-name>
```

Verify attribution identifies both:
- the Route 53-selected edge; and
- the Grid/Praxis-selected serving provider.

The supplied verification scripts require a successful inference response
but do not interpret environment-specific attribution headers. The narrated
demo is incomplete unless the recorded response proves both identities.

### 21. DNS Verification

```bash
demos/grid-route53-edge-entry/scripts/verify-dns.sh \
  /secure/path/inventory.yaml \
  --nameserver <authoritative-ns> \
  --test-inference
```

## Cleanup

Cleanup uses the generated DELETE payloads from step 10 in reverse
dependency order. Every created AWS object is removed by its exact
identity.

### Step 1: Delete Latency or Weighted Records

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/delete/latency-records.json
```

Or, if weighted records are active:

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/delete/weighted-records.json
```

### Step 2: Delete Regional Records

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/delete/regional-records.json
```

### Step 3: Delete Origin CNAME Records

```bash
aws route53 change-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  --change-batch file:///secure/path/records/delete/origin-records.json
```

### Step 4: Delete Health Checks

```bash
aws route53 delete-health-check --health-check-id <east1-id>
aws route53 delete-health-check --health-check-id <east2-id>
aws route53 delete-health-check --health-check-id <west1-id>
aws route53 delete-health-check --health-check-id <west2-id>
```

### Step 5: Delete OpenShift Routes

```bash
demos/grid-route53-edge-entry/scripts/uninstall-routes.sh \
  /secure/path/inventory.yaml
```

### Step 6: Verify Baseline Restored

```bash
aws route53 list-resource-record-sets \
  --hosted-zone-id "$HOSTED_ZONE_ID" \
  > /secure/path/after/record-sets.json

diff /secure/path/before/record-sets.json /secure/path/after/record-sets.json
```

The record sets should match the pre-run baseline. Only the apex NS and
SOA records (and any pre-existing records unrelated to this demo) should
remain.

### Cold Rerun

After cleanup, the complete workflow can be repeated from an empty
workflow-owned state:

1. Verify the baseline is clean (no demo-owned records or health checks).
2. Re-run the pre-flight, rendering, apply, Phase A, Phase B, Route 53
   record submission, and verification steps.
3. All new resource IDs (change IDs, health-check IDs) are fresh.

## DNS Behavior

### Resolver Caching and TTL

Route 53 records use a configurable TTL (default 60 seconds). Recursive
resolvers **may** cache beyond the configured TTL. A low TTL does not
guarantee that all resolvers update within that period.

Existing HTTP connections do not re-resolve DNS. A client that holds
an open connection to a withdrawn edge will continue using that
connection until it closes or times out. DNS failover applies to new
DNS resolutions, not existing TCP sessions.

DNS propagation timing depends on:
- Route 53 change submission to `INSYNC` status;
- authoritative nameserver answer update;
- recursive resolver cache expiry; and
- client DNS resolution behavior.

### All Records Unhealthy

Route 53 does not return `NXDOMAIN` or an empty answer merely because
every record in a routing-policy group is unhealthy. If all records are
unhealthy, Route 53 treats them as eligible again and answers according
to the configured routing policy.

Consequently, health checks withdraw an unhealthy edge only while the
record group contains another healthy answer. A regional entry point
cannot use Route 53 health state alone as a fail-closed residency or
availability boundary. Praxis and Grid policy must still reject an
unauthorized or out-of-region provider choice, and the edge must return a
controlled error when no permitted provider is available.

### Latency Routing

Route 53 latency-based routing selects the origin with the lowest
estimated network latency from the querying resolver (not the client) to
the specified AWS region. It is observational, not deterministic:

- The resolver location determines selection, not the client location.
- EDNS Client Subnet (ECS) may improve accuracy when supported.
- DNS caching at any layer can return a previous selection.
- A single test from one resolver does not prove geographic routing.

Prove latency routing by querying from resolvers in different AWS
regions and observing selection trends. Do not claim that every request
from a given location will always reach the nearest edge.

### Route 53 Does Not Use

Route 53 health checks and routing policies do not consume GPU metrics,
model availability, queue depth, KV-cache utilization, provider state,
or any Grid telemetry signal. Route 53 selects a healthy edge endpoint.
Grid and Praxis select the inference provider after the request reaches
the edge.

## Security and Non-Claims

### What This Demo Does Not Claim

- Route 53 does not provide gateway-aware failover with TCP 443 checks.
- Route 53 does not select inference providers, models, or GPU backends.
- DNS alone does not enforce data residency or request authorization.
- When every record in a policy group is unhealthy, Route 53 can return
  those records rather than failing closed.
- Low TTL does not guarantee immediate resolver convergence.
- Existing connections are not migrated by DNS failover.
- Latency routing is observational, not deterministic geographic routing.

### Security Boundaries

- Only Praxis consumer/edge gateways are publicly reachable through
  Route 53 and OpenShift Routes.
- Provider gateways, inference backends, the Grid operator, metrics
  endpoints, and SWIM endpoints are not publicly accessible.
- Private keys, certificates, AWS identifiers, kubeconfigs, and rendered
  Routes must remain outside the repository with restrictive permissions.
- Tracked example files use reserved `example.com` names only.
- Do not put AWS credentials, account IDs, hosted-zone IDs, TLS keys, or
  real domain values in tracked files.

### Cleanup Safety

- Never delete the hosted zone itself.
- Never use broad record deletion.
- Delete only records with the workflow's set identifier prefix.
- Save every Route 53 change ID and health-check ID before proceeding.
- Verify against the baseline snapshot after cleanup.

## Validation Scope

### 1. Repository and Static Validation (Performed)

The tracked demo artifacts pass the following static checks:

- `bash -n` syntax validation for all shell scripts
- ShellCheck analysis (SC2034, SC2086, and other common warnings resolved)
- JSON validation for all `.json` files (`jq . < file`)
- YAML validation for `inventory.example.yaml` and
  `mappings.example.yaml`
- Repository-wide `make fmt`, `make lint`, `make test`, `make doc`
- `git diff --check` for whitespace errors
- No project-specific domain names, AWS account IDs, hosted-zone IDs, or
  deployment-specific values in tracked files
- All tracked hostnames use reserved `example.com` names

### 2. Generated Payload and Schema Validation (Performed)

`materialize-records.sh` validates all inputs before generating payloads:

- JSON parse validity of all generated payloads (`jq . < file`)
- PLACEHOLDER rejection (case-insensitive)
- Angle-bracket substitution rejection (`<...>`)
- Empty/null value rejection
- Duplicate `awsRegion` rejection for latency records
- Duplicate `SetIdentifier` rejection
- AWS HealthCheckConfig field validity (no `EnableSNI` for TCP type)
- CREATE/DELETE payload identity equivalence (verified by
  `test-materialize.sh`)

The generated payloads contain real runtime values from a validated
mappings file. No placeholder substitution workflow is required.

### 3. Four-SNO Runtime Validation (Requires Execution)

Runtime validation against four independently reachable SNO clusters
proves the operational workflow. This requires running the narrated
demo script against live clusters and AWS, and recording evidence under
the private runtime workspace. It is **not** proven by static or
payload validation alone.

When performed, four-SNO runtime validation covers:

- Pre-flight checks against live clusters
- Route rendering, apply, and admission on each cluster
- Live `routerCanonicalHostname` capture from Route status
- Origin endpoint TLS and inference independence
- Route 53 health-check creation and status
- Weighted distribution (authoritative and recursive DNS)
- Health-check inversion withdrawal and restoration
- `verify-origins.sh` and `verify-dns.sh` against live state
- Exact cleanup and baseline restoration

Runtime evidence (caller identity, record-set baselines, Route
admissions, health-check IDs, weighted/latency observations, withdrawal
proof, provider attribution, and cleanup confirmation) must be captured
in the private runtime workspace, not in the repository. Do not claim
runtime validation without this evidence.

### 4. Geographic Multi-Region Validation (Not Performed)

The following require geographically distributed clusters and resolvers:

- Latency-based routing selecting different edges from different
  resolver locations
- Cross-region failover with independent failure domains
- Regional entry point restricting DNS answers to a geographic subset
  observable from multiple resolver locations
- Provider residency enforcement through combined DNS edge membership
  and Grid/Praxis request-time policy from distinct regions

The four-SNO environment can prove per-site mechanics (Route admission,
health checking, weighted distribution, withdrawal/restoration) but
does not constitute proof of geographic routing or independent failure
domain behavior across AWS regions.

## Troubleshooting

**Route not admitted**: Check `kubectl get route <name> -o yaml` for
admission conditions. Common causes: hostname conflict with another
Route, missing TLS fields for edge termination. Redact `.spec.tls`
before sharing.

**DNS not resolving after record creation**: Wait for `INSYNC` status.
Verify the CNAME target matches `routerCanonicalHostname` from Route
status, not an assumed ingress domain.

**Health check reports healthy after Route deletion**: Expected. TCP 443
checks the shared OpenShift router endpoint, which continues accepting
connections regardless of individual Route state. This is a documented
limitation of TCP health checks.

**Inference returns non-200 through global hostname**: Verify the consumer
gateway is running, the overlay mesh is healthy, and the origin is
independently reachable. Route 53 Routes only expose the consumer
gateway — they do not affect Grid's internal routing or admission.

**Latency routing returns unexpected edge**: The selection depends on the
querying resolver's location, not the client's location. EDNS Client
Subnet, caching, and resolver path all affect results. This is
observational behavior, not a bug.

**EnableSNI rejected by Route 53 API**: `EnableSNI` is not valid for TCP
health check types. Remove it from the health check configuration. SNI
is only valid for HTTPS and HTTPS_STR health check types.

**Weighted distribution uneven in small sample**: Weighted routing is
statistical. A 20-query sample may not show exact 25/25/25/25
distribution. Larger sample sizes produce more even distribution.

**Materialize rejects valid mappings**: Check for trailing whitespace,
invisible Unicode characters, or YAML quoting issues. The validation
gate is strict by design — any value matching the rejection patterns
(PLACEHOLDER, angle brackets, empty string, null) is fatal.

## Expected Evidence Artifacts

The runtime evidence directory (outside the repository) should contain:

```text
/secure/path/route53-edge-entry-runtime/
  before/
    record-sets.json           # Pre-run Route 53 baseline
    caller-identity.json       # AWS caller identity
  tls/
    certbot-config/            # Certificate material (mode 0700)
  rendered/
    *-origin-route.yaml        # Rendered Route manifests (mode 0600)
    *-global-route.yaml
    *-regional-route.yaml
  health-checks/
    health-check-*.json        # Phase A rendered configs
  records/
    create/                    # Phase B CREATE payloads
      origin-records.json
      weighted-records.json
      latency-records.json
      regional-records.json
    delete/                    # Phase B DELETE payloads (exact match)
      origin-records.json
      weighted-records.json
      latency-records.json
      regional-records.json
  evidence/
    origin-verification.txt    # verify-origins.sh output
    dns-verification.txt       # verify-dns.sh output
    weighted-distribution.txt  # Authoritative weighted sample
    latency-selection.txt      # Latency routing observations
    health-withdrawal.txt      # Withdrawal and restoration proof
    inference-attribution.txt  # Edge + provider attribution
  cleanup/
    health-check-ids.txt       # Health-check IDs for deletion
    change-ids.txt             # Route 53 change IDs
  after/
    record-sets.json           # Post-cleanup baseline comparison
```

## File Layout

```text
demos/grid-route53-edge-entry/
  README.md                          # this file
  inventory.example.yaml             # example inventory (example.com names)
  mappings.example.yaml              # example runtime mappings (zeroed UUIDs)
  openshift/
    consumer-route.yaml.tpl          # OpenShift Route template
  route53/
    examples/
      health-check.example.json      # TCP 443 health check reference
      weighted-record.example.json   # weighted CNAME reference
      latency-record.example.json    # latency-based CNAME reference
  scripts/
    preflight.sh                     # pre-apply validation
    render-routes.sh                 # render Route manifests
    render-health-checks.sh          # Phase A: health-check configs
    materialize-records.sh           # Phase B: validated record payloads
    test-materialize.sh              # tests for Phase A and Phase B
    verify-origins.sh                # post-apply Route verification
    verify-dns.sh                    # Route 53 DNS verification
    uninstall-routes.sh              # exact-name Route cleanup
```

## Relationship To Other Demos

- `grid-glb-demo` proves external client traffic through a GTM emulator
  and logical edge gateways in a disposable Kind environment. The Route
  53 edge entry demo replaces the local GTM emulator with production
  Amazon Route 53 DNS on existing OpenShift clusters.
- `grid-workload-inference` proves cluster-local inference routing without
  global ingress.
- `grid-combined-site` proves the compact three-cluster form where every
  site contains both consumer and provider roles.
- `grid-llmd-pool-metrics` proves metrics-driven capacity failover
  between two inference pools.
