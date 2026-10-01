# Getting Started

This guide takes you from nothing to a working grid of two clusters and points
you at the document that owns each next step. It does not repeat install
commands. The example README holds them, and they change with the API.

## What a Grid Is

A grid is a set of Kubernetes clusters that route inference requests to each
other. Each cluster is a site. One site, the hub, runs the enrollment service,
which holds the Grid CA and mints a one-time invite token for each site. A site
redeems its token and receives a certificate that carries its identity. Sites
then find each other through SWIM membership gossip, and each operator probes
a peer's gateway over mutual TLS before it routes to it. Every site runs the
AGN operator, which watches the grid resources, tracks remote state, and writes
the routing state for its gateways. The gateways are Praxis processes. A
consumer gateway takes client requests and picks a provider. A provider
gateway authenticates the calling site and forwards to a local backend. The
operator is never in the request path. See the
[architecture overview](architecture/overview.md) for the full picture.

## Prerequisites

- Two Kubernetes clusters, 1.26 or later, and `kubectl` contexts for both.
- Helm 3.12 or later.
- A LoadBalancer implementation on every cluster. The enrollment, SWIM, and
  provider gateway Services are type LoadBalancer.
- A DNS name for the hub enrollment service that every site resolves.
- Peers reach each other on SWIM UDP 7946 and the site gateway port 8080.
- etcd encryption at rest on the hub, since the Grid CA key lives in a Secret.
- An inference backend on the site that serves an OpenAI-style API.

The hub-site example lists these in full, including the OpenShift variants.
Trying the grid on kind needs a container engine, `kind`, `kubectl`, and the
tools named in the script header instead.

## Choose Your Path

| Path | Use it when | Read |
|---|---|---|
| Try it locally on kind | You want a running grid without real clusters. | [hub-site topology README](../tests/e2e/topologies/grid-hub-site/README.md) |
| Install a hub and a site with Helm | You have two clusters and want the enrolled install. | [examples/helm/hub-site](../examples/helm/hub-site/README.md) |
| Add the grid to existing clusters | You run consumer and provider gateways on clusters you already operate. | [existing-cluster installation](installation/existing-clusters.md) |
| FIPS | You need the validated crypto boundary. | [FIPS support](fips.md) |

The local kind path runs [e2e-hub-site.sh](../scripts/e2e-hub-site.sh). It builds the images,
creates a hub and a site cluster with Forge, runs the hub-site commands, and
asserts on the result. `KEEP=1` leaves the clusters up so you can look around.
The multi-cluster `xtask env` flow in
[operations](architecture/operations.md#development-validation-environments)
is for operator development, not for a first install.

## The Minimal Happy Path

This is the hub-site install. Run it from the repository root. The
[hub-site README](../examples/helm/hub-site/README.md) holds the exact
commands for every step.

1. Hub enrollment. Install the `grid-enrollment` chart as release
   `grid-enrollment` in its own namespace, `grid-enrollment`. It creates the
   Grid CA and one invite token per site you list. Commands:
   [Hub](../examples/helm/hub-site/README.md#hub).
2. Hub operator. Install the `grid-operator` chart in namespace `grid` with
   enrollment on, then copy the CA bundle and the hub invite into `grid` and
   create the SWIM key. Same section.
3. Hub site and gateway. Install the `grid-site` chart, which renders the
   GridNetwork, the hub GridSite, and one GridSite per peer, then the `praxis-gateway` chart as the consumer.
   Same section.
4. Site operator. On the second cluster, install the `grid-operator` chart
   pointed at the hub seed and enrollment URL. Copy the CA bundle, the site
   invite, and the SWIM key from the hub. Commands:
   [Site](../examples/helm/hub-site/README.md#site).
5. Site and gateway. Install `grid-site` with the hub digest and your
   inference backend, then `praxis-gateway` with `gatewayConfig.role=provider`.
   Same section.
6. Pin the site on the hub. Rerun the hub `grid-site` command with the site
   leaf digest. The README's
   [Leaf Digest](../examples/helm/hub-site/README.md#leaf-digest) section
   prints it.

This install routes on the static backends listed in the gateway chart. The
operator computes the routing overlay, but the gateway does not read it yet.

## What the Charts Set for You

The grid resources are served at `grid.praxis.fast/v1beta1`. The commands rely
on these behaviors.

- Site discovery. `gridNetwork.siteDiscovery.mode` defaults to `manual`, which
  fills the GridSites you declare under `peers` from SWIM and creates none.
  `auto` also creates one GridSite per undeclared member, named by its bare
  site name. This field replaces the older auto-discover label.
- Egress TLS. A GridSite with no `spec.egress` probes with
  `siteDiscovery.defaultEgressTls`, `mutualTls` by default. A `plaintext` site
  never becomes routable and reports `PlaintextIneligible`.
- Provider placement. An InferenceProvider `hostSelector` that is omitted
  matches no site and reports `HostSelectorMissing`. An empty selector, `{}`,
  matches every site. The grid-site chart sets each keyed
  `inferenceProviders` entry to this site's `grid.praxis.fast/provider-site`
  label, so the provider is hosted here and not on peers.
- Trust material. The operator never mints a grid CA on its own. With the
  Secrets missing, the GridNetwork reports `Ready=False` with reason
  `TrustMaterialMissing`. Enrollment writes them in this install. For a
  throwaway dev cluster, the grid-operator chart value `devSelfSignedCa=true`
  mints a self-signed CA instead.
- Consumer listener port. For a gatewayRef with `consumerConfig.enabled`, the
  operator reads the listener port from the numeric `targetPort` of the
  gateway Service the gatewayRef names. When it cannot, it falls back to the
  grid-operator value `consumer.listenerPort` and reports
  `ListenerPortUnresolved`.

## Verification

Check the GridSites on each cluster. The hub should show `site-a` as Active.
The site should show the hub as Discovered, because the hub advertises no
gateway address.

```bash
kubectl -n grid get gridsites
```

Check the GridNetwork on the hub. It should be Active with at least one
connected site.

```bash
kubectl -n grid get gridnetwork
```

Send a request through the hub gateway.

```bash
kubectl -n grid port-forward service/grid-gateway 18080:8080
curl -i http://127.0.0.1:18080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"my-model","messages":[{"role":"user","content":"ping"}],"max_tokens":8}'
```

A 200 response with `x-grid-provider-site: site-a` means the request crossed
to the site and back. The `model` is the one you set on the hub gateway.

If a site stays in a phase, start with the
[GridSite lifecycle diagnostics](architecture/operations.md#gridsite-lifecycle-diagnostics)
in operations. For enrollment failures, see the
[enrollment troubleshooting list](installation/enrollment.md#troubleshooting).

## Reading Order

Read in this order.

1. [Architecture overview](architecture/overview.md): what the operator does,
   what the gateways do, and where the request path starts.
2. [Custom resources](architecture/crds.md): GridNetwork, GridSite,
   InferenceProvider, their phases, and their status.
3. [Site enrollment](installation/enrollment.md): how a site gets its identity
   and what to do when enrollment fails.
4. [Operations](architecture/operations.md): network formation, trust
   bootstrap, site lifecycle, and diagnostics.
5. [Auth and policy](architecture/auth.md): peer trust modes, SWIM key
   handling, and provider authentication.
6. [Adding an inference provider](adding-provider.md): register a model on a
   site.
7. [Routing guide](routing.md): pick a routing behavior and configure it.
8. [Routing architecture](architecture/routing.md): the overlay contract and
   how it reaches the gateway.
9. [Scoring](architecture/scoring.md), [signals](architecture/signals.md), and
   [polling](architecture/polling-metrics.md): how load reaches the ranking.
10. [Consumer config](architecture/consumer-config.md),
    [external ingress](architecture/external-ingress.md), and
    [provider draining](architecture/provider-draining.md): gateway
    configuration, edge selection, and maintenance.

For chart values, use the READMEs under `charts/`. To build and test the
code, read [development](development.md).
