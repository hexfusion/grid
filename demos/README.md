# Grid Demonstrations

The demonstrations in this directory are deployable environments with
automated runtime proof. They complement the production architecture
documentation by making specific routing, security, and failure behavior
observable.

## Global Ingress

The [Global Ingress Demo](grid-glb-demo/README.md) exercises external client
inference through active Praxis edge gateways and Grid-selected provider
gateways. It also explains how that path differs from cluster-local workload
inference and identifies regional controls that are not yet demonstrated.

## Workload Inference

The [Workload Inference Demo](grid-workload-inference/README.md) exercises
cluster-local inference routing without global ingress. Platform workloads
submit requests through their cluster-local consumer gateway, which uses
Grid to select an eligible provider. No traffic manager and no public
endpoint are involved.

## Combined Sites

The [Combined-Site Demo](grid-combined-site/README.md) defines a compact
three-cluster workload-inference topology. West, central, and east each run a
consumer gateway, a separately secured provider gateway, and a private
inference endpoint. The directory currently records the standalone demo
contract in a validated, runnable environment.

## llm-d Pool Metrics

The [llm-d Pool Metrics Demo](grid-llmd-pool-metrics/README.md) runs two
inference pools and proves metrics-driven capacity failover. Grid scrapes
pool-level llm-d EPP telemetry, scores both candidates, publishes a routing
overlay, and moves live Praxis requests from the preferred local pool to the
healthier remote pool and back again without restarting the gateways.

## Route 53 Edge Entry

The [Route 53 Edge Entry Demo](grid-route53-edge-entry/README.md) uses
Amazon Route 53 as the complete public DNS layer for regional edge
selection. Route 53 owns global and regional DNS records, applies
weighted or latency routing among public Praxis edge gateways, performs
synthetic health checks, and withdraws unhealthy edges from DNS. After
a request reaches an edge, Grid and Praxis select an eligible private
inference provider.

## MaaS IPP lab

The [MaaS IPP lab](maas-ipp/README.md) is a single-cluster Forge environment
that brings up the stock Models-as-a-Service Kind path (Istio Gateway, Kuadrant
auth, controller-owned IPP EnvoyFilters, llm-d sim). Use it to develop and
validate MaaS dataplane changes before swapping IPP for Praxis in the
controller.

## Existing-Cluster Installation

For deploying Grid onto existing Kubernetes clusters (rather than disposable
Kind environments), see [examples/helm/existing-clusters/](../examples/helm/existing-clusters/README.md).
Two topology layouts are documented: dedicated logical edge gateways and
combined consumer/provider sites.
