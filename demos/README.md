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

## Existing-Cluster Installation

For deploying Grid onto existing Kubernetes clusters (rather than disposable
Kind environments), see [examples/helm/existing-clusters/](../examples/helm/existing-clusters/README.md).
Two topology layouts are documented: dedicated logical edge gateways and
combined consumer/provider sites.
