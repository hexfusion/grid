# Topology B: Combined Consumer and Provider Sites

Every cluster runs both a consumer gateway and a provider gateway as separate
Deployments with separate Services, TLS identities, and Secret mounts.

```text
east-a cluster                         east-b cluster
  Grid operator                          Grid operator
  consumer gateway (port 8080)           consumer gateway (port 8080)
  provider gateway (port 8443)           provider gateway (port 8443)
  inference backend                      inference backend

west-a cluster                         west-b cluster
  Grid operator                          Grid operator
  consumer gateway (port 8080)           consumer gateway (port 8080)
  provider gateway (port 8443)           provider gateway (port 8443)
  inference backend                      inference backend
```

## Security boundary

Provider credentials are mounted only in the provider gateway Deployment.
Consumer and provider gateways use separate TLS Secrets and separate
ConfigMaps. Colocation on the same cluster does not collapse the trust
boundary — Kubernetes RBAC and separate ServiceAccount mounts enforce
isolation.

## Values files

Each site has three values files:

| File | Helm chart | Description |
|------|-----------|-------------|
| `<site>-operator.yaml` | `grid-operator` | SWIM identity, seeds, consumer gateway discovery |
| `<site>-consumer-gateway.yaml` | `praxis-gateway` | Consumer role, overlay, port 8080 |
| `<site>-provider-gateway.yaml` | `praxis-gateway` | Provider role, credentials, port 8443 |

The operator discovers the consumer gateway for routing overlay
delivery. The provider gateway is a separate Helm release on the
same cluster with its own Service and Secret mounts.

## Installation

Use the shared installer with `topology: combined-site` in your inventory:

```bash
../scripts/install.sh inventory.yaml
```

Or install manually per site:

```bash
# 1. Grid operator
helm upgrade --install grid-operator ../../../charts/grid-operator \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system --create-namespace \
  --values values/east-a-operator.yaml

# 2. Consumer gateway
helm upgrade --install consumer-gateway ../../../charts/praxis-gateway \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system \
  --values values/east-a-consumer-gateway.yaml

# 3. Provider gateway
helm upgrade --install provider-gateway ../../../charts/praxis-gateway \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system \
  --values values/east-a-provider-gateway.yaml
```

Repeat for each site.

## Compared to dedicated-edge topology

| Aspect | Dedicated edge | Combined site |
|--------|---------------|---------------|
| Clusters | 4 (2 consumer, 2 provider) | 4 (each runs both roles) |
| Credential boundary | Cluster boundary | Deployment/Secret boundary |
| Independent scaling | Consumer and provider scale separately | Share cluster resources |
| Upgrade isolation | Full blast-radius separation | Rolling upgrades affect both roles |

Choose combined-site when cluster count is constrained. Choose dedicated-edge
when you need full blast-radius isolation between consumer and provider roles.
