# grid-glb-demo — Internal E2E Topology

Internal test fixture for the Grid global-ingress E2E scenario.

## xtask command

```console
cargo xtask env run-grid-glb-demo \
  --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml \
  --quick --teardown
```

## What this tests

- Five-cluster global-ingress topology (2 edges, 2 providers, 1 GTM emulator)
- SWIM discovery and overlay convergence
- Active/active routing through the local GTM emulator
- Secure provider boundary (mTLS, peer identity, credential replacement)
- Edge withdrawal and recovery
- Hot-reload failure safety

## Public quickstarts

User-facing Grid demos with full documentation are maintained in the
[Praxis demos repository](https://github.com/praxis-proxy/demos).
