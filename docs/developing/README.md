# Developing Grid

Run a two-site grid on a laptop with kind: a hub and a site cluster, SPIFFE peer
trust, polled signals, and the operator's generated `grid_site_route` config.
CI runs the same script on every pull request.

## Prerequisites

- Docker or Podman, kind, kubectl, Helm, Git
- The Rust toolchain from `rust-toolchain.toml`, used to build the images and
  [Forge](forge.md)
- About 4 vCPU and 16 GB of memory

## Bring it up

```bash
git clone https://github.com/praxis-proxy/grid && cd grid
./scripts/forge.sh install
MODES=spiffe HUB_ROUTER=operator KEEP=1 scripts/e2e-hub-site.sh all
```

With rootless Podman:

```bash
CONTAINER_TOOL=podman \
KIND_CREATE_PREFIX="systemd-run --scope --user -p Delegate=yes" \
NET_PREFIX="podman unshare --rootless-netns" \
MODES=spiffe HUB_ROUTER=operator KEEP=1 scripts/e2e-hub-site.sh all
```

`all` builds the images, creates the clusters, installs both sites, and runs the
assertions. Run `up` and then `test` to split the steps. `SKIP_BUILD=1` reuses
images you already built.

## Look around

```bash
kubectl --context kind-grid-hub-site-hub  -n grid get gridsites,gridnetworks,inferenceproviders
kubectl --context kind-grid-hub-site-site -n grid get gridsites
```

## Tear down

```bash
scripts/e2e-hub-site.sh down
```

## Notes

- The Helm commands behind the script are in
  [examples/helm/hub-site](../../examples/helm/hub-site/README.md).
- On macOS, kind LoadBalancer addresses are not reachable from the host without
  extra setup.
- Membership on `main` still uses SWIM. Declared membership with SWIM off is in
  progress.

## More

- [Forge tooling](forge.md)
- [Conventions](conventions.md)
- [Type design](type-design.md)
