# GridSite

Describes a site's network membership, gateway endpoint, and trust policy.

## Ownership

| Responsibility | Component |
| --- | --- |
| Created by | Administrator, or Grid operator when site discovery is enabled |
| Spec owner / writer | Administrator (declared sites) or Grid discovery (generated sites) |
| Status writer | Grid operator network and site controllers |
| Direct consumers | Grid operator site probes, network routing, and provider selection |
| Derived consumers | Praxis gateway reads generated peer routing configuration |

## Lifecycle

- Declared sites take their spec from administrator configuration. Discovered
  sites take generated fields from live membership information.
- The network controller records discovery and advertised capabilities. The
  site controller probes remote gateways and maintains connectivity status.
- Edit the authoritative input. Discovery can reconcile generated spec fields
  again. Controllers manage status in both modes.

## References

[Fields] | [Schema] | [Site controller] | [Discovery]

[Fields]: ../crds.md#gridsite
[Schema]: ../../../crates/operator/src/crd/grid_site.rs
[Site controller]: ../../../crates/operator/src/controller/grid_site.rs
[Discovery]: ../../../crates/operator/src/controller/grid_network.rs
