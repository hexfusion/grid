# Polling Cross-Site Load Signals

A gateway routes across sites by preferring the least-loaded peer that serves a
model. To do that it needs each peer's current load. Each gateway polls its
peers' signal endpoints over mutual TLS, records the readings in a local store,
and the router orders cross-site candidates from that store.

This document covers the poll path and its failure behavior. The signal wire
format and the store are described in [Signals](signals.md). How the ordered
candidates are selected is in [Routing](routing.md) and [Scoring](scoring.md).

## The Poll Path

Polling is direct per peer, not through a relay. One poller dials one peer,
verifies that peer's Grid site certificate, and attributes the readings to that
one verified identity.

| Step | What happens |
|---|---|
| Dial | The poller opens a mutual-TLS connection to the peer's `/v1/site/signals`. Both ends present Grid site certificates. |
| Verify | The peer's certificate is checked against the Grid CA and its SPIFFE identity, then the verified identity is compared to the site the poller intended to reach. A valid Grid peer answering for a site the poller did not dial is refused. |
| Read | The exposition body is read under a byte ceiling and a time bound, so a slow or oversized peer cannot hold the poll open or exhaust memory. |
| Store | Each reading is keyed on the verified peer identity, never a value the response body carries. A body label that disagrees with the verified owner is dropped. |

The verified peer identity is the store key. The response body cannot choose
where its readings land, so one peer cannot inject readings attributed to
another.

## When a Gateway Polls

A gateway polls its peers only while its own endpoint is actively serving.

Cross-site routing is a decision this gateway makes when it receives a request
it can place elsewhere. A gateway that is not serving receives no such requests.
It has no routing decision to inform, so it holds no mutual-TLS connections to
its peers and adds no scrape load to them. Polling follows serving. It starts
when the gateway begins serving and stops when it drains.

The number of live peer connections is then proportional to the serving
gateways, not to the size of the grid. A drained or standby gateway is silent on
the peer signal endpoints.

## From Poll to Route

The poll path and the route path meet at the store, and only at the store.

The poller writes readings into the store as they arrive. Ordering runs off the
request path as a control step: it reads each candidate's recent worst load from
the store and produces a candidate list ordered least-loaded-first. The request
path reads one ordered snapshot and takes the front admitted candidate. The
request path does not read raw signals or compute load. It reads resolved order.

## Provider Readiness

Each site's operator decides whether each of its providers can serve now and
publishes the verdict as `grid_provider_ready{grid_site,grid_provider}`: 1 when
ready, 0 when not. A provider is not ready when its EPP reports zero ready
endpoints for two scrapes in a row, when no scrape has succeeded within
`staleMetricsSeconds` (half the signal TTL when unset), or when the provider is
`Unavailable`. The same verdict is the provider's `Ready` condition.

The gateway reads the latest readiness sample for each candidate when it orders
the snapshot. A candidate whose latest sample is 0 is excluded, so a site is
dropped within one poll of its operator deciding, and readmitted within one poll
of it recovering. A missing series reads as ready, so a site whose operator
predates readiness is still routed. The serving config carries the same verdict
for this site's own providers as `admission: none`.

When every candidate for a model is excluded, the gateway answers 503 with
`Retry-After`, not 404: the model exists but cannot be served now.

With the defaults of a 5 s scrape and a 5 s poll, exclusion takes at most about
15 s and rejoin about 10 s.

## Provider In-flight

Each site's operator also publishes how many requests each provider holds, as
`grid_provider_in_flight_requests{grid_site,grid_provider}`. Every input comes from the EPP's
`/metrics`, and nothing scrapes vLLM. The value is the larger of two estimates, plus the
requests the EPP's flow control holds for the pool (`llm_d_epp_flow_control_queue_size`):

- The EPP's per-endpoint `llm_d_epp_inflight_requests`, summed, taking each endpoint's
  largest count across producer instances. It needs the EPP's inflight-load-producer.
- The pool's average running plus average queued requests, times ready endpoints.

The larger, so an EPP restart that zeroes its count does not make the site look idle.
The per-endpoint count carries no pool label, so when one EPP serves more than one pool the
operator uses the pool averages alone. A site with no fresh endpoint publishes nothing,
since its averages are frozen at their last value, and the gateway reads it as unknown.

On a prefill/decode pool the EPP counts a request on its prefill and its decode
endpoint, so the value counts endpoint occupancy, up to twice the requests. Capacity
must therefore count the slots of every ready endpoint, prefill and decode alike.

Point `metricsConfig.metricsEndpoint` at the EPP Service. A pod or headless address
can reach a standby replica, which reports no series.

Capacity is the provider's per-endpoint `spec.maxRunning` times the EPP's fresh ready
endpoints, published as `grid_provider_capacity_requests`, so it shrinks when pods are lost.
With no fresh endpoint it is unpublished, and the gateway reads it as unknown.

With both known, the operator also publishes their ratio, requests held over
capacity, as `grid_provider_saturation_ratio`. Gateways choose sites by it.

## Failure Behavior

| Condition | Signal produced | Routing effect |
|---|---|---|
| No reading for a candidate | The candidate scores as maximally loaded. | It sorts after every candidate that has a reading, so a measured healthy peer is preferred over an unmeasured one. |
| Readings all stale (older than the window) | Same as no reading. | The candidate sorts last until a fresh reading arrives. |
| Peer unreachable or slow | The poll returns an error and no reading is written. | The candidate ages out of the window and then sorts last. The poll loop continues, and one unreachable peer does not wedge the others. |
| Peer presents an untrusted or mismatched certificate | The connection is refused, so no reading is written. | The peer contributes nothing to the order. |
| Peer reports `grid_provider_ready 0` | The candidate is excluded. | It takes no new requests until a later reading says 1. If every candidate is excluded, the model answers 503. |

Loss of signal degrades to "least preferred," never to "silently treated as
idle." A drained burst stays penalized until it ages out of the window rather
than snapping back to idle on the first missing sample.

## See Also

- [Signals](signals.md), the signal wire format and the store.
- [Routing](routing.md), how an ordered candidate is selected.
- [Scoring](scoring.md), how load maps to order.
- [Authentication](auth.md), the Grid mTLS peer identity layer the poll path uses.
