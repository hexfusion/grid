# Tuning cross-site site selection

This guide covers how a grid gateway serving `gridServing` chooses a site for each
request in `grid_site_route`, what feeds that choice, and what you can change. The
routing overlay and the consumer Praxis `intelligent_route` filter, with selection
groups and scoring policies, are a separate path described in
[routing.md](routing.md).

## Site choice

Each site's operator scrapes its EPP and publishes, per provider, how many requests
the provider holds and how many it can run at once. The gateway polls every site and
computes each site's load, rho, as requests held over capacity. A site has room when
rho is below 1.

Among healthy sites with room:

- **Three or more:** the gateway draws two sites in proportion to capacity and takes the
  one with lower rho.
- **Two:** one draw, each site weighted by capacity over 1 + rho, each share kept between
  0.1 and 0.9.
- **One:** that site.

With no site that has room, the gateway draws by capacity among the healthy sites tied
on the best polled queue depth. When every healthy site has rho of at least 1.05, the
model sheds: requests get 429 with `Retry-After` until a site reaches 0.95.

A healthy site is ready and has a backend praxis reports healthy. A site that is not
ready, or whose cluster has no healthy endpoint, is chosen only when no healthy site is
left.

## Inputs

| Input | Published as | Source |
|---|---|---|
| Requests held | `grid_provider_in_flight_requests` | The larger of the EPP's `llm_d_epp_inflight_requests` summed over endpoints, and its average running plus queued requests times ready endpoints, plus requests its flow control holds (`llm_d_epp_flow_control_queue_size`) |
| Capacity | `grid_provider_capacity_requests` | `spec.maxRunning` times the EPP's fresh ready endpoints (`llm_d_epp_ready_endpoints`) |
| Load (rho) | `grid_provider_saturation_ratio` | Requests held over capacity, computed by the operator when both are known |
| Readiness | `grid_provider_ready` | 0 after two scrapes with no ready endpoints, or no successful scrape within `staleMetricsSeconds` |
| Queue depth | `llm_d_epp_average_queue_size` | The EPP, republished as is |

Every input comes from the EPP's `/metrics`. Point `metricsConfig.metricsEndpoint` at
the EPP Service, since a pod address can reach a standby replica that reports nothing.
In-flight counts need the EPP's inflight-load-producer. Without it the pool averages
stand in, and they miss requests the EPP holds.

A provider with no `maxRunning`, or no fresh endpoint, publishes no capacity. Its rho
is unknown, and it takes requests only through the queue-depth overflow.

## Latency inputs

Each operator also publishes recent latency per provider, over the last 30 seconds,
when at least 20 requests completed: `grid_provider_ttft_p50_seconds`,
`grid_provider_ttft_p90_seconds`, `grid_provider_tpot_seconds`,
`grid_provider_prefill_seconds_per_token`, and `grid_provider_error_ratio`. The gateway
reads them, but site choice does not use them: it ranks by rho alone. They show why one
site is slower than another, and the error ratio shows a failing site, which the latency
series would read as fast.

The EPP times TTFT from receiving the request, so it includes flow-control wait and
network. Prefill seconds per token is an estimate that moves with the workload mix.

## Choosing maxRunning

Set `spec.maxRunning` to the most requests one endpoint runs at once, which is the
engine's `--max-num-seqs`. The operator multiplies it by ready endpoints, so it follows
scale-up and pod loss.

An engine can run out of KV cache before it reaches max-num-seqs. vLLM logs its KV
concurrency at startup, as maximum concurrency for its configured tokens per request.
If your typical request is long, set `maxRunning` to the smaller of max-num-seqs and the
concurrency the KV cache holds at your typical request length. Too high a value makes a
KV-bound site look roomier than it is.

On a prefill/decode pool, the EPP counts a request on both its prefill and its decode
endpoint, and ready endpoints count both roles. Use the same per-endpoint value for
both roles.

## Settings

| Setting | Default | Where | Effect of changing it |
|---|---|---|---|
| `spec.maxRunning` | none | InferenceProvider | Sets capacity per endpoint. Without it, rho is unknown. |
| `metricsConfig.metricsEndpoint` | none | InferenceProvider | The EPP Service to scrape. Required for every input. |
| `metricsConfig.staleMetricsSeconds` | half the signal TTL | InferenceProvider | How long a failing scrape keeps the last readiness before the provider reads not ready. |
| `GRID_SIGNALS_SCRAPE_INTERVAL_SECS` | 5 | Operator environment | How often the operator scrapes its EPP. Faster reacts sooner and loads the EPP more. |
| `metricsListener.enabled` | false | praxis-gateway chart | Serves the decision counters on a TLS listener, with a Service and ServiceMonitor. |

The rest is fixed:

- The gateway polls each site every 5s with a 2s timeout. A load sample is fresh for
  two polls, 10s.
- Shedding starts when every healthy site holds rho of at least 1.05 and stops when one
  reaches 0.95.
- A two-site draw keeps each share between 0.1 and 0.9.
- One request weighs at most 64 sites with room.
- A shed 429 and an outage 503 each carry a Retry-After of 3 to 7 seconds.

## What to watch

- `grid_route_decisions_total{site,reason}` on the gateway's metrics listener. `routed`
  and `fallback` count by site. Refusals count under an empty site as `not_ready`,
  `no_route`, `bad_request`, or `shed`.
- `grid_route_site_score{site,cluster}`: the queue depth each candidate was ordered by.
  NaN marks an excluded or demoted candidate.
- `grid_provider_in_flight_requests`, `grid_provider_capacity_requests`,
  `grid_provider_saturation_ratio`, and `grid_provider_ready` on each site's signals
  endpoint, `/v1/site/signals`, and on each operator's `/metrics`. A hub's `/metrics`
  also carries what it polled from its peers.
- `x-grid-site` and `x-grid-backend` on every routed response.
- Operator WARN `serving config: refusing candidates whose site is not a DNS-1123 label`.

## Symptoms

| Symptom | Likely cause | Action |
|---|---|---|
| Traffic piles onto one site | No capacity published, so the queue-depth overflow herds onto the site tied on the best queue for one poll window | Set `maxRunning` on every provider and check `grid_provider_capacity_requests` appears for each |
| A site is never chosen | Not ready, or its cluster has no healthy endpoint. Its `grid_route_site_score` is NaN. | Check `grid_provider_ready` and the gateway's cluster health log line. |
| A small or slow site takes little | Expected: draws follow capacity, and two choices never pick the busiest of three | Raise its `maxRunning` only if the engine runs more |
| 429 `capacity_exhausted` under light load | rho reads high: `maxRunning` too low, or in-flight counts both P/D roles against a decode-only capacity | Compare `grid_provider_in_flight_requests` with what the engines run, and correct `maxRunning` |
| 503 `no_healthy_site` | Every site serving the model is not ready or has no route from this gateway | Check `grid_provider_ready` and the serving config candidates |
| Traffic moves between sites every few seconds | Load near capacity, read at 5s polls | Expected near full. Check capacity covers the load. |
| A peer never routes | Its site id is not a DNS-1123 label, and the operator refuses it | Look for the refused-site WARN and rename the site |

## What you cannot tune

The gateway keeps no count of its own requests. Load comes from what each site's
operator publishes, so it is as fresh as the scrape and poll, about 5 to 15 seconds. The
selection is coarse by design. It keeps each site near its capacity share and away from
full, and the site's EPP picks the endpoint within the site. No per-site weight or
threshold is settable.

