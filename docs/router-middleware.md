# Router Middleware

This fork adds one in-process router middleware to Private AI Gateway. The
middleware is intentionally narrow: one public model, many configured upstream
routes, cache affinity when it is safe, and PIG pressure awareness when a route
is busy.

It is not a standalone vLLM Router, an external adapter, or a new verification
authority. Private AI Gateway still owns attestation, upstream verification,
channel binding, forwarding, receipt finalization, and all public ACI evidence.
The middleware only orders candidate routes before the verified backend forward.

## Request Path

```text
client
  -> PAG frontend
  -> router middleware
  -> PAG verified backend
  -> selected PIG upstream
  -> vLLM or SGLang
```

The middleware runs inside the same process and attested workload as the rest of
the gateway. Plain requests are visible to it after TLS termination, and ACI
E2EE requests are visible after gateway-side decryption. That is why the
middleware source and config are part of the same audit boundary as the gateway.

## Upstream Source

Routes come from the live upstream config at `<state_dir>/upstreams.json`.
Operators manage that file through the existing authenticated APIs:

```text
GET /v1/admin/upstreams
PUT /v1/admin/upstreams
PATCH /v1/admin/upstreams/{name}
```

No route list is compiled into the binary. No external router control API is
required. A production deployment can boot with an empty seed and add PIG-backed
upstreams later without restarting the gateway.

Upstreams with `"enabled": false` stay in the admin-visible config but are not
route candidates. They are also skipped by PIG metrics polling and background
upstream verification, which avoids repeated noise while a node is known to be
down. Use `PATCH /v1/admin/upstreams/{name}` to disable or re-enable one node
without losing its stored config. `PUT /v1/admin/upstreams` is full replacement
and removes any node omitted from the submitted array.

In middleware mode, every configured upstream should expose the same public
model id. The selected backend route id has this form:

```text
<upstream name>:<public model id>
```

## Selection Algorithm

For each request, the router:

1. Confirms that the requested `model` equals the configured public model.
2. Builds candidates from the current upstream config.
3. Reads local route state: in-flight count, processed count, and the bounded
   radix-tree cache index used for prefix affinity.
4. Reads the latest PIG metrics sample for each upstream when metrics polling is
   enabled and the sample is fresh.
5. Classifies pressure.
6. Selects the best first candidate and returns the rest as fallback candidates
   ordered by lower effective load.

PIG pressure always wins over cache affinity. A warmed-prefix route is preferred
only when it is not waiting, not full, and not meaningfully more loaded than a
healthier route.

When routes are equally idle and no cache match exists, the router uses the
processed counter as a cold-traffic tie breaker so new traffic spreads across
nodes over time.

## PIG Metrics

The router polls each upstream's metrics endpoint concurrently. By default:

```text
metrics_path = /v1/metrics
metrics_poll_ms = 1000
metrics_timeout_ms = 800
metrics_stale_ms = 3000
```

The upstream bearer token from the route config is used for metrics auth. Admin
snapshots redact secrets and never expose upstream tokens.

The router currently uses these PIG metrics when present:

```text
pig_dynamic_observed_running
pig_dynamic_observed_waiting
pig_dynamic_global_limit
pig_tier_basic_limit
pig_tier_inflight{tier="basic"}
pig_tier_inflight{tier="premium"}
```

If a metrics sample is missing, failed, or stale, the route stays usable. The
router falls back to gateway-local in-flight counters instead of blocking
traffic only because observability is temporarily unavailable.

## Basic And Premium

The router does not trust caller-supplied `x-user-tier` by default.

```json
{
  "middleware": {
    "trusted_user_tier_header": false
  }
}
```

With the default, every public request is routed as `basic`, and any inbound
`x-user-tier` header is stripped before forwarding to PIG.

Only set `trusted_user_tier_header=true` behind a trusted front door that strips
or sets the header. In that mode:

- `basic` traffic avoids routes whose global limit or basic-tier limit is full.
- `premium` traffic avoids global-full routes, but does not treat a basic-full
  route as blocked. This lets reserved premium capacity remain useful.
- The trusted tier value is forwarded to PIG so PIG can enforce the same tier
  accounting.

## Cache Affinity

The router stores bounded routing-text records in a process-local radix tree
per public model. Each tree node tracks the most recent route that reached that
prefix, so a new request can find a warmed-prefix candidate without scanning all
previous records. The route is preferred only when the matched-prefix rate
reaches `cache_threshold` and the PIG pressure gate says that route is still
acceptable.

The index is intentionally limited:

- It is process-local and lost on restart.
- It is not persisted.
- It is not exposed by admin APIs.
- Each route keeps at most `max_history_per_route` routing-text records.
- Each stored routing text is capped internally; the cap is visible in
  `/v1/admin/router` as `routing_text_max_chars`.
- Disabled or removed routes are pruned from the model's cache index before
  selection, so stale cache affinity cannot route to an inactive upstream.

Cache affinity is an optimization, not a proof. Receipts still prove the
selected route and upstream verification facts, not a cache-hit claim.

## Failure Behavior

The middleware fails closed only when it cannot select a valid route for the
requested public model. When a candidate route fails during the verified backend
forward, the backend may try the remaining middleware-ordered candidates before
finalizing the response.

When every candidate fails without a relayable upstream HTTP response, the
middleware returns one aggregate client error but keeps the full attempt chain
internally for structured `request_outcome` logs. When the chain ends in an
upstream HTTP response, including an all-429 chain, the gateway relays the
terminal upstream status after normal response classification.

Streaming response errors are handled at the body boundary. The gateway logs a
`stream_abort` warning and ends the body normally, rather than surfacing a body
error to Hyper and causing a client-visible connection reset.

If all candidates are unavailable or PIG rejects because no capacity exists, the
client sees an OpenAI/vLLM-shaped error response. The middleware should not mask
real backend crashes as client errors.

## Admin Snapshot

When `admin_token` is configured, operators can inspect the router:

```text
GET /v1/admin/router
```

The snapshot includes:

- Public model and middleware config.
- Upstream config digest.
- Per-route local running and processed counters.
- Cache-selection and load-selection counters.
- Cache index type, aggregate index counters, and per-route cache record counts.
- Redacted PIG metrics status, including sample age and parse errors.

The snapshot is operational state only. It is not part of the ACI proof chain.

Downstream gateways that only need a coarse capacity signal can call
`GET /v1/upstream-status` with API bearer auth. The response is one plain-text
integer: `0` green, `1` yellow, `2` red, `3` unknown. It does not include route
names, reasons, limits, or per-node counters.

## Security Boundary

The route selected by middleware is committed into the receipt as middleware and
backend events, but verification facts always come from the backend:

```text
middleware.forwarded
route.selected
request.forwarded
upstream.verified
response.received
response.returned
```

Middleware cannot forge `upstream.verified`; the backend verifies or refreshes
the selected upstream lease and enforces the verified channel binding before
sending request bytes.
