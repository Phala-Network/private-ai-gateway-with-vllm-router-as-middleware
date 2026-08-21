# Router Middleware

This fork adds one in-process router middleware to Private AI Gateway. The
middleware is intentionally narrow: one public model, many configured upstream
routes, cache affinity when it is safe, and PIG pressure awareness when a route
is busy.

It is not a standalone vLLM Router, an external adapter, or a new verification
authority. Private AI Gateway still owns attestation, upstream verification,
channel binding, forwarding, receipt finalization, and all public ACI evidence.
The middleware only orders candidate routes before the verified backend forward.
It reads request JSON for `model`, routing text, and tier selection, but it does
not rewrite, rebuild, or reserialize the request body.

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
the gateway. In the Phala router deployment, user-facing E2EE terminates at the
downstream PAG before it calls this router. Router middleware does not
participate in that E2EE data path; its security boundary is the ACI service
surface exposed to the downstream PAG plus phala-direct verification for
selected upstream nodes. It treats the downstream PAG's request body as the
already-cleartext routing input and never enters inherited PAG E2EE
compatibility paths.

When another downstream PAG calls this router, the downstream side should verify
the router as an `aci-service`. This router then verifies selected model nodes
with the same `phala-direct` upstream verifier used by PAG. The request bytes
received from the downstream PAG are the bytes forwarded to the selected node.

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
5. Classifies pressure and removes routes that are not selectable for the
   request tier.
6. Attempts a prefix-cache match when routing text is present.
7. Accepts the matched route only if it is not waiting, not full, and not
   meaningfully more loaded than the least-loaded route.
8. Falls back to the least-loaded route when no prefix match exists or the
   matched route fails the load guard.
9. Returns the rest as fallback candidates ordered by lower effective load.
10. Commits the routing text to the cache index only after the actual serving
    route succeeds. A buffered response must be a valid upstream 2xx JSON body;
    a streaming response must emit its first valid model-data SSE event.

The request body sent to every ordered candidate is the exact cleartext byte
sequence received by middleware from the downstream PAG. Any JSON normalization
such as streaming usage injection, provider field stripping, reasoning parameter
mapping, or tool-call cleanup must happen in the downstream PAG before it calls
this router.

PIG pressure always wins over cache affinity. The router checks cache affinity
before load fallback, but the matched route must still pass the load guard
against the current least-loaded route.

When no cache match exists, the router uses lower effective running count and
then processed count as cold-traffic tie breakers so new traffic spreads across
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
- Selection alone never writes the index. Upstream 429/5xx, verification or
  transport failure, all-candidate failure, malformed success responses, and
  client cancellation before the first valid stream event do not create a
  record. When failover succeeds, only the final serving route is recorded.

Cache affinity is an optimization, not a proof. Receipts still prove the
selected route and upstream verification facts, not a cache-hit claim.

`GET /v1/metrics` includes low-cardinality Router outcome metrics alongside the
gateway metrics. Important series are:

```text
router_cache_affinity_considered_total{route}
router_cache_affinity_selected_total{route}
router_cache_affinity_success_total{route}
router_cache_affinity_retarget_total{from_route,to_route}
router_cache_affinity_rejected_total{reason}
router_cache_record_committed_total{route}
router_cache_record_skipped_total{reason}
router_cache_match_rate_bucket{route,le}
router_cache_match_chars_bucket{route,le}
router_cache_prompt_tokens_total{route,selection_reason}
router_cache_cached_tokens_total{route,selection_reason}
router_cache_usage_skipped_total{reason}
```

Route names are bounded by the configured upstream set, and `reason` and
`selection_reason` use fixed enums. No prompt, message, tool schema, request id,
user id, or session id is placed in a Prometheus label. The token counters are
populated only when the upstream reports both prompt-token and cached-token
details; missing details increment `router_cache_usage_skipped_total` instead
of being treated as zero cache reuse.

`selected_by_cache` remains a request-level Router decision counter. Actual
cache effectiveness must be calculated from reported cached prompt tokens and
compared with TTFT; the two measurements are not interchangeable.

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

An unknown or unroutable public model is a `404 model_not_found`, not a malformed
request. A provider-side `404` for one selected candidate is treated as a
catalog miss for that provider and can fail over to another candidate; request
body errors such as `400` and `422` stay terminal.

Streaming response errors are handled at the body boundary. The gateway logs a
`stream_abort` warning and ends the body normally, rather than surfacing a body
error to Hyper and causing a client-visible connection reset.

If all configured candidates are unavailable or PIG rejects because no capacity
exists, the client sees an OpenAI/vLLM-shaped capacity error, normally `429`.
The middleware should not mask real backend crashes as client errors.

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

For transparent routing, `middleware.forwarded.body_hash` and
`request.forwarded.body_hash` are expected to match `request.received.body_hash`
for successful upstream attempts. A difference means some configured backend
adapter or non-router path rewrote the request and should be audited separately.
