# Cache-Aware Load Guard Plan

## Goal

Adjust the router selection order so prefix-cache locality is tried first, but
only inside a strict load and pressure guard. If there is no cache match, or the
matched route is too pressured compared with the least-loaded route, the router
falls back to load balancing.

The intended order is:

```text
selectable routes
  -> cache prefix match
  -> matched-route pressure/load guard
  -> cache route if acceptable
  -> least-loaded route otherwise
```

This keeps the primary production goal as load-safe routing while improving
prefix-cache reuse when the matched route is still healthy.

## Current Issue

The current implementation computes global imbalance before cache matching. If
any selectable route set is considered imbalanced, it skips cache-aware
selection entirely and routes to the least-loaded route.

That is too coarse. Global imbalance does not prove that the cache-matched route
is the pressured route. A request may match a low-load route while a different
route creates the global imbalance.

## Desired Behavior

| Scenario | Expected route reason |
| --- | --- |
| Cache match exists and matched route is load-safe | `cache` |
| Cache match exists but matched route has waiting, full tier, stale metrics disadvantage, or too much load over the least route | `least_running` with cache rejection counted |
| No cache match | `least_running` |
| No routing text | `no_text` |
| Only one selectable route | `single` or `least_running` when other configured routes were filtered out |

## Non-Goals

- Do not change the proof chain.
- Do not change upstream verification.
- Do not treat router cache affinity as a real backend `cache_hit` claim.
- Do not add per-request billing fields.
- Do not make traffic concentration the primary policy.
- Do not change production route membership except during the required
  drain-safe update flow.

## Implementation

1. Remove the global imbalance fast path that skips cache-aware selection for
   all requests.
2. Always attempt cache-aware selection when routing text exists.
3. Keep `cache_route_is_acceptable(cache_route, least_route, ...)` as the load
   and pressure guard.
4. Preserve the existing fallback to `least_loaded`.
5. Preserve `cache_rejected_by_pressure` accounting when a matched route is
   rejected by the guard.
6. Add a regression test where a third route creates global imbalance while the
   cache-matched route is still the least-loaded route. The router must select
   the matched route.
7. Add a regression test where the cache-matched route is materially more loaded
   than the least-loaded route. The router must reject cache affinity and select
   the least-loaded route.

## Test Plan

Source tests:

```text
cargo test middleware::router
cargo test middleware_completion
```

Review checks:

```text
git diff --check
git diff --stat
```

Remote simulation:

- Build the image on the remote builder.
- Run deterministic middleware tests inside the builder environment.
- Run a small synthetic route-selection simulation covering:
  - no cache match,
  - cache match on low-load route while another route is high-load,
  - cache match rejected by high matched-route pressure,
  - disabled or stale route excluded,
  - basic and premium tier pressure handling.

Deployment gates:

1. Push source before publishing the image.
2. Publish an image whose source commit matches GitHub.
3. For each router CVM, snapshot enabled upstreams first.
4. Disable originally enabled upstreams and wait until all observed Router/PIG
   running and waiting counts reach zero.
5. Deploy the new image.
6. Verify `/health`, authenticated `/v1/models`, `/v1/upstream-status`, and
   `/v1/admin/router`.
7. Restore exactly the upstreams that were enabled before the update.
8. Confirm route selections increase normally and no unexpected 5xx, 429 loop,
   or metrics parsing regression appears.

## Target Routers

```text
a238a0d9-86e1-4bf5-8dc6-ea12da505b55
a15a7b4e-27f8-4215-b92c-0cdccb1e71e0
b3c75644-aa59-4811-a2c2-9d9317f5bc18
```
