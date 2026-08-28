# PIG Pressure Passthrough Plan

## Goal

Fix the Router plus PIG feedback loop:

1. PIG learns a low cap.
2. Router treats that low cap or full PIG metrics as a hard pre-reject.
3. PIG no longer observes real demand.
4. PIG stays at the low cap and cannot recover.

The Router must keep hard routing protection, but PIG pressure must be a soft
signal. PIG remains the final admission authority.

## Safety Boundary

Router-generated 429 is allowed only when there is no hard-eligible upstream for
the requested public model:

- no configured upstream;
- no enabled upstream;
- requested model does not match the Router public model;
- the upstream list cannot provide any route candidate.

Router must not generate a 429 only because all PIG metrics are pressured:

- `observed_waiting > 0`;
- tier or global fullness is at or above 100%;
- request-aware capacity is currently protected;
- the route looks temporarily saturated by local reservations or unreconciled
  dispatches.

In those cases Router forwards to a selected upstream and lets PIG return the
real admission decision. This keeps the learning loop observable.

## Routing Design

1. Keep the current fast path:
   - filter enabled upstreams for the requested public model;
   - among routes without soft PIG pressure, use the existing cache-aware and
     load-aware selector.

2. Add a pressure passthrough fallback:
   - if hard-eligible routes exist but every route is soft-blocked by PIG
     pressure, select one fallback route instead of returning Router 429;
   - order fallback routes by the existing pressure key: metrics health,
     waiting, fullness, effective running, processed count, and route id;
   - label the selection reason as `pig_pressure_passthrough`.

3. Keep candidate ordering useful:
   - for normal selection, put the cache/load winner first but retain
     pressure-ordered hard-eligible fallbacks;
   - for passthrough fallback, keep all hard-eligible routes and put the selected
     least-bad route first;
   - preserve pressure order for the remaining candidates, so a PIG 429 can
     immediately fall through to a sibling that may still admit the request.

4. Bound the first pressure fallback window:
   - pass at most the first three candidates into verified forwarding;
   - only an explicit HTTP 429 advances the capacity algorithm; request errors
     remain terminal and an accepted SSE stream is never moved;
   - a 429 records a short penalty for that route in the current metrics epoch,
     preventing stale-low-pressure herding.

5. Add one fresh-metrics retry window:
   - when every actual first-window response is 429, wait for the next completed
     metrics epoch and re-evaluate the current live upstream config;
   - prefer normally selectable and previously untried routes, then use the
     same tier-aware pressure key;
   - attempt at most three more candidates and at most six total by default;
   - if the new epoch does not arrive within one poll interval plus a bounded
     guard, or the second window is also all 429, return the normal PIG-shaped
     429.

6. Update aggregate status semantics:
   - green: at least one route has clear capacity;
   - yellow: no clear capacity, but at least one hard-eligible route can still
     receive passthrough/probe traffic;
   - red: no hard-eligible route exists.

## Tests

Add or update tests for:

- all routes full or waiting still forward to PIG with reason
  `pig_pressure_passthrough`;
- passthrough mode supplies multiple ordered candidates, so a 429 from the first
  pressured PIG can fail over to a later PIG that still accepts the request;
- passthrough mode supplies at most the first three pressure-ordered candidates;
- PIG/upstream 429 is preserved to the client;
- same-epoch 429 deprioritizes one route and a new metrics epoch releases it;
- an all-429 first window retries only after a fresh metrics epoch;
- both windows are bounded to three candidates and the total attempt budget is
  configurable from one through six;
- no configured/enabled route still returns the PIG-shaped Router 429;
- `/v1/upstream-status` returns yellow for soft-pressure-only exhaustion and red
  only for no hard-eligible route;
- existing cache-aware and load-aware selection continues to prefer healthy
  routes when any route has capacity.

## Release Steps

1. Implement the Router selection change.
2. Run local source checks that do not depend on Windows-only linker state.
3. Push source before building an image.
4. Build and push the Router image on the remote builder.
5. Stop after image publication unless production deployment is separately
   authorized.
6. For a separately authorized deployment, validate each Router with:
   - current Compose/image check;
   - container/log health;
   - `/health`;
   - authenticated model request where applicable;
   - admin/runtime route visibility;
   - soft-pressure behavior if it can be exercised safely.
