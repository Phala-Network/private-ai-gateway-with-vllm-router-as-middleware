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
   - for normal selection, keep only normally selectable routes;
   - for passthrough fallback, keep all hard-eligible routes and put the selected
     route first;
   - preserve pressure order for the remaining fallback candidates, so if the
     first PIG returns 429 the verified forwarding layer can immediately try the
     next least-pressured hard-eligible route instead of stopping at the first
     rejected node.

4. Bound the pressure fallback walk:
   - `pig_pressure_passthrough` passes at most the first three pressure-ordered
     candidates into verified forwarding;
   - the cap is count-based, not time-based, so it cannot abort an accepted
     generation request;
   - if all three candidates return capacity signals, the existing forwarding
     path returns the final real upstream 429.

5. Update aggregate status semantics:
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
5. Update the authorized Router CVMs directly with the new image.
6. Validate each Router with:
   - current Compose/image check;
   - container/log health;
   - `/health`;
   - authenticated model request where applicable;
   - admin/runtime route visibility;
   - soft-pressure behavior if it can be exercised safely.
