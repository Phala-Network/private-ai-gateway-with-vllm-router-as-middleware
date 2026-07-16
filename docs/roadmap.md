# Roadmap

Date: 2026-07-16 UTC.

This fork has a narrow purpose: run Private AI Gateway with an in-process
single-model router middleware that can choose among multiple PIG-backed
upstreams without adding a plaintext proxy hop outside the attested gateway
process.

The upstream Private AI Gateway project remains the source for general ACI
protocol evolution. This fork should stay small and operationally focused.

## Current Scope

- One public model per gateway deployment.
- Multiple configured upstream routes for that model.
- Dynamic add/remove through `GET` and `PUT /v1/admin/upstreams`.
- Cache affinity for warm-prefix reuse when route pressure is acceptable.
- PIG metrics polling for running, waiting, global limit, and tier pressure.
- Basic/premium-aware routing when a trusted front door supplies
  `x-user-tier`.
- Private AI Gateway backend-owned upstream verification, channel binding, and
  receipts.

## Shipped

| Area | Status | Notes |
| --- | --- | --- |
| In-process router middleware | Done | Middleware runs inside the gateway process and orders candidates before the verified backend forward. |
| Dynamic upstream config | Done | Live route set comes from `<state_dir>/upstreams.json`; admin `GET`/`PUT` updates do not require gateway restart. |
| Cache-aware ordering | Done | Bounded in-memory routing-text history can prefer a warmed route when pressure is balanced. |
| PIG pressure awareness | Done | Router polls upstream `/v1/metrics`, parses PIG running/waiting/limit/tier counters, and avoids pressured routes. |
| Tier handling | Done | `trusted_user_tier_header` defaults to `false`; public callers cannot self-promote to premium. |
| Verification chain | Done | Middleware does not mint verification facts; backend verification and receipt finalization remain unchanged. |
| Admin observability | Done | `GET /v1/admin/router` exposes redacted router state and PIG metrics status. |
| Deployment example | Done | `deploy/compose.yaml` uses git-launcher and enables the middleware with an empty upstream seed. |

## Hardening Backlog

| Priority | Item | Rationale |
| --- | --- | --- |
| P0 | Keep upstream Private AI Gateway compatibility current | This fork should avoid broad divergence outside the middleware and required wiring. |
| P0 | Expand router middleware regression tests | Cover more mixed basic/premium, stale metrics, backend waiting, and cache-affinity edge cases. |
| P0 | Exercise production-like multi-node smoke regularly | Validate admin add/remove, attestation, receipts, 429 shape, streaming, and cache-affinity behavior together. |
| P1 | Add structured router decision counters | Prometheus counters for selected-by-cache/load/order and blocked-by-pressure would reduce reliance on admin snapshots. |
| P1 | Add optional per-route health cooldown | If a route repeatedly fails before response bytes, a short cooldown can reduce repeated bad picks without hiding backend failures. |
| P1 | Document a strict production verifier policy | Operators still need a product-level policy for accepted gateway commits, dstack image posture, TLS bindings, and upstream provider pins. |

## Non-Goals

- Standalone vLLM Router packaging.
- External adapter process.
- `proxy_url` based forwarding.
- Generic multi-model routing policy.
- Model-specific production node lists in this repository.
- Letting middleware replace provider verification or receipt facts.
- Storing prompt text or cache history durably.

## Documentation Map

- [README.md](../README.md): repository overview, trust model, API surface, and
  quick start.
- [router-middleware.md](router-middleware.md): routing algorithm, PIG metrics,
  basic/premium behavior, and security boundary.
- [configuration-reference.md](configuration-reference.md): static config,
  middleware fields, upstream config, environment variables, and TLS binding.
- [../deploy/README.md](../deploy/README.md): git-launcher deployment path and
  compose ownership boundary.
- [providers/README.md](providers/README.md): provider verification documents
  used by the backend proof chain.
- [live-e2e-test-suite.md](live-e2e-test-suite.md): live test harness shape.
