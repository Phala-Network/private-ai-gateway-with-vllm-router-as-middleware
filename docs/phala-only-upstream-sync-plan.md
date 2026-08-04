# Phala-Only Upstream Sync Plan

Date: 2026-08-04 UTC.

## Goal

Refresh this router fork against the current `Dstack-TEE/private-ai-gateway`
`main` branch only where the change affects the Phala-owned serving path:

```text
client -> Private AI Gateway router middleware -> PIG -> vLLM or SGLang
```

The fork should remain a narrow single-model, multi-upstream, cache-aware and
load-aware router. It should not become a general provider compatibility fork.

## Scope

Keep or port changes that directly improve the Phala path:

| Upstream area | Decision | Reason |
| --- | --- | --- |
| #105 delayed capacity retry | Include | PIG and upstream routers can return capacity signals. Premium traffic should get one short retry window while basic traffic remains fast-fail. |
| #107 TLS-pinned upstream client reuse | Already present in branch baseline | Phala Direct / ACI-pinned forwarding benefits from persistent clients and avoids repeated TCP/TLS setup. |
| #112 reasoning normalization | Include | OpenAI-compatible Phala routes need consistent reasoning request shaping for vLLM/SGLang and optional reasoning exclusion. |
| #114 `/v1/messages` stream EOF handling | Include | Streaming usage and client-visible SSE correctness matter for Phala gateway surfaces. |
| #115 routing 404 semantics | Include | Unroutable model ids should be OpenAI-compatible `404 model_not_found`, while provider-side model-catalog 404s should fail over to sibling candidates. |
| Provider-neutral SSE parser hardening | Include | It protects the streaming path used by Phala routes without changing provider policy. |

Exclude changes that only serve third-party provider-specific integrations:

| Upstream area | Decision | Reason |
| --- | --- | --- |
| #108 NEAR AI authenticated attestation fetch | Exclude | It does not affect the PIG/vLLM/SGLang path. |
| Provider verifier scripts or dependencies for non-Phala providers | Exclude | This fork should not grow unrelated provider runtime surface. |
| Generic provider behavior changes not exercised by Phala deployments | Exclude unless separately requested | Keeps the repository auditable and narrow. |

## Implementation Steps

1. Compare `upstream/main` with this fork and classify each upstream change as
   Phala-path, already-present, or out-of-scope.
2. Keep the current local diff limited to:
   - middleware routing and completion flow,
   - request/response/SSE transforms,
   - Phala-relevant capacity/error classification,
   - route candidate metadata needed by those transforms.
3. Confirm no provider verifier, NEAR, Chutes, Secret AI, or Tinfoil-specific
   files are changed unless the change is already required by the existing
   Phala proof chain.
4. Run remote builder validation inside the Ubuntu builder container.
5. Push source only after tests pass, so the published image can be traced back
   to GitHub.

## Validation Plan

Remote builder checks:

```text
cargo fmt --all --check
cargo test --all --no-run
cargo test --lib middleware::reasoning
cargo test --lib middleware::request_transform
cargo test --lib middleware::stream_transform
cargo test --test middleware_completion
cargo test --test upstream_config_admin
```

Functional coverage:

| Area | Expected result |
| --- | --- |
| Basic capacity exhaustion | Returns the existing OpenAI/vLLM-shaped 429 quickly. |
| Premium capacity exhaustion | Retries capacity-only candidates once after a short delay, then preserves the best upstream response if still full. |
| Unroutable public model | Returns `404 model_not_found`, not a malformed-request `400`. |
| Provider catalog 404 | Fails over to sibling candidates before committing the 404 response. |
| Real backend 5xx | Not disguised as client input or capacity unless the body matches the narrow capacity marker. |
| Reasoning request fields | Normalized once before routing, then projected per candidate for vLLM/SGLang-compatible OpenAI routes. |
| `include_reasoning=false` | Removes visible reasoning fields while preserving usage and reasoning-token counters. |
| Streaming SSE | Handles CRLF, oversized lines, parse errors, upstream missing `[DONE]`, and client close without breaking metering. |
| Provider-specific code | No new NEAR or other non-Phala provider runtime changes. |

## Deployment Boundary

This plan does not authorize production deployment by itself. For router CVM
updates, use the established drain-safe procedure:

1. Snapshot the enabled upstream set.
2. Disable the enabled upstreams.
3. Wait until Router/PIG running and waiting counters drain to zero.
4. Deploy the new image.
5. Validate health, authenticated models, upstream status, admin state, and a
   representative streaming request.
6. Restore exactly the upstreams that were enabled before the update.
