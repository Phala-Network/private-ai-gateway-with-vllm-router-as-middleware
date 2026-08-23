# Configuration Reference

Private AI Gateway uses one read-only static config file and one writable state
directory. Operators must choose the config file with
`PRIVATE_AI_GATEWAY_CONFIG_PATH` and put gateway policy in that file.

## Runtime Files

| Item | Owner | Runtime path |
| --- | --- | --- |
| Static gateway config | Deployment | Required. Selected by `PRIVATE_AI_GATEWAY_CONFIG_PATH`. |
| Upstream seed config | Deployment | Selected by `upstream_config_seed_path` in the static gateway config. |
| Active upstream config | Gateway | `<state_dir>/upstreams.json` |
| Attested-session log | Gateway | `<state_dir>/sessions.jsonl` |

Operators configure `state_dir`, not the individual writable files inside it.
The gateway creates `state_dir` on startup, seeds `upstreams.json` from the
read-only upstream seed only when the active file is missing or empty, and
updates `upstreams.json` through `PUT /v1/admin/upstreams`.

Unknown fields in the static gateway config are rejected at startup.

## Minimal Config

This is the smallest practical container config.

```json
{
  "bind": "0.0.0.0:8086",
  "state_dir": "/var/lib/private-ai-gateway",
  "upstream_config_seed_path": "/etc/private-ai-gateway/upstreams.seed.json",
  "admin_token": "<long-random-admin-token>",
  "api_token": "<public-api-bearer-token>",
  "dstack_endpoint": "unix:/var/run/dstack.sock"
}
```

## Config Fields

| Field | Default | Meaning |
| --- | --- | --- |
| `bind` | `127.0.0.1:8086` | Public HTTP listener address. Use `0.0.0.0:8086` in containers that expose the gateway port. |
| `state_dir` | `/var/lib/private-ai-gateway` | Gateway-owned writable state directory. The active upstream config and attested-session log are derived from this directory. |
| `upstream_config_seed_path` | unset | Read-only JSON seed copied to `<state_dir>/upstreams.json` only when the active upstream config is missing or empty. |
| `admin_token` | unset | Bearer token for `GET`, `PUT`, and `PATCH /v1/admin/upstreams`, plus `GET /v1/admin/router`. When unset, the admin API is not exposed. |
| `api_token` | unset | Optional bearer token for public inference, model catalog, gateway/Router metrics, and `/v1/upstream-status`. When unset, those routes are publicly reachable. |
| `dstack_endpoint` | dstack SDK default | dstack SDK endpoint, such as `unix:/var/run/dstack.sock`. |
| `direct_serving` | `false` | Set only when inference is served inside this same attested workload with no upstream hop. It is mutually exclusive with middleware mode. |
| `enable_e2ee` | `false` | Inherited PAG field. Router middleware deployments do not use it because user-facing E2EE, if enabled, terminates at downstream PAG before this Router is called. |
| `middleware` | unset | Optional single-model router middleware. When present, the gateway orders configured upstream candidates locally, then forwards through the verified backend. See [Middleware](#middleware). |

When `middleware` is configured, Router does not evaluate user-facing E2EE
headers or enter inherited E2EE request handling. The Router deployment expects
downstream PAG to send already-normalized cleartext bytes.

## Middleware

The optional `middleware` section runs middleware in the request path. The
middleware is inside the gateway process, after frontend normalization and
before the verified backend forward. Router middleware may choose routes and
read parsed request JSON for cache-aware ordering, but it forwards the exact
cleartext request bytes it received. Upstream verification, channel binding,
forwarding, and receipt finalization remain backend responsibilities. When the
section is omitted the gateway serves directly.

This fork supports one middleware shape: a single public model routed across
multiple configured upstreams with cache-aware and PIG-aware load ordering. The
router polls each configured upstream's metrics endpoint in the background and
uses PIG's observed running, waiting, global limit, and tier counters to avoid
full or pressured nodes before forwarding. It attempts prefix-cache affinity
first, accepts the matched route only when that route passes the load guard
against the current least-loaded route, and otherwise falls back to lower-load
routing. If upstream metrics are unavailable or stale, routing falls back to
gateway-local in-flight counters instead of blocking traffic. It does not use
`control_url`, `proxy_url`, or an external adapter/vLLM Router process. See
[router-middleware.md](router-middleware.md) for the selection algorithm and
security boundary.

| Field | Default | Use |
| --- | --- | --- |
| `middleware.public_model` | unset | Public model id served by this gateway. When unset, the router derives it from the live upstream config and requires exactly one unique public model. |
| `middleware.cache_threshold` | `0.30` | Minimum common-prefix match rate needed to try a previously warmed route before load fallback. |
| `middleware.balance_abs_threshold` | `64` | Absolute running-request gap above which a cache-matched route is rejected in favor of the least-running route. |
| `middleware.balance_rel_threshold` | `1.50` | Relative running-request gap above which a cache-matched route is rejected in favor of the least-running route. |
| `middleware.max_history_per_route` | `256` | Maximum routing-text records kept per public model and route in the process-local radix cache index. Each stored routing text is capped internally and the cap is visible as `routing_text_max_chars` in `/v1/admin/router`. |
| `middleware.metrics_poll_ms` | `1000` | Background upstream metrics polling interval. Set to `0` to disable PIG-aware routing and use only gateway-local in-flight counters. |
| `middleware.metrics_timeout_ms` | `800` | Per-upstream metrics request timeout. Polling uses an internal concurrency bound of four, so one slow upstream does not block the other active slots and a large target set cannot open one metrics connection per node at once. |
| `middleware.metrics_stale_ms` | `3000` | Age after which a metrics sample is ignored and the route falls back to local in-flight state. |
| `middleware.metrics_path` | `/v1/metrics` | Metrics path appended to each upstream base URL. The upstream's configured bearer token is used for metrics auth. |
| `middleware.trusted_user_tier_header` | `false` | Whether inbound `x-user-tier` is trusted for routing and forwarding to PIG. Keep `false` for public endpoints unless a trusted front door strips or sets this header. With the default, all requests are routed as `basic` and no caller-supplied tier header is forwarded. |
| `middleware.default_engine` | unset | Deprecated compatibility field. Transparent router middleware accepts it in existing configs but does not use it to shape request bodies. |
| `middleware.control_url` | unset | Optional control-plane URL for best-effort post-request usage reports only. Routing and catalog handling stay local. |
| `middleware.control_token` | unset | Bearer token sent to the optional control-plane usage-report endpoint. |
| `middleware.control_post_timeout_ms` | `10000` | Timeout for the fire-and-forget post-request usage report. |
| `middleware.pricing` | unset | Optional static pricing block used to inject `usage.cost` into client responses and usage reports. |
| `middleware.sse_keepalive_ms` | `10000` | Idle keep-alive interval for streaming responses; `0` disables the heartbeat. |

Request outcome observation is always on and needs no configuration. Requests
that reach the middleware completion path emit structured `request_outcome`
tracing lines for routing/shaping failures, upstream errors, stream failures,
client disconnects, and anomalous successful finish reasons. The `detail`
field is emitted only when the `request_outcome` target is enabled at `debug`.
Silence or re-route the target via `RUST_LOG` (the subscriber uses `EnvFilter`).

Router cache-affinity outcome and token-efficiency counters are appended to the
authenticated `GET /v1/metrics` response when middleware is enabled. Cache
records are committed only after the final serving upstream succeeds; no extra
configuration flag is required. See [router-middleware.md](router-middleware.md)
for metric names and lifecycle semantics.

```json
{
  "middleware": {
    "public_model": "gemma4-31b-it",
    "metrics_path": "/v1/metrics",
    "trusted_user_tier_header": true,
    "control_url": "https://control.example"
  }
}
```

No middleware field is strictly required, but production deployments normally
set `public_model` and `trusted_user_tier_header` explicitly.

## Source Provenance

Source provenance is not a gateway config field. The gateway reports source
provenance from the dstack git-launcher pin at
`/etc/git-launcher/gateway.conf`:

```text
REPO_URL=https://github.com/Phala-Network/private-ai-gateway-with-vllm-router-as-middleware.git
COMMIT_SHA=<audited-full-40-or-64-hex-commit-sha>
WORK_DIR=/var/lib/git-launcher/private-ai-gateway-router
```

When the launcher config is absent, source provenance is unknown and the
gateway omits `source_provenance` from attestation reports. Production
deployments should use `git-launcher`. The native ACI-service verifier checks
that `attestation.evidence.app_compose` hashes to the `compose-hash` event bound
into RTMR3. Binding the reported repository commit or image digest to reviewed
source remains a verifier-policy TODO.

The canonical attestation endpoint publishes the raw measured `app_compose`.
Never place plaintext tokens, API keys, or passwords in Compose. Use Phala
encrypted environment variables and leave only variable references in the
measured file. Their encrypted values are not published or bound by
`app_compose`.

If the launcher config exists, `COMMIT_SHA` must be a full 40- or 64-character
hexadecimal commit hash. Branch names, tags, and short hashes are rejected at
startup.

## TLS Binding

TLS binding is optional. Configure it only when clients verify the gateway's
public TLS certificate SPKI from the attested keyset.

| Field | Use |
| --- | --- |
| `tls.domain_certificates` | One mounted leaf certificate per public hostname. |

For multi-domain listening, use `tls.domain_certificates`:

```json
{
  "tls": {
    "domain_certificates": [
      {
        "domain": "api.example.com",
        "certificate_path": "/run/certs/api.pem"
      },
      {
        "domain": "chat.example.com",
        "certificate_path": "/run/certs/chat.pem"
      }
    ]
  }
}
```

Raw SPKI digest inputs are not supported. The gateway reads mounted leaf
certificates, computes `sha256(SPKI)`, and publishes those digests in the
attested keyset. When `tls.domain_certificates` is configured, the request
`Host` selects the matching downstream TLS binding for
`/v1/aci/attestation`. Unknown hosts return `404 not_found`.

## Upstream Config

The upstream seed file and active upstream database use the same JSON shape: an
array of upstream entries. The seed file is deployment-owned and read-only. The
active file at `<state_dir>/upstreams.json` is gateway-owned and is replaced by
the admin API.

```json
[
  {
    "name": "route-a",
    "provider": "aci-service",
    "base_url": "https://upstream-a.example",
    "models": {
      "public-model": "provider-model"
    },
    "accepted_subjects": ["app-id:0x<measured-app-id>"],
    "accepted_dstack_kms_root_public_keys": ["<kms-root-public-key>"]
  }
]
```

Supported `provider` values:

| Provider | Use |
| --- | --- |
| `openai-compatible` | Generic OpenAI-compatible upstream with no provider-owned verifier. |
| `aci-service` | ACI service that exposes dstack/DCAP evidence. |
| `tinfoil` | Tinfoil provider adapter. |
| `near-ai` | NEAR AI provider adapter. |
| `chutes` | Chutes provider adapter. |
| `secret-ai` | Direct SecretAI SecretVM origin with optional workload pinning; see [SecretAI verification](providers/secret-ai/verification.md). |
| `phala-direct` | Direct Phala dstack-vllm-proxy endpoint. |

Provider verification policy belongs on the upstream entry. For ACI service
routes, configure accepted keyset subjects, image digests, or dstack KMS
root public keys. For `aci-service` upstreams a subject anchors only in its
measured form — `app-id:0x<hex>` of the RTMR3-verified app id. The upstream
does not need to set a keyset `subject` of its own. For `secret-ai` the same
field pins measured SecretVM workload ids on that entry.

For `secret-ai`, `base_url` must be the root HTTPS inference origin. The optional
`accepted_subjects` field pins measured SecretVM workloads in this form:

```text
secretvm:<cpu-type>:<environment>:<template>:<artifacts-version>:sha256:<compose-sha256>
```

Without this field, the verifier still reconstructs and reports the exact
production workload, but does not assert that its serving software was
operator-approved. When pins are configured, a nonmatching workload fails
verification. TDX workloads must report DCAP status `UpToDate`. An SEV-SNP
origin must meet the componentwise AMD TCB minimum embedded in the verifier.

For `aci-service`, `base_url` is the HTTPS origin used for both model traffic and
`/v1/aci/attestation`. The router fetches the report through normal TLS,
derives the attested TLS SPKI binding from that report, then pins that SPKI for
the actual upstream request. In router-middleware deployments, request body
model rewriting must happen before the request reaches this router; the router's
upstream config supplies route and verification metadata, not a data-plane body
transform.

## Environment Variables

The gateway runtime reads only these environment variables. Provider verifier
bridges may consume provider-specific environment variables such as
`DSTACK_VERIFIER_URL` or `PRIVATE_AI_VERIFIER_DIR`.

| Variable | Use |
| --- | --- |
| `PRIVATE_AI_GATEWAY_CONFIG_PATH` | Required. Selects the static gateway config file. |
| `RUST_LOG` | Tracing filter consumed by `tracing_subscriber`. |

Deployment tooling also uses these variables:

| Variable | Use |
| --- | --- |
| `PRIVATE_AI_GATEWAY_CACHE_DIR` | `entrypoint.sh` build and toolchain cache root. Defaults to `/var/lib/private-ai-gateway/cache`. |
| `CARGO_HOME` | Optional override for Cargo cache. Defaults under `PRIVATE_AI_GATEWAY_CACHE_DIR`. |
| `RUSTUP_HOME` | Optional override for Rustup state. Defaults under `PRIVATE_AI_GATEWAY_CACHE_DIR`. |
| `CARGO_TARGET_DIR` | Optional override for Cargo build output. Defaults under `PRIVATE_AI_GATEWAY_CACHE_DIR`. |
| `PRIVATE_AI_GATEWAY_REPO_COMMIT` | Used by `deploy/compose.yaml` interpolation for the git-launcher `COMMIT_SHA` pin. |
| `PRIVATE_AI_GATEWAY_ADMIN_TOKEN` | Used by `deploy/compose.yaml` interpolation for the static config's `admin_token`. |
