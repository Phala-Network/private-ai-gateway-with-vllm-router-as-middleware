# Providers

This directory documents upstream-provider verification. These documents are
still relevant in this fork because the router middleware only chooses a target
route; Private AI Gateway still verifies the selected provider before it sends
request bytes.

One directory per upstream provider may contain:

- `verification.md`: how the gateway verifies that provider and what
  cryptographically binds the enforced session.
- `review.md`: a point-in-time admission audit against
  [`audit-criteria.md`](audit-criteria.md).

| Provider | TEE | Session binding | Verification | Audit |
| --- | --- | --- | --- | --- |
| Chutes | Intel TDX + NVIDIA CC | `e2ee_public_key_sha256` | [verification](chutes/verification.md) | [review](chutes/review.md) |
| NEAR AI | Intel TDX + NVIDIA CC | `tls_spki_sha256` | [verification](near-ai/verification.md) | [review](near-ai/review.md) |
| Tinfoil | AMD SEV-SNP or TDX + NVIDIA CC | `tls_spki_sha256` | [verification](tinfoil/verification.md) | [review](tinfoil/review.md) |
| ACI service | Intel TDX + NVIDIA CC | `tls_spki_sha256` | [verification](aci-service/verification.md) | First-party |
| PhalaDirect | Intel TDX + NVIDIA CC | `tls_spki_sha256` | [verification](phala-direct/verification.md) | [review](phala-direct/review.md) |
| SecretAI | AMD SEV-SNP + NVIDIA CC | Deferred | Deferred | [review](secret-ai/review.md) |

For this router-middleware fork, production routing is expected to use multiple
PhalaDirect/PIG-backed upstreams serving the same public model. The other
provider docs are kept because the backend verifier code still supports them
and the proof-chain model is shared.

The router middleware design is documented separately in
[`../router-middleware.md`](../router-middleware.md). Middleware cannot create
or alter provider verification facts.

## Prefix-cache Tenant Isolation

As observed on 2026-07-13, Private AI Gateway does not guarantee per-tenant
prefix-cache partitioning for the active Kimi-K2.6 providers. The gateway
preserves a caller's `cache_salt` but does not derive one from the authenticated
Redpill tenant.

- Tinfoil replaces `cache_salt` with a value derived from Redpill's shared
  upstream credential. The gateway does not set `user_cache_secret`, so Redpill
  tenants share one namespace.
- Chutes passes `cache_salt` to vLLM but does not generate it. Unsalted requests
  share the serving instance's namespace.

Tinfoil's behavior is attestation-backed. Chutes configuration is control-plane
evidence and is not bound by its current attestation. The intended interface is
caller-controlled: preserve `cache_salt` for Chutes and translate it to
`user_cache_secret` for Tinfoil. The gateway should not derive or override the
partition from Redpill tenant identity.

## Shared Verification Model

A session binding is only trustworthy if it is bound into verified attestation.
Every provider produces exactly one kind of binding, and in every case the bound
value lives inside, or is digested into, the quote/report whose signature is
verified. Each `verification.md` states plainly what is bound and what a tamper
rejects.

The two binding types:

- `tls_spki_sha256`: SHA-256 fingerprint of the upstream TLS public key. The
  backend enforces it against the actual upstream HTTPS connection before
  forwarding.
- `e2ee_public_key_sha256`: SHA-256 of the upstream end-to-end public key. The
  backend encrypts the request body to that key, so only the attested enclave
  can decrypt.

## Invariant: Verified Means Enforceable

A `verified` result that carries no enforceable channel binding is rejected.
Forwarding fails closed if the selected backend cannot enforce the accepted
binding. A provider can never be "verified but unpinned."

## Attested Session Record

When an upstream is verified, the gateway content-addresses the verified
binding, verifier id, target, and evidence digest into a stable `session_id`
(`as_<sha256>`), stores an `AttestedSession` served by
`GET /v1/aci/sessions/{session_id}`, and attaches that `session_id` to the
receipt's `upstream.verified` event. Retention follows the receipt TTL; it is a
retention window, not a binding-validity deadline.

## End-to-End Verification

1. `GET /v1/attestation/report?nonce=<random>`: verify the gateway's own ACI
   report.
2. `GET /v1/aci/receipts/{chat_id}`: verify the receipt signature under the
   attested keyset.
3. Read `upstream.verified.session_id`, then fetch
   `GET /v1/aci/sessions/{id}`.
4. Confirm the record's `target`, `verifier_id`, `evidence.digest`, and
   `session_binding` match the receipt event.
5. The gateway has already enforced that binding on the wire before forwarding.

`scripts/live_e2e/user_verify.py` and
`scripts/live_e2e/cases/attested_sessions.py` implement this check.
