# PhalaDirect Verification

PhalaDirect is the expected production upstream provider for this
router-middleware fork. Each upstream entry points directly at one PIG-backed
Phala model endpoint, and the router middleware chooses among those entries for
one public model.

## Topology

```text
Private AI Gateway
  -> selected PhalaDirect/PIG endpoint
  -> vLLM or SGLang backend
```

- TEE: Intel TDX CPU workload plus NVIDIA Confidential Compute metadata.
- Session binding: `tls_spki_sha256`.
- Verifier: `verify_phala_direct` through the vendored
  `scripts/confidential_verifier` bridge and an external dstack-verifier
  service selected by `DSTACK_VERIFIER_URL`.
- Required producer behavior: attestation version 2 with a TLS SPKI fingerprint
  bound into quote `report_data`.

## Producer Requirement

The serving endpoint must bind its public TLS certificate SPKI into the quote.
For a fresh nonce:

```text
GET /v1/attestation/report?version=2&signing_algo=ecdsa&nonce=<hex>

report_data[0:32]  = SHA256(signing_address || tls_cert_fingerprint)
report_data[32:64] = nonce
response.tls_cert_fingerprint = SHA256(SPKI DER)
```

Soundness depends on TLS termination staying inside the model CVM. If TLS is
terminated outside the attested workload, the SPKI binding no longer proves that
the gateway is connected to the attested model endpoint.

## What The Gateway Verifies

For each selected upstream origin, the verifier:

1. Fetches `/v1/attestation/report?version=2&signing_algo=ecdsa` with a fresh
   nonce and the configured upstream bearer token.
2. Requires `tls_cert_fingerprint`. An older producer that ignores version 2 is
   rejected because the gateway cannot pin the request channel.
3. Rejects debug-mode TDX quotes.
4. Uses dstack-verifier to verify the quote, event log, VM config, and RTMR
   replay.
5. Checks `SHA256(app_compose) == compose_hash`.
6. Parses quote `report_data` and verifies the nonce plus
   `signing_address || tls_cert_fingerprint` binding.
7. Records NVIDIA GPU evidence as supplemental metadata. GPU evidence failures
   are recorded but are not the channel-binding gate.

The verified TLS SPKI fingerprint is then enforced against the actual HTTPS
connection before request bytes are forwarded.

## Session Binding

The enforced `tls_spki_sha256`, response signing address, and nonce are bound
inside the verified TDX quote. The gateway records the resulting attested
session and attaches its `session_id` to the receipt's `upstream.verified`
event.

Middleware cannot create or modify this provider verification event. It only
selects a configured route; the backend verifier owns the session binding.

## Tamper Cases

The bridge tests cover the important fail-closed cases:

- Missing `tls_cert_fingerprint`: rejected.
- Swapped TLS fingerprint: rejected by `report_data` binding.
- Wrong nonce: rejected by `report_data` binding.
- Invalid dstack quote: rejected.

GPU evidence nonce mismatch or NRAS failure does not reject the upstream by
itself. It is recorded as supplemental metadata because the CPU TEE quote and
measured serving software are the security boundary for the request channel.

## Provider Claims

The verifier records claims such as:

- `trust_boundary = phala-dstack-cvm`
- `evidence_scope = model_instance`
- `canonical_model_id`
- `attestation_version = 2`
- `tls_spki_from_report_data`
- `signing_address`
- `report_data_nonce_matched`
- `compose_hash_verified`
- `tdx_debug_mode = false`
- `tcb_status`
- `os_image_hash`, `os_image_version`, `os_image_is_dev`,
  `production_os_image`
- Supplemental GPU fields such as `gpu_verified`, `gpu_evidence_present`,
  `gpu_evidence_nonce_matched`, and `gpu_arch`

`production_os_image` is recorded metadata, not a hard gate in this verifier.
Fleet policy can decide whether to accept dev or production dstack images.

## Configuration

For this router-middleware fork, configure one upstream entry per serving node.
All entries should expose the same public model id so the middleware can choose
among them:

```json
[
  {
    "name": "node-a",
    "provider": "phala-direct",
    "base_url": "https://model-node-a.example.com",
    "models": {
      "public-model": "provider-model"
    },
    "bearer_token": "<upstream-api-token>"
  },
  {
    "name": "node-b",
    "provider": "phala-direct",
    "base_url": "https://model-node-b.example.com",
    "models": {
      "public-model": "provider-model"
    },
    "bearer_token": "<upstream-api-token>"
  }
]
```

See [`../../../deploy/upstreams.example.json`](../../../deploy/upstreams.example.json)
for the deployment example.

## Reproduce

```bash
export DSTACK_VERIFIER_URL="http://localhost:8080"

echo '{"api_version":"aci.provider-verifier.request.v1","provider":"phala-direct",
  "upstream_name":"phala-direct-live",
  "url_origin":"https://<model-endpoint>",
  "model_id":"<canonical-model>",
  "provider_options":{"phala_direct_bearer_token":"<api-key>"},
  "forwarded_body_hash":"sha256:'"$(printf '0%.0s' {1..64})"'",
  "required":true,
  "timeout_seconds":300}' \
  | uv run python scripts/private_ai_provider_verifier.py
```
