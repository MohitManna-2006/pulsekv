# PulseKV semantic context — Phase 10.5 summary

**Date:** 2026-08-21  
**Status:** complete  
**Scope:** OpenAI-compatible ingress gateway; no inference-engine or PulseKV
core changes

## 1. Outcome

Phase 10.5 replaces the final gateway stubs with a working standalone process.
An application can now point its OpenAI-compatible chat-completions client at
PulseKV's gateway instead of directly at SGLang or vLLM:

```text
application
    -> POST /v1/chat/completions
    -> decompose in original order
    -> resolve eligible blocks through Tiers 0-3
    -> substitute only accepted canonical versions, in place
    -> forward to the configured inference upstream
    -> relay the upstream status, headers, and response stream
```

No file in `node/`, `control/`, `proto/`, or `adapters/` changed. The gateway
does not call PulseKV and does not change either inference adapter's cache-key
logic. It changes text before the inference server tokenizes it, which is the
architectural boundary fixed in the design.

## 2. Deployment decision: inline proxy with an external direct route

Phase 10.5 resolves design §8's E-vs-F packaging question as **option E, an
inline reverse proxy**. It has one standard request surface:

- `POST /v1/chat/completions`
- `GET /healthz`
- `GET /readyz`

`upstream_url` remains a real, separately routable inference endpoint. The
application or load balancer must keep a direct-to-upstream route as its
gateway-process-down fallback. This is deliberately outside the process: code
inside an unavailable process cannot fail open. Component failures inside a
running process take the byte-exact pass-through path described below.

The process runs one Uvicorn worker. The registry already supplies a
connection per request thread and WAL-mode concurrent access; a single process
also avoids multiplying the in-memory vector index and ONNX session without
measurements that justify it.

## 3. Request path as built

### 3.1 Namespace resolution

Namespace is resolved before matching and never inferred from prompt text.
All four deployment shapes in the frozen enum now have defined behavior:

| Source | Resolution |
|---|---|
| `static` | one explicitly configured namespace |
| `header` | a named header supplied by a trusted auth/routing layer |
| `api_key` | an opaque credential mapped to a namespace in configuration |
| `route` | the exact inbound path mapped to a namespace in configuration |

Every resolved value passes the frozen `Namespace` validator. A missing or
invalid namespace is a client/deployment error (`400`, or `401` for API-key
mode), not a semantic miss. In header mode the trusted namespace header is
removed before forwarding. Inference authorization and unrelated headers are
preserved.

### 3.2 Decomposition and assembly

`decompose()` remains the single classifier. The new assembler runs the same
deterministic message-then-tool traversal and verifies each block still
matches the request it came from before editing a copy.

The assembler enforces:

- no duplicate or unknown block indices;
- no empty or non-string canonical substitution;
- tool substitutions must decode to a JSON object;
- message order and tool order never change;
- unrelated request fields never change;
- when no substitution is accepted, no serialization occurs and the exact
  inbound bytes are forwarded.

This last property is stronger than JSON semantic equality. Whitespace and key
formatting in a no-match/error request are retained byte-for-byte.

### 3.3 Matching and canonical fetch

Eligible blocks run through the existing complete matcher. An accepted result
does not use an unversioned or cached text held outside the registry: the
gateway reads the exact accepted `(namespace, context_id, version)` and only
then adds its canonical text to the substitution map. If that read fails, the
final audited decision becomes a registry error and the original block is
sent.

When `model_dir` is configured, the gateway loads the ONNX encoder and builds
each namespace's vector bucket on first use. An unavailable model does not
prevent the process from starting: Tiers 0/1 remain active and Tier 2 is
reported disabled in `/healthz`. Encoder calls and guard checks use their real
existing timeout wrappers. SQLite's configured busy timeout bounds registry
lock waits.

### 3.4 Eligible-token bypass

The design asks for a token threshold while also rejecting inference-engine
tokenization inside the gateway. Phase 10.5 resolves that seam with a coarse
`ceil(characters / 4)` estimate, used **only** to decide whether to skip work.
It is never hashed, embedded, compared, guarded, or sent upstream. The default
512 threshold remains explicitly unmeasured; Phase 10.8 owns replacing it
with production evidence.

Every below-threshold block still receives a decision-log outcome. Eligible
blocks are `below_min_tokens`; ineligible blocks remain
`ineligible_block_type`.

### 3.5 Proxy behavior

The gateway preserves query strings, upstream authorization, request IDs, and
non-hop-by-hop headers. It removes `Host`, stale content length, hop-by-hop
headers, and the trusted namespace header. The response path relays upstream
status and content type and streams unread HTTPX response bodies; the test
transport's already-buffered body is also supported.

Malformed JSON is forwarded for the upstream to validate. Oversized bodies
are rejected locally with `413` before decomposition or forwarding. An
upstream transport failure is a `502`, because fail-open semantic processing
cannot manufacture an inference response from an unavailable inference
server. The client-facing error is generic; details go to the operator log.

## 4. Fail-open evidence

The Phase 10.5 request tests use a stub downstream and inspect the bytes it
actually receives.

| Condition | Demonstrated wire result |
|---|---|
| no candidate | exact original bytes forwarded |
| semantic feature disabled | exact original bytes, namespace not required |
| registry disappears after a Tier 0 hit but before canonical fetch | exact original bytes; audited as registry error |
| encoder error | exact original bytes; audited as encoder error |
| guard error | exact original bytes; audited as `guard_error` rejection |
| malformed canonical tool JSON | assembly refuses the substitution |
| malformed OpenAI JSON | exact original bytes forwarded for upstream validation |
| gateway process unavailable | direct `upstream_url` route is the required LB/client fallback |
| inference upstream unavailable | `502`; this is not a semantic-component failure |

The registry failure test is deliberately a mid-request transition: Tier 0
resolves an alias, the registry is closed, and the version fetch required for
assembly fails. The body observed by the downstream is still the exact
original.

Decision records continue to contain hashes and outcomes, never prompt or
canonical text. Audit-sink failure remains non-fatal by the `AuditLog`
contract. `/healthz` exposes only state, a generic startup error code, and
dropped-audit count; it does not expose the registry path, API-key mapping, or
namespace.

## 5. Configuration and operator surface

`config.load()` now:

1. reads YAML or JSON;
2. requires a mapping root;
3. rejects unknown fields;
4. applies typed defaults;
5. reports all Pydantic field errors together;
6. reports all cross-field problems together;
7. returns warnings separately from fatal problems.

Cross-field checks cover the selected namespace source, every configured
namespace, SQLite-only registry DSNs, an absolute HTTP(S) upstream without a
query/fragment, listen settings, and `guard_top_n <= top_k`.

`gateway/gateway.example.yaml` is the complete starting configuration. The
installed console command is:

```bash
pulsekv-gateway --config /path/to/gateway.yaml
```

Pinned additions in Phase 10.5 are Starlette 0.48.0, HTTPX 0.28.1, Uvicorn
0.35.0, and PyYAML 6.0.3.

## 6. First `T_gateway` measurement

The Phase 10.5 gate asks for a first end-to-end number against a stub, not the
real-inference benchmark that belongs to Phase 10.8. The measurement used:

- macOS development host;
- Python 3.14.4;
- Starlette `TestClient` at the ingress and HTTPX `MockTransport` downstream;
- one registered system-prompt alias hit plus one ineligible user block;
- SQLite WAL registry and in-memory audit log;
- 20 warm-up requests followed by 300 measured requests;
- identical local Starlette stub as the harness baseline.

| Measurement | p50 | p95 |
|---|---:|---:|
| baseline stub round trip | 0.260 ms | 0.346 ms |
| gateway + alias match + stub | 0.780 ms | 1.302 ms |
| observed delta (`T_gateway`) | **0.520 ms** | **0.956 ms** |

These are local in-process harness numbers, not a production SLO and not a
claim about a semantic ONNX path. They replace only the prior “no gateway
exists to measure” caveat. Phase 10.8 must measure real network transport,
concurrency, request sizes, semantic hits, and real inference savings before
the cost/benefit gate is answered.

## 7. Verification

The pinned verification environment ran with the real MiniLM model directory:

```text
427 passed in 7.44s   # first full pass after proxy implementation
430 passed in 7.42s   # final pass after stream, upstream, and body-limit cases
```

Phase 10.5 adds 21 focused tests covering configuration, assembly, namespace
shapes, proxy request/response behavior, byte identity, health/readiness,
response streaming, upstream failure, and all in-process fail-open rows. The
pre-existing model, registry, deterministic-tier, retrieval, and guard corpus
suites remain green.

## 8. Handoff to Phase 10.6

The gateway is ready to point at a real SGLang OpenAI endpoint. Phase 10.6
should keep the same hard boundary: do not edit PulseKV core, adapters, or
inference-engine internals. Its job is to replace the stub upstream with real
SGLang and prove that two wording variants sent through this gateway produce
the byte-identical canonical tokens that the unchanged exact cache already
recognizes.

Open limitations intentionally carried forward:

- the bypass threshold and budgets are provisional until Phase 10.8;
- the in-memory semantic index is built lazily and registry publication still
  requires an index refresh/restart policy in hardening;
- the direct-route fallback is an operator/LB responsibility, not automatic
  client retry code in this repository;
- Phase 10.5 does not claim real SGLang/vLLM cache-hit evidence — that is
  exactly what Phases 10.6 and 10.7 exist to establish.
