# PulseKV v3 / Phase 10 — Semantic Context Canonicalization: System Design

**Status:** architectural investigation, not yet implemented. Companion to
`pulsekv-v2-distributed-design.md` (what v2 is) and this document's sibling,
`pulsekv-semantic-context-implementation-plan.md` (how it gets built). Does not
modify v1 or v2. v2's data plane, control plane, adapters, and proto contract
remain exactly as documented in Phases 0–9.

**Supersedes, with corrections:** `docs/pulsekv-v2-semantic-canonicalization-report.md`
(Aug 18, 2026). That report reached the same top-level architectural
conclusion this document reaches — an ingress gateway producing canonical
*text*, not tokens, feeding unmodified SGLang/vLLM — and that conclusion is
reaffirmed here against the current source, not just against the design docs
it was written from. But it also states a number of specific latencies,
thresholds, and a break-even table (§7, §11 of that report) as measured fact
with no benchmark behind them. None of those numbers are repeated here as
fact. Where this document gives a number, it is either sourced to a real
artifact in the repository (cited) or explicitly labeled a hypothesis to be
measured in Phase 10.3/10.8, per this investigation's own evidence standard
(prompt §40).

---

## 1. Problem statement

PulseKV v2 does exact-match KV-cache reuse: a token sequence hashes to a
block key, and only a request producing the *identical* token sequence hits
that key. Two prompts that mean the same thing but are worded differently
tokenize differently, miss the cache, and pay full prefill. In production
LLM-serving traffic, a large fraction of the prompt is not the user's actual
question — it is system instructions, tool schemas, organization policy,
RAG context, and few-shot examples, assembled by an application layer that
frequently does not word these blocks identically across services, versions,
or deploys. Every such variation is a cache miss v2's exact model cannot
recover from, no matter how well the underlying cluster performs.

## 2. Why this feature exists

Not because approximate KV reuse is desirable — §7 rules that out on
correctness grounds, not availability grounds. The feature exists because a
narrower, safer transformation is available: if two differently-worded
*reusable* context blocks can be shown to mean the same thing, replacing both
with one agreed canonical string before either reaches the tokenizer makes
them produce byte-identical tokens, and v2's existing exact-match machinery
does the rest with zero changes. The opportunity is real to the extent that
production prompts contain large, structurally reusable blocks (§13); it does
not depend on canonicalizing free-form user text, which this design
explicitly excludes.

## 3. Real workloads that benefit

Grounded in the block taxonomy already used in the prior investigation and in
the task brief (§3 of the master prompt), the workloads with a plausible
payoff are ones where the *same organization's* applications, or the *same
application's* different versions, independently construct a large reusable
block that means the same thing but is worded differently:

- A tool-use agent framework whose system/tool-policy block is templated
  server-side but re-rendered per request with minor non-semantic variation
  (whitespace, ordering, a maintainer's wording tweak between deploys).
- Two application surfaces (a web app and a Slack bot, say) built by
  different teams against the same internal policy document, each
  paraphrasing it into their own system prompt.
- A RAG pipeline whose retrieved-document formatting wrapper changes between
  versions while the underlying document block is unchanged.

Workloads this does **not** plausibly help, and which Phase 10 explicitly
does not target: open-ended user questions, conversation history, anything
where "same meaning, different wording" is exactly the kind of judgment call
that produces false positives (§8 below).

## 4. Goals

- Increase exact PulseKV cache-hit rate for long, structurally reusable
  prompt blocks, without weakening the exact-match invariant v2 is built on.
- Keep `node/engine/`, `node/grpc_shim/`, `control/`, `proto/`, and the
  existing `adapters/pulsekv_adapters/{client,key,sglang,vllm,vllm_key}.py`
  modules **unmodified**. See the companion codebase impact map for the
  directory-by-directory classification this claim rests on.
- Make the feature optional and fail-open: SGLang, vLLM, and PulseKV must
  each work exactly as they do today with the gateway absent, disabled, or
  down.
- Bias hard toward zero false positives over high match rate. A missed match
  costs a prefill recompute — the system's normal, correct behavior today. A
  false match changes what the model was asked, silently.

## 5. Explicit non-goals

- Approximate KV tensor reuse across non-identical token sequences (ruled out
  in §7 — not a scope choice, a correctness one).
- Canonicalizing free-form user queries in the MVP (task brief §3, §12).
- Any change to `node/engine/`'s value semantics, key format, or eviction
  policy.
- A learned/LLM-based prompt rewriter in the MVP (see §10 — rejected on
  latency and hallucination-risk grounds, not merely deferred).
- Silent prompt reordering (§14) — a gateway that changes block *order*
  without proving the reorder is semantics-preserving is not safer than one
  that changes block *wording* without proving equivalence, and both are out
  of scope for the same reason: unproven behavior change.
- Solving the unrelated Phase 9 soak-test question raised in the master
  prompt (§25). Addressed as a gating question in §17 and in the risk
  register, not folded into this design.

## 6. Existing PulseKV architecture (as verified against source, not docs alone)

Read directly for this investigation: `adapters/pulsekv_adapters/{client,key,
sglang,vllm,vllm_key,__init__}.py`; `proto/{node,metadata,adapter}.proto` and
`proto/README.md`; `control/internal/router/router.go`;
`control/internal/metadata/service.go`; `node/README.md`; `node/engine/
README.md`; `node/engine/include/pulsekv_engine.h`; the v2 distributed design
and implementation plan; `docs/pulsekv-v2-progress-report.md`; Phase 7 and
Phase 8 summaries; `Makefile`; and the current `deploy/run/soak-report.json`
and `deploy/run/logs/soak-chaos.log` artifacts. Three findings from that
reading materially change or sharpen conclusions the prior semantic report
reached from docs alone:

**Finding 1 — `AdapterService` (`proto/adapter.proto`) is dead code.** It was
frozen in Phase 0 as "the narrow surface the Python LLM adapters call," but
Phase 7's actual implementation of `PulseKVHiCacheStorage` (and Phase 8's
`PulseKVKVConnector`) call `PulseKVClient` — which talks to
`ClusterMetadataService` for topology and directly to `NodeService` on data
nodes for gRPC/bulk transport — and never construct an `AdapterService`
stub. Nothing serves `AdapterService`; nothing calls it. This matters for
Phase 10 because it removes a boundary that looked, from the design docs
alone, like the intended integration seam for external callers. It is not:
the real seam Phase 7/8 actually use is `pulsekv_adapters.client.PulseKVClient`
itself, which is a plain, general-purpose, well-tested Python object
(`get`/`set`/`exist`/`put`, topology-aware routing, automatic unary/chunked/
bulk transport selection) with no LLM-specific assumptions in it. A gateway
process is a legitimate second consumer of that exact class, imported as a
library, the same way `sglang.py` and `vllm.py` are.

**Finding 2 — canonical *text* is sufficient; no PulseKV-side key changes are
needed.** `pulsekv_adapters/key.py` (SGLang) and `vllm_key.py` (vLLM) both
derive block hashes from token IDs the *inference engine's own tokenizer*
produces, chained SHA-256 over 4-byte big-endian token bytes. Neither module
is reachable from outside the adapter call path; neither takes canonicalization
metadata as input today. If a gateway hands SGLang or vLLM byte-identical
canonical text for two originally-different requests, the engines' own
existing tokenization and hashing — completely unmodified — produces
byte-identical block hashes, and `PulseKVHiCacheStorage`/`PulseKVKVConnector`
resolve to the same PulseKV key with **zero new PulseKV-side logic**. This
directly answers investigation question 4 (§38.4 of the master prompt): yes,
canonical text automatically produces the exact existing cache identity,
verified against the real key-derivation code, not asserted from the design
doc.

**Finding 3 — the control plane's readiness-gate pattern is the right
precedent for a registry, but the registry does not belong inside it.**
`control/internal/metadata/service.go`'s `snapshot()` method is the one choke
point both `GetNodeList` and `GetShardMap` pass through, and it fails closed
(`Unavailable`, not a silent empty answer) when the membership source is not
yet ready (see `pulsekv-v2-restart-readiness-fix-summary.md` for why that
gate exists at all — a real bug, fixed by exactly this kind of choke-point
discipline). A semantic registry needs the same fail-closed discipline for
its *own* callers, but it must not become a second thing `ClusterMetadataService`
is responsible for: that service's entire job today is shard placement, and
Raft's write path is reserved, by the v2 design doc's own explicit rule, for
low-volume cluster metadata — not a registry that gets read on every
gateway-processed request. §17 gives the registry its own component instead.

## 7. Correctness invariants (what must remain exact)

Unchanged from the master prompt's framing, restated with the source grounds
now confirmed:

1. **A different token sequence must never receive another sequence's KV
   state.** Nothing in this design touches PulseKV's storage semantics; every
   stored KV block is still addressed by an exact chained hash of the tokens
   actually sent to the model. The gateway's only lever is *what text gets
   tokenized*, never what happens after tokenization.
2. **Canonicalization is a pre-tokenization text transform, not a token or
   tensor transform.** Confirmed compatible with Finding 2: the adapters and
   the engines downstream of them have no canonicalization awareness and need
   none.
3. **A rejected or low-confidence match must fall through to the original
   text, unmodified**, not to a lower-confidence canonical guess. There is no
   partial-credit mode.
4. **The gateway is not on PulseKV's critical path for existing traffic.**
   Nothing already using `PulseKVClient`, `NodeService`, or
   `ClusterMetadataService` today should notice the gateway exists,
   independent of whether the gateway itself is healthy.

## 8. Proposed system architecture

```
Application / API client
        |
        v
+-----------------------------------------------------------+
|  PulseKV Context Gateway  (new component, Python, gateway/) |
|                                                             |
|  decompose(request) -> blocks[]                             |
|      |                                                       |
|      v  (per eligible block; ineligible/user-query blocks   |
|      |   pass through untouched, unreordered)                |
|  resolve(block):                                             |
|      1. exact-hash lookup (registry, <1ms, in-proc cache)     |
|      2. structural-normalization lookup, IF block is a        |
|         structured type (tool schema JSON etc.) -- §12       |
|      3. embedding + candidate retrieval, IF neither above hit  |
|      4. equivalence guard on the top candidate                 |
|      5. accept -> canonical text substituted                   |
|         reject/miss/error/timeout -> ORIGINAL block, unchanged |
|      |                                                          |
|  assemble(blocks) -> forwarded request (order preserved,        |
|                       nothing reordered -- see 14)               |
+-----------------------------------------------------------+
        |
        v
SGLang / vLLM  (UNCHANGED -- no code, no config beyond a normal
                 reverse-proxy front-end, points at the gateway
                 instead of directly at the app)
        |
        v  (block-hash keyed get/exist/set, via
        |   pulsekv_adapters.sglang / .vllm -- UNCHANGED)
        v
PulseKV v2 cluster (UNCHANGED -- node/engine, node/grpc_shim,
                     control/, proto/ all exactly as built)
```

The gateway is a new process the application points at instead of the
inference server directly (or a header/route rule an existing reverse proxy
dispatches through) — not a patch to SGLang, vLLM, or anything in
`adapters/`. `pulsekv_adapters.client.PulseKVClient` is imported by the
gateway's own registry-lookup path only for the exact-hash tier (§10's Tier
0), not for anything resembling the inference request itself; the gateway
never calls `NodeService` for KV tensor data and never constructs a block
hash — that stays exclusively the job of the unmodified SGLang/vLLM
adapters, downstream of the gateway, per Finding 2.

## 9. Component boundary evaluation

Evaluated fresh against the current source, not accepted from the prior
report:

| Candidate | Verdict | Why (source-grounded) |
|---|---|---|
| **A. `node/engine/` (C)** | Reject | `node/engine/README.md`'s own stated rule: "No gRPC, no C++, no protobuf" — enforced by CMake include-path separation, not convention. A Python embedding/vector stack has no way to live here without violating a boundary the codebase actively compiles against. |
| **B. `control/` (Go)** | Reject | `control/internal/metadata/service.go` and the v2 design doc are explicit: Raft is reserved for low-volume cluster metadata, off the data write path. A registry read on every gateway request is not that. Gossip/Raft libraries have no vector-search primitive and adding one couples cluster liveness to an unrelated subsystem. |
| **C. `adapters/pulsekv_adapters/` (existing modules)** | Reject | Confirmed by reading `sglang.py`/`vllm.py`: SGLang's `RadixCache`/HiCache and vLLM's `BlockManager` construct block hashes from *already-tokenized* prompts before calling into the adapter. Canonicalizing inside the adapter would need to un-tokenize, rewrite, and re-tokenize after the engine has already committed to a token sequence and block layout — strictly harder and strictly later than doing it before the engine ever sees the text. |
| **D. Client SDK (a new SDK layer)** | Reject | Forces every calling application to embed an embedding model and vector index locally, and is incompatible with the common case of an application built against a standard OpenAI-compatible client that has no PulseKV awareness at all. |
| **E. Ingress gateway (new component)** | **Accept** | Sits exactly at the one point in the request path where the full, structured, pre-tokenization prompt is visible and mutable, and where SGLang/vLLM/PulseKV are all still standard, unmodified deployments on the other side. This is also where the prior report landed (§3 of that report) — reaffirmed here against source, not merely against design docs. |
| **F. Separate always-on service called by applications directly (not inline proxy)** | Partially folded into E | A viable deployment shape for the same logic (§8's gateway as a sidecar/library call rather than a network hop) — not a different architectural answer, a different packaging of E. Left as a Phase 10.5 deployment decision, not a boundary decision. |
| **G. Hybrid gateway + registry** | **Accept, refines E** | The registry (§17) is intentionally a separate component from the gateway process's request-handling code, for the durability reason given there — but it is not a different *boundary*, it is a dependency the gateway calls, same relationship the gateway has to PulseKV itself. |

## 10. Registry design

**Name:** *PulseKV Context Registry*. Rejected: "cache" (it stores decisions
about text, not approximate KV state — the master prompt's own naming
guidance in §29 applies directly) and "semantic cache" for the same reason.

**What it answers:** given an incoming (namespace-scoped) block of text, is
there a registered canonical context this block is equivalent to, and at what
confidence/method?

**Record shape**, adapted from the master prompt's §8 sketch, with the fields
actually load-bearing for the invariants in §7 kept, and speculative fields
(e.g. `tokenizer_constraints`) deferred until a real need is shown:

```
context_id:        string, human-assigned (e.g. "github-agent-policy")
version:            monotonically increasing integer, IMMUTABLE once published
namespace:           tenant/org scope -- see §15, participates in retrieval
                      BEFORE similarity, not as a post-filter
canonical_text:      the exact string substituted on a match
content_hash:        SHA-256 of canonical_text, used for the exact-hash tier
embedding:            precomputed vector for canonical_text, model-versioned
                      (embedding_model_id + embedding_model_version fields --
                      a registry entry embedded with model A is not a valid
                      candidate when the gateway is running model B)
block_type:           one of the §12 taxonomy's eligible types
safety_class:         reserved, unused in the MVP -- not fabricated content
aliases:               deterministic exact-match strings that resolve to this
                      context_id with method=alias, no embedding involved
created_at / created_by / deprecated_at:  audit trail
```

**Immutable versions, mutable pointer.** A `context_id` can have many
`version`s; each version's `canonical_text` and `content_hash` never change
once published (this is what makes the exact-hash tier and the audit trail
in §21 meaningful — a hit against version 4 always means the same substituted
text, forever). "Updating a canonical context" publishes a new version;
callers resolve `context_id` to whatever version is marked current, but
existing gateway decisions logged against version 4 remain interpretable
after version 5 exists. This directly answers investigation question 15.

**Storage.** Not PulseKV itself — flagged explicitly in the master prompt
(§17) as the wrong default, and confirmed wrong against source: PulseKV's own
`node/engine/README.md` states its NVMe tier is loss-tolerant by design ("the
tier is purged at startup and shutdown... losing a spilled value on crash is
fine — this is a cache"). A registry entry is not loss-tolerant — losing one
silently degrades every future request that would have matched it back to
"no match, fall through," which is safe per §7.3, but losing one *and it
coming back different* would not be. Recommendation: an ordinary relational
store (Postgres, or SQLite for the MVP's realistic scale — see next
paragraph) that the gateway process treats as a required, fail-closed
dependency, not PulseKV's fault-tolerant engine.

**Realistic scale.** Not benchmarked — no registry exists yet. But bounded by
what MVP §5's non-goals leave in scope: registered, curated context blocks
(tool policies, org policies, RAG document templates), not arbitrary
user-generated content. This is a curation-gated, human-reviewed set by
construction (§14 of the master prompt's guardrail requirement and §32's
"operator review" loop both assume this). Estimate, explicitly labeled as
such: low hundreds to low thousands of entries for a single organization's
deployment, not millions. This estimate should be validated against Phase
10.1's actual usage before any vector-index technology choice more complex
than brute-force cosine similarity over an in-memory array is justified —
brute force over a few thousand 384-dimensional vectors is sub-millisecond on
any modern CPU and needs no new dependency; this is deferred to be measured
in Phase 10.3, not assumed here.

## 11. Matching pipeline

Four tiers, cheapest and most deterministic first, each one a strict filter
before the next is even attempted — not parallel races:

**Tier 0 — exact hash.** `SHA-256(normalized_block_text)` against
`content_hash` in the registry (and against registered `aliases`). Zero
embedding cost, zero ambiguity, method recorded as `exact` or `alias`. This
alone captures the "same organization re-sends byte-identical blocks with
incidental whitespace differences" case — normalize whitespace/casing
deterministically before hashing, never normalize meaning.

**Tier 1 — structural normalization**, only for block types with real
structure (§12): a tool-schema JSON block is parsed and re-serialized in a
canonical key order/whitespace before Tier 0's hash is computed, rather than
run through embeddings at all. This is not a separate pipeline stage so much
as a pre-processing step *before* Tier 0 for structured types — grouped here
because the master prompt (§12) calls it out as its own decision point, and
because it is the tier with the strongest correctness guarantee after exact
match: it changes zero semantic content, only serialization form.

**Tier 2 — embedding + candidate retrieval.** Only reached on a Tier 0/1
miss. Encode the block, retrieve the top-K nearest registered canonical texts
*within the request's namespace* (§15 — namespace filters before, not after,
similarity ranking). This tier produces *candidates*, never a decision — the
master prompt is explicit (§10) that embedding similarity must not be treated
as equivalence, and this design does not weaken that.

**Tier 3 — equivalence guard.** Runs only against Tier 2's top candidate (or
top-K if Tier 3 rejects the first and a second is worth trying — MVP: just
the top-1, escalate only if Phase 10.4's evaluation corpus shows a real need
for more). Never runs on Tier 0/1 hits, which need no guard by construction.
Detailed in §12 below.

**Rejected as MVP tiers, with reasons tied to the master prompt's own
comparison table (§7):**

- *Intent classification* — coarse by nature; collapses exactly the nuance
  (negation, parameter values) the guard in §12 exists to catch. Not a
  candidate-retrieval improvement over embeddings for this task.
- *Cross-encoder reranking* — real accuracy improvement, real added latency;
  deferred to a Phase 10.4+ evaluation, not assumed necessary for MVP scope
  (curated, low-thousands registry — Tier 2's candidate set is already small
  enough that a cross-encoder's benefit over a good guard is unproven at this
  scale).
- *Small-LM rewriter* — rejected outright, not deferred. A model call in the
  hot path both reintroduces the latency this feature exists to avoid *and*
  introduces exactly the class of failure (hallucinated rewrite) the
  equivalence guard exists to prevent. There is no version of "trust an LLM
  to rewrite text losslessly" that is compatible with §7's invariants.

## 12. Equivalence validation

The hardest correctness problem in this design, and the one the master
prompt is most insistent about (§11, §24, §26).

**Status: implemented and measured (Phase 10.4, `gateway/pulsekv_gateway/
guardrail.py`).** The three checks below and the τ value are no longer
hypothetical — see `docs/pulsekv-semantic-context-phase10.4-summary.md` for
full methodology. Two departures from the original text of this section are
kept, both driven by corpus evidence rather than by preference, and both
recorded where they happened:

**What the guard checks**, run only against a Tier 2 candidate that already
cleared τ = 0.90 (derivation below) — as built, type is checked first
(cheapest, most decisive), then polarity, then entities, the latter two
sharing one tokenization pass:

1. **Negation/polarity.** Deterministic check for negation and exception
   markers (`not`, `never`, `without`, `except`, `excluding`, `unless`, and
   contractions — matched by the `n't`/`n't` suffix rule, not a fixed list,
   so it also covers terms not yet written into any list) plus families
   extending §12's original six to reach failure modes named elsewhere in
   the master prompt (before/after, above/below threshold, allow/deny,
   required/optional). **As built, this compares family *counts*
   (multiset), not presence/absence as this section originally specified.**
   The corpus contains a pair where both texts contain "Never" and only the
   count differs — a second prohibition added to one clause — which a
   presence-only comparison passes and a multiset comparison correctly
   refuses. This check runs before similarity is consulted at all, so a
   negation mismatch is logged as `negation_mismatch`, never as a
   `low_similarity` rejection that would hide why the pair was dangerous.
2. **Entity/value preservation.** **As built, this requires set equality,
   not the superset-or-equal rule this section originally specified.** The
   corpus contains the case that makes the difference concrete: "Delete
   unused resources in staging" against a registered "…in staging and
   production" satisfies superset-or-equal (the candidate's entities are a
   superset of the incoming block's) and would silently extend a
   staging-scoped deletion to production. Equality closes that; it costs
   nothing the corpus has found a legitimate use for yet.
3. **Structural-type consistency.** As specified — confirmed redundant with
   Tier 2's own namespace/type-scoped retrieval, and kept anyway as a direct
   guard-level guarantee rather than relying on Tier 2 alone.

**τ = 0.90, derived from the adversarial suite, not asserted.** The
corpus's 25 adversarial-negative examples: 19 are refused by the polarity or
entity check with no similarity score even consulted; the remaining six are
the "guard-blind" class the checks above don't catch by construction, topping
out at 0.8187 similarity. τ = 0.83 would also produce zero false positives on
this corpus and matches one more positive example (12/13 vs. 9/13) — deliberately
not chosen, because fitting a threshold 0.011 above the highest member of a
six-example class fits that class's examples, not the class it's drawn from.
τ cannot do the safety work alone regardless of where it's set: positive
similarity spans 0.8462–1.0000, adversarial spans 0.1333–1.0000, and 17 of 24
adversarial pairs outrank the lowest genuine paraphrase — this is the
concrete evidence, not a general embedding-similarity caveat, for why §11
treats Tier 2 as candidate retrieval only and never as a decision.

**Failure bias.** Every check in this section is a reject-biased gate: any
signal of doubt (negation mismatch, entity mismatch, type mismatch, guard
error, guard timeout) routes to "use the original block, unmodified" — the
same fallback path as a registry miss. There is no "reduced confidence"
substitution mode.

## 13. Context decomposition and granularity

Decompose per the block taxonomy already used in the master prompt (§12,
§13), evaluated for MVP eligibility against §5's non-goals:

| Block type | MVP eligible? | Matching approach |
|---|---|---|
| `SYSTEM_PROMPT` | Yes | Tier 0/2/3 |
| `TOOL_SCHEMA` | Yes | Tier 1 (structural) then Tier 0 |
| `TOOL_POLICY` | Yes | Tier 0/2/3 |
| `ORG_POLICY` | Yes | Tier 0/2/3 |
| `AGENT_INSTRUCTION` | Yes | Tier 0/2/3 |
| `RAG_DOCUMENT` | Yes, if pre-registered (see below) | Tier 0/2/3 |
| `REPOSITORY_CONTEXT` | Deferred | needs its own eligibility study — highly variable structure |
| `FEW_SHOT_EXAMPLES` | Deferred | ordering/count sensitivity not yet analyzed |
| `CONVERSATION_HISTORY` | **No** | Per-user, not reusable across requests by construction; canonicalizing it has no cache-hit benefit and real leakage risk (§15) |
| `USER_QUERY` | **No, never** | Master prompt §3's core constraint; not revisited by this design |

`RAG_DOCUMENT` eligibility is narrower than it looks: only documents that are
*registered* (i.e., a known, stable corpus entry with its own `context_id`)
are eligible. An ad hoc retrieval result assembled fresh per query is not
"canonicalizable" in any safe sense — there is nothing stable to register it
against, and treating live retrieval output as a match candidate reintroduces
exactly the free-form-text risk §3/§5 exclude.

Granularity is per-block, not whole-prompt and not sub-block (individual
policy clauses) in the MVP — whole-prompt is too coarse (one changed word
anywhere invalidates the entire match, per §7.3's Failure Mode 3 example) and
sub-block decomposition is a real future direction (master prompt §12) but
adds a decomposition-correctness problem of its own that the MVP does not
need to solve to capture the bulk of the benefit described in §3.

## 14. Prompt assembly and ordering

The gateway assembles the forwarded request by substituting canonicalized
text **in place**, block for block, preserving the application's original
block order exactly. It does not reorder blocks to "maximize cache reuse,"
even though the master prompt (§14) raises this as worth investigating,
because reordering is a second semantics-changing operation layered on top
of canonicalization, and this design has exactly one such operation it is
willing to make safety claims about. An application that assembles its
prompt with dynamic content before static content gets no prefix-caching
benefit from this gateway (or from PulseKV's exact-match caching, with or
without this gateway) — that is a limitation of the application's own prompt
construction, correctly out of this design's scope to silently fix.

## 15. Tenant isolation

Namespace is a first-class field on every registry record (§10) and
participates in Tier 2 candidate retrieval as a **pre-filter**, not a
post-hoc check on the winning candidate — the master prompt is explicit
(§15) that this ordering matters, and this design follows that: the
candidate set handed to the equivalence guard in §12 is already
namespace-scoped before any similarity comparison runs, so a cross-tenant
match is not a low-probability event guarded against after the fact, it is
structurally impossible to construct given how the candidate set is built.
Namespace resolution (which namespace a given incoming request belongs to)
is an application/deployment-layer concern the gateway takes as an input
(e.g., an API key, a routing rule) — not something this design invents; it
reuses whatever tenant identity the application already established before
the request reached the gateway.

## 16. Model and tokenizer identity

Per Finding 2 (§6), PulseKV's own keys need no new fields — SGLang and
vLLM's existing block-hash derivation already produces different keys for
different models/tokenizers/dtypes, because it hashes actual tokens, and
different tokenizers produce different tokens for the same text. This
answers investigation question 16 directly: **no**, PulseKV does not need new
cache-key fields, and this design does not add any.

What *does* need model/tokenizer awareness is the registry's `embedding`
field (§10) — an embedding computed for one embedding model is not a valid
similarity comparison against a different embedding model's output, and this
is tracked (`embedding_model_id`/`version`) purely as a Tier 2
candidate-retrieval correctness concern, entirely internal to the gateway/
registry, invisible to PulseKV.

## 17. Failure model

Fail-open is the only mode considered, per the master prompt's explicit
default (§21) and per Finding 3's precedent (§6) of `metadata.Service`'s own
fail-closed-for-*its own answers*, fail-open-for-*its callers'
availability* pattern — the same shape, applied at the gateway boundary
instead of the metadata-service boundary:

| Failure | Gateway behavior |
|---|---|
| Registry unreachable | Every block treated as a miss; original text forwarded unchanged. Logged, not silently swallowed. |
| Embedding encoder unavailable/slow past a budget | Tier 2/3 skipped for that request; Tier 0/1 (if already resolved) still apply; anything not resolved by Tier 0/1 passes through unchanged. |
| Equivalence guard errors or times out | Treated as reject, not accept — consistent with §12's reject-biased default. |
| Gateway process itself down | Deployment-dependent (§8's E vs F packaging), but the requirement is the same either way: applications must have a working path to SGLang/vLLM that does not depend on the gateway being up. This is a deployment/routing decision for Phase 10.5, not a gap in this design — flagged explicitly so it is not lost. |
| Stale registry (a version was just deprecated) | A deprecated version simply stops being served as a match target for new decisions; already-issued decisions are not retroactively invalidated (nothing to invalidate — the substituted text was already sent). |

No mode in this table makes PulseKV's own availability depend on the
gateway. `node/engine/`, `node/grpc_shim/`, and `control/` have no gateway
dependency to fail closed or open on — they simply never hear about it,
which is the point of Finding 1/2 (§6).

## 18. Observability

Adopting the master prompt's metric list (§22) with light editing for
naming-consistency with existing PulseKV metrics (`pulsekv_*`, matching the
convention `control/internal/promexport/promexport.go` already
established in Phase 9):

```
pulsekv_semantic_requests_total{status="processed|bypassed"}
pulsekv_semantic_bypass_total{reason="below_min_tokens|ineligible_block_type|disabled"}
pulsekv_semantic_candidates_total
pulsekv_semantic_match_total{method="exact|alias|structural|semantic"}
pulsekv_semantic_reject_total{reason="low_similarity|negation_mismatch|entity_mismatch|type_mismatch|guard_error|guard_timeout"}
pulsekv_semantic_lookup_latency_seconds{tier="exact|structural|embedding|guard"}
pulsekv_semantic_similarity_score (histogram)
pulsekv_canonical_context_hits_total{context_id,version}
pulsekv_canonical_tokens_total
pulsekv_semantic_fallback_total
pulsekv_semantic_error_total{component="registry|encoder|guard"}
```

Correlating a semantic decision to an actual PulseKV/SGLang/vLLM cache hit
(master prompt §22's closing requirement) needs one more field than the
prior report's list had: the gateway should log its own decision (block,
method, context_id/version, accept/reject) with enough of a correlation key
(request ID) that it can be joined against the *existing* Phase 9
`pulsekv_cache_hits_total`/`pulsekv_cache_misses_total` series — this is a
join across two already-existing observability surfaces, not a new metric
PulseKV itself needs to expose.

## 19. Performance model

The master prompt (§40) requires labeling any unmeasured number as a
hypothesis. Everything in this section is a hypothesis until Phase 10.3/10.8
produce real numbers; nothing here should be read as a target the design
already meets.

```
T_gateway   = T_decompose + T_registry_lookup + T_embed (Tier 2 only)
              + T_vector_search (Tier 2 only) + T_guard (Tier 3 only)
T_saved     = T_prefill_avoided - T_pulsekv_exact_cache_transfer
Worth it when: T_saved > T_gateway
```

Two real, measured numbers from this repository bound one side of that
inequality — not the gateway side (nothing exists to measure yet), but the
"what PulseKV itself already costs to serve an exact hit" side, which is the
floor this feature's overhead needs to clear:

- SGLang cross-replica demo (`docs/pulsekv-v2-phase7-summary.md`, Benchmark 1,
  10 trials): **10.38 ms average** Replica-B lookup+read for a 512-token/32-page
  shared prefix, already an exact-cache hit end to end. This is real,
  measured, and it is the number a hypothetical semantic-match path would be
  *adding to*, not competing with — the gateway does not replace this cost,
  it sits in front of it.
- vLLM cross-replica demo (`docs/pulsekv-v2-phase8-summary.md`): scheduler
  match **5.28 ms average**, worker layer load **79.60 ms average** across 16
  layers for the same 512-token prefix.

No prefill-recompute baseline (the other side of `T_saved`) exists in this
repository — v2's benchmarks measure PulseKV's own cost, not a real model's
prefill cost, because v2 deliberately never stood up real GPU inference
(Phase 7/8's "real SGLang/vLLM integration" tested the adapter/storage path,
not GPU-bound generation economics). **This means the single most important
number for justifying this feature — how much prefill time a real model
actually avoids — does not exist yet anywhere in this codebase's history.**
Phase 10.8's benchmark is not optional polish; it is the first time this
project will have measured the actual justification for building any of
this. Recommendation: do not treat Phase 10's business case as proven until
that benchmark exists. The prior report's break-even table (its §11) should
be treated as illustrative motivation, not evidence, until then.

**Runtime bypass policy** (the master prompt's §20 "if reusable_context_tokens
< threshold: bypass" sketch): the mechanism (a configurable minimum eligible-
block token count below which the gateway skips straight to forwarding
unchanged) is worth building from Phase 10.5 on, because it is cheap and
obviously never harmful. The *threshold value* is explicitly not hardcoded
from the prior report's unsupported 512-token figure — it ships as a
configuration default pending Phase 10.8 data, not as a design conclusion.

## 20. Security and privacy

- Audit records (§21 below) store `content_hash` and `context_id`/`version`,
  not raw prompt text, by default — consistent with the master prompt's
  privacy-preserving-audit guidance (§31).
- The registry itself necessarily stores raw `canonical_text` (it has to, to
  substitute it), which makes the registry a store of potentially sensitive
  organizational content (tool policies, internal RAG documents) — this is
  the practical reason §10 recommends a real database with normal
  access-control expectations, not an ad hoc file store.
- Namespace-scoped retrieval (§15) is the tenant-isolation control; there is
  no cross-namespace fallback path in this design — a namespace with no
  registered contexts simply never gets a Tier 2 match, which is correct
  fail-open behavior, not a gap.

## 21. Auditability

Per request, per eligible block, log (not the full prompt by default, per
§20): request ID, block type, original-block content hash, decision
(`bypassed|exact|alias|structural|semantic|rejected`), matched
`context_id`/`version` if any, similarity score if Tier 2 ran, guard outcome
and reason if Tier 3 ran, tenant/namespace, model, timestamp. This is the
record that lets someone later answer "what did the gateway actually send to
the model for this request" without re-deriving it from logs scattered across
components — a single, structured decision log is a Phase 10.2 deliverable
(alongside the registry it logs against), not deferred to Phase 10.9.

## 22. Alternatives rejected

Covered with reasoning inline above; summarized for reference:

- Approximate KV tensor reuse (§7, §26 of master prompt) — mathematically
  unsound given causal attention and RoPE positional binding; not a
  scope choice.
- Semantic logic inside `node/engine/`, `control/`, or the existing adapters
  (§9, options A/B/C) — each violates a boundary the current codebase
  actively enforces, not merely a stylistic preference.
- Token-level canonicalization instead of text-level (master prompt §9) —
  rejected because it would couple the gateway to specific
  tokenizers/models, when the whole point (Finding 2, §6) is that the
  existing engines' own tokenizers already do the right thing once given
  canonical text; producing tokens ourselves duplicates that work and adds a
  drift risk if a gateway-side tokenizer ever disagrees with the engine's.
- Small-LM rewriter (§11 above) — rejected outright on both latency and
  correctness grounds, not deferred as a "Phase 2 nice-to-have."
- Silent prompt reordering (§14) — rejected as an unproven second
  semantics-changing operation stacked on the one this design already makes
  careful claims about.

## 23. Research extensions (explicitly not MVP)

Per the master prompt's §33 instruction to keep these separate: approximate/
non-prefix KV composition (CacheBlend-style selective recomputation),
learned KV adapters, cross-model KV reuse, and the "learning loop" from
master prompt §32 (auto-suggesting new aliases from repeated near-miss
traffic for human review) are all real future directions and none of them
are assumed, scoped, or scheduled by this design or its implementation plan.

## 24. Success criteria

Restated from the master prompt's §39, unchanged — this design does not
water them down, and does not consider the feature validated until real
SGLang/vLLM inference (not just the storage-path demos v2 already has)
demonstrates cross-replica reuse *through* semantic canonicalization, with
adversarial near-matches reliably rejected, cross-tenant matches structurally
impossible (not merely untested), gateway failure verified to fall back to
today's exact-only behavior, and all of v2's existing exact-cache tests still
green throughout.
