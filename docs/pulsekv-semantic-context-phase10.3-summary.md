# PulseKV v3 / Phase 10.3 — Semantic candidate retrieval (Tier 2)

**Status:** complete. A block that misses Tiers 0 and 1 is now embedded and
ranked against the registered contexts in its own namespace and block type.
Nothing it finds becomes a match: Tier 2 produces candidates, and the guard
that could accept one is Phase 10.4's.

**Scope actually touched:** `gateway/` only. `git diff --stat -- src include
tests node control proto adapters` is **empty**, and so is `git status
--porcelain -- gateway/tests/corpus`.

---

## 1. The §3 gate — resolved, on evidence. The waiver chain ends here.

Phases 10.0, 10.1 and 10.2 each ran under an explicit waiver because the
confirmation soak had not finished. **It has, and it passed.**
`deploy/run/soak-report.json`, written 2026-08-19:

```json
"verdict": { "result": "healthy", "problems": [], "error_rate": 0.0307,
             "dead_intervals": 0, "longest_dead_interval_run": 0,
             "intervals_evaluated": 90 }
"fault_injection": { "chaos_enabled": true, "chaos_interval_seconds": 45,
                     "crash_cycles": 74, "injectors_started": 1,
                     "distinct_cycle_numbers": 74 }
```

| | |
|---|---|
| Duration | **5400.05 s** — the full 90 minutes, not a truncated run |
| Operations / verified | 17,691,636 / 13,062,930 at 3,276 ops/s |
| **Value mismatches** | **0** — no read ever returned bytes that were not written |
| **Dead intervals** | **0 of 90.** The lowest any interval verified was 58,725 reads. This is the exact condition that ran for 75 minutes in the incident |
| Injectors started | **1**, against three interleaving on one cluster in the incident |
| Crash cycles | 74, all recovered — "once a node was down it could not be brought back" did not recur |
| Error rate | 3.07%, against `soak-verdict.py`'s 50% ceiling; chaos-induced and clustered on crash/restart |
| Read p99, worst interval | 96.7 ms under active node crashes |

Every link of the original failure chain is now covered by a passing check, and
the fix set (`deploy/test-lifecycle.sh`, 8 checks, 6 of which fail against
pre-fix code) is regression-gated in `make test`. **Phase 10.3 is the first
phase of Phase 10 to run with the gate genuinely closed rather than waived.**

**One thing left undone, deliberately, and it is not this phase's to do.**
`docs/pulsekv-v2-soak-collapse-analysis.md` §10 ("The fresh long-duration
soak") is *still* the unfilled placeholder `<!-- FRESH_SOAK_RESULTS -->`, and
`pulsekv-v2-progress-report.md` §4.2 still carries only the old Phase 9.4
figures that the analysis doc's own §9 flags as unreconciled. The results above
are the content those two sections are waiting for. Writing them into a v2
document is Phase 9.x work, outside this phase's scope boundary — recorded here
so the gap is visible rather than rediscovered.

---

## 2. Step 10.3.1 — the model and index decisions, with the measurements

### Embedding model: `sentence-transformers/all-MiniLM-L6-v2`, ONNX, CPU

| Reason | Detail |
|---|---|
| **384 dimensions** | The exact width design doc §10 already assumed when it argued brute-force cosine needs no index at MVP scale ("a few thousand 384-dimensional vectors") |
| **6 layers, 22M params** | The fastest widely-validated sentence encoder. Speed is the point: §19 names the gateway-cost-versus-prefill-saved question as the single most important unmeasured number in the project, and this phase starts answering it. A stronger, slower encoder would have made that number worse; a static-embedding model would have flattered it while handing the guard weaker candidates |
| **Apache-2.0, ONNX published in-repo** | The artifact is reproducible by URL with no conversion step |

**The fp32 export is used, not the int8 one, and that is a correctness choice.**
The repository also ships `model_qint8_arm64.onnx` and `model_quint8_avx2.onnx`,
which are faster. Quantized kernels differ per architecture, so a registry
populated on an arm64 host and read by an x86 worker would compare vectors that
were never comparable — silently. That is precisely the drift design doc §16
and risk register row 6 exist to prevent, and the registry caches embeddings
across restarts and across machines, so determinism is worth more than the
speed here.

**Named upgrade trigger, not an open question:** if Phase 10.4's corpus shows
Tier 2 *recall* is the binding constraint — true paraphrases never reaching the
guard — `BAAI/bge-small-en-v1.5` is the same 384 dimensions with better
retrieval quality at roughly twice the layers. A swap invalidates every stored
embedding by design (§16), so it should follow evidence, not intuition.

### Registry scale: plan §7 asked whether §10's estimate holds. It cannot be checked.

Plan §7 asks for the index decision to be "resolved against Phase 10.1's actual
registry population size rather than assumed". Checked, and the honest answer
is that **no real population exists**: the registry has been live for two
phases, no gateway process is running (Phase 10.5), and every row in existence
is a synthetic test fixture. Design doc §10's "low hundreds to low thousands"
remains an estimate, and this phase measured *across* that range and past it
rather than pretending to validate it.

### Index: brute-force cosine, no ANN library — and a real defect found proving it

Design doc §10 predicts brute force is "sub-millisecond on any modern CPU" at
MVP scale. The first implementation here scored each record with a Python loop
over 384 floats and was **not** sub-millisecond:

```
Retrieval only, per-record Python loop        Retrieval only, one matrix-vector product
  records   mean ms     p50     p95             records   mean ms     p50     p95
      200     3.178   3.152   3.331                 200     0.183   0.168   0.237
     1000    16.462  16.366  17.812                1000     0.822   0.797   0.923
     5000    80.106  79.672  83.318                5000     4.895   4.590   8.286
                                                  20000    21.502  20.102  28.925
```

§10's claim is correct — but it is a claim about *vectorized* arithmetic, and a
naive reading of "brute force" produces something 17–20× slower. Each bucket
now keeps its vectors as one contiguous `float32` matrix and scores a query
with a single BLAS call. At the scale §10 estimates, retrieval is **0.18 ms at
200 records and 0.82 ms at 1,000** — sub-millisecond, as predicted, once the
implementation matches the prediction.

**No ANN library is added.** At 20,000 records — an order of magnitude past
§10's estimate — brute force still costs 21.5 ms against an encode that costs
4.7–87 ms for the same request. The search is not the bottleneck anywhere near
the scale this feature targets, and an approximate index would trade exactness
for time there is no evidence is needed.

`encoder.cosine_similarity` remains the reference definition in pure Python;
`TestScoringAgreesWithTheReference` asserts the matrix path matches it to
1e-6 (float32 accumulation against float64 — measured delta ~1e-8).

---

## 3. Step 10.3.6 — the benchmark gate, reported honestly

First real numbers in this project for
`pulsekv_semantic_lookup_latency_seconds{tier="embedding"}`. Apple Silicon,
CPU only, `onnxruntime==1.29.0`, single block per call:

```
Encode (the dominant cost)                    Tier 2 total at 1,000 records
block              tokens  mean ms   p95        block              encode + retrieve = total
short                  18     4.67    8.01      short                4.67 + 0.82  =   5.5 ms
medium                 98    11.44   12.48      medium              11.44 + 0.82  =  12.3 ms
long (truncated)      512    86.74  123.19      long (truncated)    86.74 + 0.82  =  87.6 ms
```

**This is materially worse than the figures this project was told not to
trust.** The superseded report asserted 2.5–6 ms for semantic lookup with no
benchmark behind it; design doc §19 explicitly flags those numbers as not
evidence. Measured, Tier 2 costs **5.5 ms for a short block and ~88 ms for a
long one**, and the long case is the one this feature exists for — §19's own
bypass threshold is stated at 512 tokens, which is exactly where the encoder
saturates.

For scale, the only other real numbers in the repository: SGLang's cross-replica
exact-cache hit costs **10.38 ms** end to end (§19, Phase 7 benchmark). Tier 2
on a long block is roughly **8× that**, and it is spent *before* any cache is
consulted, on a request that may still not match.

**What this does and does not say.** It does not say the feature is
net-negative: the other side of §19's inequality — how much prefill a real
model actually avoids — still does not exist anywhere in this codebase, and
Phase 10.8 is the first time it will. It does say three things concretely:

1. Risk register rows 8 and 9 are now partly bounded on the cost side, and the
   cost is larger than the superseded report implied.
2. The runtime bypass policy (§19) matters more than it looked. Blocks short
   enough to be cheap to embed are the ones least worth canonicalizing.
3. **Tier 0/1's short-circuit is worth far more than it appeared.** A
   deterministic hit avoids the entire cost above, which makes Phase 10.2's
   ordering an economic property, not only a correctness one.

---

## 4. Implementation

| Module | What it is now |
|---|---|
| `encoder.py` | `Encoder` base (budget, dimension check), `OnnxEncoder`, `vector_to_bytes`/`vector_from_bytes`, `cosine_similarity` |
| `index.py` | `VectorIndex` — bucketed storage, matrix scoring, model-identity enforcement, `build_from_registry` + `BuildReport` |
| `matcher.py` | `Matcher.try_semantic`, `Matcher.resolve_with_candidates`, Tier 2 wired at Phase 10.2's marked insertion point |

### Namespace and block type are not filters

Design doc §15's "structurally impossible" claim is implemented as
**partitioning, not filtering**: vectors live in buckets keyed by
`(namespace, block_type)`, and a query loads one bucket. There is no ranked
list containing another tenant's record to filter out — the comparison is never
performed. A filter can be forgotten, reordered, or short-circuited by a later
edit; a bucket that was never read cannot leak.

`TestNamespaceIsolation` attempts the leak rather than exercising the filter:
byte-identical policy text is registered in two namespaces, asserted to produce
**identical embeddings**, and each namespace's query is shown to return only its
own record at similarity 1.0 while the other's vector demonstrably exists in the
index (`len(index) == 2`). A third namespace gets nothing.
`TestBlockTypePreFilter` does the same across block types: a `TOOL_SCHEMA`
query whose text scores 1.0 against a registered `ORG_POLICY` returns only the
schema.

### Model-identity enforcement (risk register row 6)

Row 6 names the location — "enforce the version check in `Index.top_k`, not
just document it". The as-built name Phase 10.0 froze is
`VectorIndex.find_candidates`; the check is there, applied **before any
arithmetic**, and also at `add`:

- `add` raises `IndexModelMismatchError` for a record stamped with another
  model — an explicit call with a stale record is a programming error.
- `find_candidates` re-checks every record in the bucket, so an encoder swapped
  underneath a warm index cannot serve stale vectors. Tested by reaching into
  the bucket to create exactly that state, because `add` refuses to.
- `build_from_registry` **skips and counts** rather than raising:
  `BuildReport` separates `without_embedding`, `model_mismatched`, `malformed`
  and `registry_errors`. Row 6 says such an entry is "treated as no embedding
  available"; a build that died on the first stale row would take the tier down
  for one bad record, which is the opposite of §17. The counts are the
  operator's only warning that a half-finished re-embedding left the index
  empty — `model_mismatched == seen` is what that looks like.

`model_version` is **derived from the artifact, not asserted**:
`ENCODER_REVISION + sha256(model.onnx)[:16]`. Swapping `model.onnx` in place —
the drift a hand-written version string misses entirely — changes the version,
and every entry embedded with the old one stops being a candidate.
`ENCODER_REVISION` covers this module's own contribution (pooling,
normalization, truncation), which the weights digest cannot see.

### Two things the encoder must be fed correctly

**Text.** `try_semantic` embeds `normalize_for_hash`'s output — the same form
Tier 0 hashed. Embedding the raw block instead would give one block two
identities, one per tier, and the similarity Tier 3 reasons about would not be
the similarity of the thing Tier 0 looked up.

**Vectors.** The blob format Phase 10.1 left opaque is little-endian float32,
no header: the record's `embedding_model_id`/`_version` already pin the model
and therefore the width. `vector_from_bytes` refuses a wrong-width blob rather
than reading it as a shorter vector, and `VectorIndex.add` refuses a vector
whose norm is not 1.0 — cosine is scored as a dot product, which is only a
cosine for unit vectors.

---

## 5. Where the frozen contract could not do what the prompt asked

Step 10.3.4 asks that a retrieved candidate be recorded "including
`similarity`" in the decision log while the overall outcome stays
`NO_CANDIDATE`. **`models.py` forbids that combination**, verified rather than
assumed:

```
DecisionLogRecord(outcome=NO_CANDIDATE, similarity=0.97)
  -> ValidationError: similarity: must be unset when outcome=no_candidate
DecisionLogRecord(outcome=NO_CANDIDATE, context_id="c", version=1, similarity=0.97)
  -> ValidationError: context_id: must be unset ...; version: must be unset ...;
                      similarity: must be unset when outcome=no_candidate
```

The frozen `MatchOutcome` has no member for "a candidate was found but nothing
has validated it yet", because the contract was frozen assuming the full
pipeline: in it, a retrieved candidate always receives a verdict from the
guard. Phase 10.3 is an intermediate state Phase 10.0 had no reason to model.

**Resolved by not lying.** Three options were available and two were rejected:

- *Log it as `REJECTED` with `GUARD_ERROR`.* Contract-legal, and arguably
  defensible (§17 says a guard error is a reject, and a guard that does not
  exist is maximally unavailable). Rejected because it would put "phase not
  implemented" into `pulsekv_semantic_reject_total{reason="guard_error"}` and
  corrupt the very metric risk register row 5 relies on to prove fail-open works.
- *Widen `MatchOutcome`.* Changing the frozen contract to accommodate a
  temporary state is the opposite of what freezing it was for.
- **Chosen:** `resolve` returns `NO_CANDIDATE`, the log records it bare, and the
  candidates are returned through an additive
  `Matcher.resolve_with_candidates(block, namespace) -> (MatchResult, candidates)`.

The observability gap is real for one phase and **closes by construction** in
10.4: once the guard runs, a retrieved candidate becomes `MATCHED` or
`REJECTED`, and both carry `similarity` legally.
`TestDecisionLogUnderTier2` asserts the current behavior so it is not mistaken
for a bug.

### `Registry.find_candidates` was left raising, on purpose

The prompt permits removing its `NotImplementedError`. It was not removed, and
the message now points at the real location. Four reasons:

1. **Plan §7 — ground truth per the prompt — puts the interface on the index**,
   not the registry: `Index.top_k(vector, namespace, block_type, k)`.
2. **The as-built signature has no query vector**, and `Candidate` requires a
   similarity. Ranking without one is impossible; adding the parameter changes
   a Phase 10.1 signature the scope boundary protects.
3. **`registry.py` is pinned by its own Phase 10.1 test to standard-library
   imports only.** It cannot host vector math without breaking that pin or
   growing a second copy of the model-identity enforcement — and row 6 wants
   that logic in exactly one place.
4. **Phase 10.2's handoff already assigned this module the supporting role:**
   `list_records(namespace=..., current_only=True)` is the scan
   `build_from_registry` reads.

---

## 6. Two bugs the tests caught

**1. The encoder's typed-error guarantee only held when a budget was
configured.** `Encoder.encode` normalized exceptions to `EncoderUnavailableError`
inside the timeout branch only; with `timeout_ms=None` a raw model exception
escaped `encode`, sailed past `Matcher`'s `except EncoderError`, and came out of
`resolve` — which design doc §17 requires never to raise for an expected
failure. Found by `TestFailOpen`, which configures no budget. Fixed: every
failure leaves `encode` as an `EncoderError` on both paths.

**2. Three tests were constructing Tier 0 hits and calling them Tier 2 misses.**
They used `POLICY + "   "` as "text Tier 0 will miss" — but trailing whitespace
is exactly what `normalize_for_hash` removes, so Tier 0 hit and Tier 2 never
ran. The implementation was right. The fix required a real insight about
testing with a non-semantic stub: `StubEncoder` scores two *different* strings
near zero, so a "paraphrase" has to be constructed by embedding the record on
the incoming block's text (`make_record(..., embedding_text=...)`). A real
encoder produces that shape — different text, close vectors — without help.

---

## 7. Tests

**263 passing, 0 skipped** with the model present (256 passing, 7 skipped
without it):

```
test_models.py                94   (was 98; four parametrized stub cases retired
                                    with encoder/index)
test_registry.py              58   unchanged
test_deterministic_tiers.py   63   unchanged
test_semantic_retrieval.py    48   new
```

New suite by what it proves:

```
TestOnnxEncoder 6 · TestFailOpen 6 · TestEncoderContract 5
TestModelVersionEnforcement 5 · TestRetrievalIsNotDecision 5
TestNamespaceIsolation 4 · TestShortCircuitPreserved 4
TestScoringAgreesWithTheReference 3 · TestVectorSerialization 3
TestPhaseBoundary 3 · TestDecisionLogUnderTier2 2 · TestBlockTypePreFilter 1
TestLatency 1
```

The stub encoder lives in the test file, not the package, so a non-semantic
encoder cannot be configured into a deployment by accident. Tests needing the
real 90 MB model skip themselves when `PULSEKV_GATEWAY_MODEL_DIR` is unset.

**Reproducing:**

```bash
python3 -m venv /tmp/gwvenv
/tmp/gwvenv/bin/pip install pydantic==2.13.4 numpy==2.5.2 onnxruntime==1.29.0 \
    tokenizers==0.23.1 pytest==9.1.1
# weights: see gateway/README.md
PULSEKV_GATEWAY_MODEL_DIR=... PYTHONPATH=gateway \
    /tmp/gwvenv/bin/python -m pytest gateway/tests -q      # 263 passed
```

---

## 8. Exit criteria — verified

| # | Criterion | Evidence |
|---|---|---|
| 1 | `Encoder`/`Index` implemented to their stub signatures | Implemented as `Encoder.encode` and `VectorIndex.find_candidates` — the names Phase 10.0 froze. The prompt's `Encoder.embed` / `Index.top_k` are plan §7's names for the same methods; the as-built stubs won, per the prompt's own item 6. Additive: `dimension`, `count_tokens`, `max_sequence_tokens`, `close`, `build_from_registry` |
| 2 | Namespace and block-type pre-filtering proven by attempting a leak | `TestNamespaceIsolation` registers byte-identical text in two namespaces, asserts the embeddings are identical, and shows each query returns only its own at similarity 1.0 while both records are demonstrably in the index. `TestBlockTypePreFilter` does the same across types |
| 3 | Embedding-version mismatch enforced (row 6) | `TestModelVersionEnforcement`, 5 tests: `add` refuses; `build_from_registry` skips and counts; a vector whose stamps drift under a warm index vanishes from results; a malformed blob is counted, not raised; a deprecated version is not a target |
| 4 | A candidate never becomes a match | `TestRetrievalIsNotDecision`: a similarity-1.0 candidate still resolves to `NO_CANDIDATE`, `matched is False`, `substitutes is False`, `method is None`. Also: retrieval applies no threshold at all — a candidate scoring under 0.5 is still returned, because τ is 10.4's |
| 5 | Tier 0/1's short-circuit still holds | `TestShortCircuitPreserved` counts encoder invocations: a Tier 0 hit, a Tier 1 hit and an ineligible block each leave the count unchanged; a deterministic miss raises it by exactly one |
| 6 | A real measured latency number | §3. 5.5 ms (short) to 87.6 ms (long) at 1,000 records, reported against — and worse than — the superseded 2.5–6 ms claim |
| 7 | Fail-open on encoder unavailability/timeout | `TestFailOpen`, 6 tests: a broken encoder and an over-budget one both produce `ERROR`/`component=encoder`; the caller is released in <400 ms against a 500 ms delay; Tier 0 keeps working while the encoder is down; `resolve` never raises; half-configuring Tier 2 is refused outright |
| 8 | `git diff --stat -- src include tests node control proto adapters` empty | **Empty**, and `git status --porcelain` over the same paths too |
| 9 | `gateway/tests/corpus/` untouched | **Empty** `git status` |
| 10 | This summary | This document |
| — | No GPU dependency | `onnxruntime` (never `onnxruntime-gpu`), `numpy`, `tokenizers`, all pinned. `TestPhaseBoundary` AST-checks `encoder`/`index`/`matcher` against `torch`, `tensorflow`, `jax`, `cupy`, `onnxruntime_gpu` |

---

## 9. What Phase 10.4 can now assume — and the two findings it must act on

### The seam

`Matcher.resolve_with_candidates(block, namespace) -> (MatchResult, Tuple[Candidate, ...])`
is the guard's input. Candidates arrive **already** namespace-scoped,
block-type-scoped, model-identity-checked, non-deprecated, ordered by
descending similarity, and tie-broken deterministically by
`(context_id, version)` so a top-1 guard is reproducible run to run. Each
`Candidate` carries the full `CanonicalContextRecord` and its `similarity`.

The insertion point is the line immediately after `try_semantic` returns in
`Matcher.resolve_with_candidates` — marked, and directly analogous to the one
Phase 10.2 left for this phase. The guard turns `candidates[0]` into `MATCHED`
(method `SEMANTIC`, `guard_outcome=PASSED`) or `REJECTED` (with a reason), and
the §5 observability gap closes as a side effect.

### Finding 1 — no value of τ separates a paraphrase from its negation

Measured with the chosen encoder:

```
cos(policy, true paraphrase)                    = 0.7989
cos(policy, "Never delete" -> "Always delete")  = 0.9933
cos(policy, unrelated sentence)                 = 0.0276
```

**The meaning-inverting edit scores higher than the genuine paraphrase**, and
by a wide margin. This is direct empirical support for design doc §12's
insistence that negation be a deterministic pre-check that runs *before*
similarity is consulted, rather than a tiebreaker — a threshold cannot separate
these, at any value, because they are on the wrong sides of each other.

Phase 10.4 should treat τ tuning as necessary but nowhere near sufficient, and
should expect its adversarial-negative corpus to contain pairs scoring **above**
its positive-paraphrase pairs. `TestOnnxEncoder` carries this as a
characterization test, not a regression gate: if a future encoder separates
them, that is information, not a break.

### Finding 2 — the 512-token truncation boundary

The model sees the first 512 tokens of a block and nothing else. The shipped
tokenizer truncates at 128 by default; this phase raised it to the model's
maximum, which is as far as it goes.

The consequence is asserted by test: two long blocks sharing a 512-token prefix
produce **byte-identical vectors**, no matter how they differ afterwards. The
blocks this feature targets are long by construction — §19's bypass threshold is
stated at 512 tokens, i.e. right at the boundary — so this is not an edge case,
it is the common case for the payload that matters.

**The guard cannot delegate to similarity for long blocks.** §12's entity and
negation checks must run over the *whole* text, not the embedded prefix.
`Encoder.count_tokens` and `Encoder.max_sequence_tokens` are exposed so the
guard can detect truncation rather than infer it.

### Left alone, as instructed

`normalizer.py`'s deferred case-folding question was not touched. It remains
Tier 0/1's shared normalizer, and changing it would move Tier 0's match set.
One observation for whoever settles it: Tier 2 inherits the same normalized
text, so the decision now affects what is *embedded* as well as what is hashed —
which makes it more consequential than it was in Phase 10.2, not less.

**Deliverable:** `docs/pulsekv-semantic-context-phase10.4-summary.md`.
