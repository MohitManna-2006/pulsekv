# PulseKV v3 / Phase 10 — Semantic Context Canonicalization: Implementation Plan

**Status:** implementation plan, not yet started. Companion to
`pulsekv-semantic-context-design.md` (what/why) — this document is how it
gets built, phase by phase, at the granularity needed to actually execute it,
in the same spirit as `pulsekv-v2-implementation-plan.md`. Does not modify
v1 or v2's `src/`, `include/`, `tests/`, `node/`, `control/`, `proto/`, or
`adapters/pulsekv_adapters/{client,key,sglang,vllm,vllm_key}.py`.

---

## 1. Component and language map

| Component | Language | Why | Touches v2? |
|---|---|---|---|
| **Gateway** — decomposition, tier 0-3 matching orchestration, request forwarding | Python | Same ecosystem as `adapters/pulsekv_adapters/`; imports `PulseKVClient` directly as a library (Finding 1, design doc §6); FastAPI/ASGI is the natural fit for an ingress reverse proxy in front of OpenAI-compatible SGLang/vLLM endpoints | No — new `gateway/` directory |
| **Registry** — canonical context storage, versioning, embeddings | Python service + relational store (SQLite MVP, Postgres-ready) | Registry reads/writes are the gateway's own dependency, not a cluster-wide concern; no reason to introduce Go/C here | No — new, outside `control/` (design doc §6 Finding 3) |
| **Structural normalizer** (Tier 1) | Python, stdlib `json`/regex | Tool-schema JSON re-serialization is a parsing problem, not a systems problem | No |
| **Embedding encoder** (Tier 2) | Python, ONNX Runtime CPU or equivalent — exact model choice is a Phase 10.3 decision, not fixed here | CPU-only keeps the MVP deployable without a GPU dependency the rest of v2 doesn't have either | No |
| **Equivalence guard** (Tier 3) | Python, deterministic rule-based checks | No ML dependency needed for the checks in design doc §12 | No |

**The build-vs-borrow rule**, carried over from v2's own stated philosophy
(`pulsekv-v2-implementation-plan.md` §1): hand-build the pieces that are this
feature's actual novel contribution (the tiered matching pipeline, the
equivalence guard, the registry's versioning semantics) and use ordinary,
boring, vetted libraries for solved problems (an embedding model, a JSON
parser, a relational store's client library, an ASGI framework). Nothing
here needs a hand-rolled vector index, a hand-rolled ONNX runtime, or a
hand-rolled web framework — none of those are this project's differentiator.

## 2. Dependency graph

```
Phase 10.0  Contracts, decisions, evaluation-corpus skeleton
   |
   v
Phase 10.1  Canonical Context Registry (storage, versioning, CRUD, namespace)
   |
   v
Phase 10.2  Deterministic tiers: exact-hash (Tier 0) + structural (Tier 1)
   |         + the audit/decision log (design doc §21)
   v
Phase 10.3  Semantic candidate retrieval (Tier 2: embedding + vector search)
   |
   v
Phase 10.4  Equivalence validation & guardrails (Tier 3) + evaluation corpus
   |         built out to real adversarial scale
   v
Phase 10.5  OpenAI-compatible ingress gateway (decompose/assemble/forward,
   |         wires Tiers 0-3 together, fail-open wiring, deployment shape)
   v
Phase 10.6  SGLang real-model integration
   |
   v
Phase 10.7  vLLM real-model integration
   |
   v
Phase 10.8  Semantic + exact-cache benchmark (the T_saved vs T_gateway
   |         measurement the design doc §19 says does not exist yet anywhere
   |         in this codebase)
   v
Phase 10.9  Hardening, multi-tenant isolation proof, soak
```

No safe parallelization point comparable to v2's Phase 1/2 split — Phase
10.1's registry schema is a real dependency for every later phase (a schema
change after 10.2 is written means rework), and Tiers 0→1→2→3 are a strict
pipeline by design (design doc §11), not independent subsystems. Phase 10.6
and 10.7 could run in parallel once 10.5 is done, mirroring v2's Phase 7/8
relationship, but are sequenced here (SGLang first) for the same reason v2
sequenced them that way: SGLang's interface is simpler and de-risks the
gateway-to-real-engine integration before vLLM's tighter worker/scheduler
coupling is attempted.

## 3. Gate before Phase 10 starts — RESOLVED

**Status: closed on evidence, not waived.** See
`docs/pulsekv-v2-soak-collapse-analysis.md` for the full investigation. This
section retains the original finding below it, because the way this gate was
first assessed is itself instructive.

**Answer to the master prompt's §25 question:** the collapse is **not** a v2
availability bug. It is a defect in the test harness under `deploy/`, and
PulseKV's data plane, control plane and SDK each behaved exactly as designed
throughout — including at the moment of collapse.

The chain, each link evidenced in the analysis document:

1. `deploy/common.sh`'s pid registry did an unlocked read-modify-write on one
   shared file. Concurrent lifecycle operations silently lost each other's
   entries — reproducibly, 2-5 of 24 records surviving under contention.
2. Every liveness, stop and start decision was taken from that registry alone,
   so a lost record made a running control-plane replica read as dead. At
   01:18:57 on 2026-08-19 `local-node.sh` refused to act because "no
   control-plane replica is running"; three seconds later all three replicas
   committed a membership generation.
3. `local-node.sh` refuses to start a data node on that answer — 370 times
   during this cluster's lifetime. Once a node was down it could not be brought
   back.
4. The cluster reached a genuinely empty membership (generation 19, zero data
   nodes, 01:21:12) and stayed there for **80 minutes**. The SDK installs an
   authoritative empty topology and then returns `ErrNoLiveNodes` with no
   network call at all — an instant error on every operation, permanently,
   while `--continue-on-error=true` kept the benchmark running.
5. The concurrency came from chaos injectors that survived their parent soak's
   cleanup. Three were running against one cluster, at cycle counters 1-5,
   77-88 and 152-165.

**Why it looked like "53.5 minutes":** nothing accumulates to a threshold. The
collapse needs two lifecycle operations to overlap, and when that happens is a
beat frequency between independent injector schedules. One run collapses at
53.5 minutes, another at 3.5. That is also why re-running the same command
never reproduced it — the essential ingredient is a second actor the command
line does not mention.

**Fixed and verified.** Eight fixes in `deploy/` (registry mutex, process-table
liveness, honest stop, adoption instead of doomed duplicates, a named
self-terminating injector, a soak singleton guard, settle-before-leader-cycle,
and a verdict that fails a run whose cluster served nothing).
`deploy/test-lifecycle.sh` covers each link: 6 of its 8 checks fail against the
pre-fix code and all 8 pass after. A fresh long-duration soak is recorded in
`docs/pulsekv-v2-progress-report.md` §4.2 with its artifact preserved.

**Consequence for Phase 10:** the gate is closed. Phase 10.1 onward may
proceed. Phase 10.0 ran under an explicit waiver of this gate (see
`pulsekv-semantic-context-phase10.0-summary.md` §1); that waiver is now
superseded by this resolution.

---

### Original assessment, retained

The following was this plan's first reading of the evidence. It is kept because
its central mistake is worth remembering: it judged the incident from
`soak-report.json` alone and concluded no artifact existed, when the logs of the
same cluster's full 83-minute lifetime were on disk beside it the whole time.
`soak-report.json` was short because it belonged to one short run, not because
the incident had left no trace.

Not a phase — a precondition. The master investigation prompt (§25, §38.19)
requires an explicit answer to whether the unresolved 4-hour soak
observation (successful operations collapsing ~53.5 minutes into a run, no
recovery, benchmark process continuing) is a real v2 availability bug that
must be fixed before more is layered on top.

**What this investigation found:** the repository's current soak artifacts
(`deploy/run/soak-report.json`, `deploy/run/logs/soak-chaos.log`) are from a
**180-second** run, not a 4-hour run, and they show the *opposite* pattern —
elevated `rpc_errors` during the scripted 15-second kill/restart chaos
windows (intervals 3, 5, 6, 7, 8, 10, 12 in the JSON), recovering to zero in
intervals 9 and 11, with a **positive** throughput drift (+1.66%) and a
modest p99 latency drift (+6.6%) from first to last quarter — this is the
harness's designed chaos behavior recovering as intended, not a collapse.
Separately, the measured `ops_per_second` in this JSON artifact (1,505.99)
and total `operations` (271,111) do not match the figures narrated in
`docs/pulsekv-v2-progress-report.md` §2/§4.2 (5,390 ops/s, 182,312 reads
verified) for what is described as the same Phase 9.4 evidence — the error
counts are close (13,803 vs. 13,809) but the throughput and volume figures
are not, which means the progress report's table was drawn from a different
run than the one currently on disk, and the two have not been reconciled.

**Conclusion:** this investigation can answer neither "is the 53.5-minute
collapse a real bug" (A) nor any of options B–E in the master prompt's
framing (§25), because **no artifact matching that specific incident's
duration or failure signature exists in the current repository snapshot** to
examine. This is not the same finding as "confirmed not a bug" — it is
"unable to confirm or characterize with what's on disk."

**Recommendation:** treat this as a hard gate, per the master prompt's own
stated default ("the existing distributed core should be stable before
layering new semantics on top") — before Phase 10.0 work begins:

1. Locate or reproduce the actual 4-hour run's logs (soak harness supports
   long durations per `deploy/soak-test.sh`; the 180s artifact currently
   checked into `deploy/run/` is evidently a short validation run, not that
   one).
2. Reconcile the throughput/volume discrepancy between
   `soak-report.json`-shaped evidence and `pulsekv-v2-progress-report.md`'s
   narrated numbers — at minimum, note in the progress report which run
   produced which numbers, since right now a reader cannot tell.
3. If the 53.5-minute collapse reproduces, characterize it with the same
   rigor Phase 9's other findings used (root cause, not just symptom) before
   deciding whether it blocks Phase 10 or can run in parallel as a tracked,
   separate issue the way the `data-node` chaos flake was handled in
   `pulsekv-v2-restart-readiness-fix-summary.md`.

This gate is Phase 9.x work, explicitly not folded into Phase 10's own scope
— consistent with how Phase 9's own prompt refused to fold the pre-existing
`data-node` chaos flake into its own scope. Phase 10.0 (next section) can
start in parallel with this investigation since it produces no code that
depends on cluster runtime behavior, but Phase 10.1 onward should not start
until this gate is resolved or explicitly, consciously waived by whoever owns
that call.

## 4. Phase 10.0 — Contracts, architecture, evaluation-corpus skeleton

**Objective:** freeze the cross-tier contract (registry record shape,
gateway-to-registry interface, decision-log schema) the same way v2's Phase 0
froze the gRPC contract — so Phases 10.1 and later don't diverge — and stand
up the evaluation corpus's *structure* (not its full adversarial content yet)
so Phase 10.4 has somewhere to grow into.

**Dependencies:** none beyond this design doc and §3's gate decision.

**Directories/files:** new top-level `gateway/` (name and layout finalized
below, §5.2), `docs/` (this document set), a new `gateway/tests/corpus/`
skeleton.

**New interfaces:** the registry record schema (design doc §10) as a
concrete Pydantic/dataclass definition; the `MatchResult` type (master
prompt §30's sketch: `matched, context_id, version, confidence, method,
rejection_reason`); the decision-log record schema (design doc §21).

**Invariants established here, enforced by every later phase:**
canonical-version immutability; fail-open on any component error; namespace
as a mandatory field on every registry write.

**What must NOT be implemented yet:** no embedding model, no vector search,
no live gateway process, no registry storage backend beyond an in-memory
stub for contract tests.

**Tests:** schema validation tests only — round-trip a registry record and a
decision-log record through the frozen contract.

**Exit criteria:** the record/contract types are frozen and reviewed;
`gateway/tests/corpus/README.md` documents the corpus's planned structure
(positive-paraphrase set, adversarial-negative set, cross-tenant set,
version-update set — mirroring master prompt §23's test classes) even before
real examples are written; §3's gate has a documented answer (resolved or
explicitly waived).

**Documentation deliverable:** none beyond this plan and the design doc —
Phase 10.0 does not produce its own summary doc; its output *is* the frozen
contract.

**Handoff to 10.1:** the registry schema is exactly what Phase 10.1 builds
storage for — no schema changes expected, though not contractually frozen
the way v2's proto was (this is an internal Python contract, not a
cross-language wire format, so the cost of a later change is lower and the
freeze is a discipline choice, not a technical necessity).

## 5. Phase 10.1 — Canonical Context Registry

**Objective:** durable, versioned, namespace-scoped storage for canonical
contexts, with the CRUD surface the master prompt's §30 API sketch
describes (`RegisterContext`, `GetContext`, `ResolveAlias`, `FindCandidates`
stubbed but not yet doing real vector search, `PublishVersion`,
`DeprecateVersion`).

**Dependencies:** Phase 10.0's frozen schema.

**Directories/files:** `gateway/pulsekv_gateway/registry.py`,
`gateway/pulsekv_gateway/models.py`; a migrations directory if using a real
SQL store from the start (recommended over SQLite-then-migrate, to avoid a
schema-migration exercise mid-project for a low-effort choice).

**New interfaces:** `Registry.register(record) -> context_id`,
`Registry.get(context_id, version=None) -> Record`,
`Registry.resolve_alias(text, namespace) -> Optional[context_id]`,
`Registry.by_content_hash(hash, namespace) -> Optional[Record]` (this is
Tier 0's actual lookup path — implemented here even though Tier 0 matching
logic lives in 10.2, because it's registry storage, not matching logic),
`Registry.deprecate(context_id, version)`.

**Invariants:** version immutability enforced at the storage layer (an
`UPDATE` on a published version's `canonical_text`/`content_hash` is
rejected, not merely discouraged by convention); namespace is required on
every write, no default/global namespace exists.

**What must NOT be implemented yet:** embeddings are stored as an opaque
blob field if provided, but nothing in this phase computes or searches them
— that's 10.3. No gateway process exists yet to call this.

**Unit tests:** CRUD round-trips; version-immutability rejection; namespace
isolation (two namespaces' `by_content_hash` calls never see each other's
records, even with an identical hash — this is the structural precondition
design doc §15 depends on, and it should be proven here, at the storage
layer, not assumed true because the gateway happens to pass namespace
through correctly later).

**Integration tests:** none yet — no other component exists to integrate
with.

**Failure tests:** storage backend unavailable → `Registry` methods raise a
typed, catchable exception (not a bare connection error) that Phase 10.5's
gateway wiring can catch cleanly for its fail-open path.

**Exit criteria:** registry CRUD is durable across a process restart (proven
with an actual restart in the test, not assumed from the storage engine's
own guarantees); namespace isolation proven; version immutability proven.

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.1-summary.md`.

**Handoff to 10.2:** `Registry.by_content_hash` and `.resolve_alias` are
ready for Tier 0/Tier 1 to call.

## 6. Phase 10.2 — Deterministic tiers (Tier 0 exact-hash, Tier 1 structural) + decision log

**Objective:** the two matching tiers that need no ML dependency, plus the
audit/decision-log writer (design doc §21) — deliberately built before any
embedding work exists, so the "does the pipeline correctly fall through
to unchanged text on a miss" behavior is provable with zero non-deterministic
components in the loop.

**Dependencies:** Phase 10.1's registry.

**Directories/files:** `gateway/pulsekv_gateway/decomposer.py` (block
extraction — see design doc §13's eligibility table; only the block-type
*classification* needed here, not routing decisions yet),
`gateway/pulsekv_gateway/normalizer.py` (Tier 1 structural normalization),
`gateway/pulsekv_gateway/matcher.py` (Tier 0 orchestration — the exact-hash
lookup path), `gateway/pulsekv_gateway/auditlog.py`.

**New interfaces:** `decompose(request) -> List[Block]` where `Block` carries
`type`, `text`, `position`; `Matcher.try_exact(block) -> Optional[MatchResult]`;
`AuditLog.record(decision)`.

**Invariants:** a Tier 0/1 miss produces the original block, byte-identical,
never a partial substitution; the decision log records every block's outcome,
including bypassed/ineligible ones (so a later query "what did the gateway
do with this request" is always answerable, not just for matches).

**What must NOT be implemented yet:** Tier 2/3, the actual gateway process/
HTTP surface, real SGLang/vLLM traffic.

**Unit tests:** decomposition correctness against the §13 eligibility table
(a `CONVERSATION_HISTORY`/`USER_QUERY` block is never marked eligible,
proven by test, not by convention); Tier 1 structural normalization
round-trips (whitespace/key-order variation on an identical JSON schema
produces the identical Tier 0 hash; a semantically different schema does
not); Tier 0 exact-hash and alias resolution against Phase 10.1's registry.

**Integration tests:** decompose → normalize → Tier 0 lookup → audit log,
end to end, against a real (test) registry instance.

**Benchmark gates:** none yet — Tier 0/1 latency is expected to be
negligible (a hash lookup and a JSON reserialize), but not asserted with a
number until Phase 10.8 measures the real pipeline.

**Exit criteria:** the deterministic-tier pipeline correctly resolves
byte-identical and structurally-equivalent (JSON) blocks, correctly leaves
everything else unchanged, and every decision is in the audit log.

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.2-summary.md`.

**Handoff to 10.3:** `Matcher` gets a `try_semantic` method added, called
only on a Tier 0/1 miss.

## 7. Phase 10.3 — Semantic candidate retrieval (Tier 2)

**Objective:** embedding-based candidate retrieval, explicitly scoped as
*retrieval only* — this phase does not decide matches, per design doc §11's
insistence that Tier 2's output is candidates, never a decision.

**Dependencies:** Phase 10.2's matcher (adds Tier 2 as the next fallback),
Phase 10.1's registry (embeddings stored per record).

**Directories/files:** `gateway/pulsekv_gateway/encoder.py`,
`gateway/pulsekv_gateway/index.py`.

**New interfaces:** `Encoder.embed(text) -> Vector`;
`Index.top_k(vector, namespace, block_type, k) -> List[Candidate]`
(namespace and block_type are filter arguments here, not post-filters — this
is where design doc §15's "namespace before similarity" requirement is
actually implemented, and it should be a unit-tested property of this
interface, not an assumption about how callers use it).

**Design decision to make in this phase, not before:** embedding model
choice and index technology (brute-force cosine vs. any ANN library),
resolved against Phase 10.1's actual registry population size rather than
assumed from the design doc's "low hundreds to low thousands" estimate —
that estimate should be checked against real usage data if any exists by
this point, and the simplest option (brute-force, in-process, no new
service) should be the default unless this phase's own benchmarking shows
it doesn't hold up at the real scale.

**What must NOT be implemented yet:** Tier 3's guard (a candidate coming out
of this phase is not yet a match); the gateway process.

**Unit tests:** embedding determinism (same text, same model version, same
vector — a property worth asserting explicitly since it's load-bearing for
the registry's cached embeddings staying valid); namespace/block-type
pre-filtering proven with a deliberately cross-namespace-similar pair that
must never appear in each other's candidate lists.

**Benchmark gates:** first real timing numbers in this project for
embedding + retrieval latency (`pulsekv_semantic_lookup_latency_seconds{tier="embedding"}`)
— measured, not assumed, and reported honestly even if higher than the prior
report's unsupported 2.5–6ms figures.

**Failure tests:** encoder unavailable/slow-past-budget → Tier 2 skipped,
falls through per design doc §17's failure table.

**Exit criteria:** candidate retrieval is namespace/type-correct and has a
real, measured latency number attached to it.

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.3-summary.md`,
including the embedding model/index technology decision and its
justification against measured data.

**Handoff to 10.4:** `Matcher.try_semantic` produces candidates; Phase 10.4
adds the guard that turns a candidate into a `MatchResult`.

## 8. Phase 10.4 — Equivalence validation, guardrails, evaluation corpus

**Objective:** the reject-biased Tier 3 guard (design doc §12), and the
evaluation corpus built out from Phase 10.0's skeleton into real adversarial
scale — this is the phase that actually earns the τ threshold and the
guard's specific checks, rather than assuming them.

**Dependencies:** Phase 10.3's candidates.

**Directories/files:** `gateway/pulsekv_gateway/guardrail.py`;
`gateway/tests/corpus/` populated with real examples across the four classes
in design doc §12/master prompt §23.

**New interfaces:** `Guardrail.check(block, candidate) -> GuardResult`
(pass/reject + reason, per design doc §12's three checks: negation/polarity,
entity/value preservation, structural-type consistency).

**Invariants:** every check is reject-biased; a guard error or timeout is a
reject, never a pass-through-with-a-warning.

**Corpus construction, per master prompt §23's test classes** (each with a
target, not yet a claimed result):

- Positive paraphrase suite: real registered-context paraphrases, target
  match rate measured honestly, not asserted at 98%+ before the corpus
  exists.
- Adversarial-negative suite: the specific failure modes in design doc §12
  and master prompt §8.1 (negation, entity swap, staging/production,
  before/after) — target false-positive rate is **0 confirmed by test on
  this corpus**, not a probabilistic claim.
- Cross-tenant suite: proves Phase 10.1's namespace isolation holds even
  when two namespaces register near-identical canonical texts.
- Version-update suite: proves an old version's decisions remain
  interpretable and a new version doesn't retroactively confuse anything
  already logged.

**τ threshold determination:** tuned against the adversarial-negative
suite specifically (design doc §12) — this phase is where a real number
finally replaces the "not yet determined" placeholder in the design doc, and
that number should ship into this phase's summary doc with the corpus size
and methodology that produced it, not as a bare constant.

**Exit criteria:** zero false positives on the adversarial-negative corpus
at whatever τ this phase lands on; positive-paraphrase match rate reported
honestly even if it is not high on a first pass — a real, low match rate
with zero false positives is a legitimate, ship-able Phase 10.4 outcome, and
is explicitly preferred over lowering τ to force a higher match rate.

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.4-summary.md`,
with the corpus itself checked in (`gateway/tests/corpus/`) so future phases
regression-test against it, not just this phase.

**Handoff to 10.5:** a complete `Matcher.resolve(block) -> MatchResult`
covering all four tiers, fully unit- and corpus-tested, with no gateway
process wired around it yet.

## 9. Phase 10.5 — OpenAI-compatible ingress gateway

**Objective:** the actual proxy process — decompose/assemble/forward,
fail-open wiring, deployment shape decision (design doc §8's E vs. F
packaging question, resolved here rather than left open).

**Dependencies:** Phase 10.4's complete matcher.

**Directories/files:** `gateway/pulsekv_gateway/server.py`,
`gateway/pulsekv_gateway/assembler.py`, `gateway/pulsekv_gateway/config.py`,
`gateway/pyproject.toml`.

**New interfaces:** the HTTP surface itself (OpenAI-compatible
`/v1/chat/completions` passthrough with block substitution applied before
forwarding); `Assembler.assemble(blocks) -> str` (order-preserving, per
design doc §14 — no reordering logic exists to build here, which is worth
stating explicitly since it would be easy to add "just this one
optimization" at this stage).

**Invariants:** gateway-down fails open per design doc §17's table;
request-order preservation is a hard invariant, tested, not just an
implementation default that happens to preserve order.

**Unit tests:** assembly order preservation; end-to-end request flow against
a mocked downstream (not real SGLang/vLLM yet).

**Integration tests:** gateway in front of a stub HTTP server standing in
for SGLang/vLLM, proving pass-through behavior and header/routing
correctness.

**Failure tests:** kill the registry mid-request, kill the encoder,
confirm every failure mode in design doc §17's table produces "original
text forwarded," not an error surfaced to the caller.

**Benchmark gates:** first end-to-end `T_gateway` measurement (design doc
§19) against the stub downstream — real numbers, replacing the "no gateway
exists to measure" caveat from the design doc.

**Exit criteria:** the gateway is a working, fail-open reverse proxy with no
real inference engine behind it yet; every failure mode in the design doc's
table is demonstrated, not just implemented.

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.5-summary.md`.

**Handoff to 10.6:** the gateway is ready to point at a real SGLang instance
instead of a stub.

## 10. Phase 10.6 — SGLang real-model integration

**Objective:** prove the gateway's canonical text produces the claimed
exact-cache behavior against a real SGLang server and the real,
**unmodified** `pulsekv_adapters.sglang`/`key.py` — this is where design
doc Finding 2 (§6) gets tested against reality instead of read code, and
where the first half of the master prompt's §39 success criteria becomes
checkable.

**Dependencies:** Phase 10.5's gateway; a working PulseKV v2 dev cluster
(unmodified, per `deploy/run-local-cluster.sh`) and SGLang, per the same
setup `pulsekv-v2-phase7-prompt.md` used.

**Directories/files:** `gateway/tests/test_sglang_integration.py`; possibly
a `deploy/demo-semantic-sglang.sh` mirroring the shape of
`deploy/demo-cross-replica-sglang.sh` but with two *differently-worded*
requests instead of the identical prefix Phase 7's demo used.

**Hard scope boundary, explicit:** no changes to `node/engine/`,
`node/grpc_shim/`, `control/`, `proto/`, or any file under
`adapters/pulsekv_adapters/` — the entire point of this phase is that none
of those need to change. If SGLang integration seems to require a change to
any of them, stop and name the gap; do not make the change.

**Test:** Replica A receives request variant 1 of a registered context,
computes and stores KV state under the canonical text's block hashes.
Replica B receives request variant 2 (different wording, same registered
context) through the gateway, gets canonicalized to the identical text, and
`pulsekv_adapters.sglang`'s **completely unmodified** `batch_exists_v2`/`get`
resolves to Replica A's stored blocks. This is master prompt §39's success
criteria 2–8, run for real.

**Exit criteria:** the cross-replica hit reproduces reliably (report the
real reproduction rate, matching the existing project's honesty standard —
Phase 7 reported 100.0% across 30/30 trials; this phase should report
whatever it actually measures, not assume the same number).

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.6-summary.md`.

**Handoff to 10.7:** vLLM integration next, same shape.

## 11. Phase 10.7 — vLLM real-model integration

**Objective:** same proof as 10.6, against vLLM's `KVConnectorBase_v1` split
scheduler/worker interface, per-layer.

**Dependencies:** Phase 10.6 (de-risks the gateway-to-real-engine pattern
first, same sequencing logic v2's own Phase 7→8 used).

**Hard scope boundary:** identical to 10.6's, plus explicitly: no changes to
`pulsekv_adapters.vllm`/`vllm_key.py`.

**Exit criteria/documentation:** same shape as 10.6, against vLLM.

## 12. Phase 10.8 — Semantic + exact-cache benchmark

**Objective:** the measurement the design doc (§19) states does not exist
anywhere in this codebase yet — real prefill-avoidance economics, not the
storage-path-only numbers Phase 7/8's demos produced.

**Dependencies:** Phase 10.6/10.7 (needs real inference engines running).

**Directories/files:** `gateway/tests/bench_semantic_gateway.py`; a
three-way comparison harness per master prompt §23: (A) inference with no
PulseKV, (B) inference with PulseKV exact caching only, (C) PulseKV Semantic
Gateway + exact caching.

**What this phase must produce, that nothing before it has:** a real
prefill-recompute baseline (config A) to compare against — this requires
actual GPU-bound generation, which no prior PulseKV v2 phase stood up. If
GPU access is not available in the environment executing this phase, that
constraint should be stated as a scorecard gap exactly the way v1 stated its
`<5ms p99` miss, not silently worked around with a CPU-only proxy presented
as equivalent.

**Test classes:** the five from master prompt §23 (exact same prompt, safe
paraphrase, different-wording-same-structure, near-semantic negative,
different tenants, registry version update) — most already have unit-level
coverage from Phase 10.4's corpus; this phase's job is running them through
real inference and measuring TTFT/throughput, not re-proving correctness.

**Exit criteria:** a real, honest break-even table replacing the
hypothetical one in the design doc §19 — met targets and missed targets both
stated plainly, per the project's established scorecard discipline.

**Documentation deliverable:** `docs/pulsekv-semantic-context-phase10.8-summary.md`.

## 13. Phase 10.9 — Hardening, multi-tenant isolation proof, soak

**Objective:** apply the same discipline v2's Phase 9 applied — sustained
load, fault injection, and this feature's own version of the "no known bugs"
cleanup pass before calling the feature done.

**Scope:** multi-hour soak with the gateway in the loop (registry outage
mid-run, embedding encoder killed mid-run, confirming fail-open holds under
sustained fault injection, not just the unit-level failure tests from
10.5); a dedicated cross-tenant adversarial run at larger scale than Phase
10.4's corpus; a repeat of §3's soak-report reconciliation exercise for
whatever the gateway's own soak run produces, so this feature does not
inherit v2's own unresolved discrepancy pattern.

**Exit criteria:** master prompt §39's full success-criteria list, all
fifteen items, each backed by a citation into a specific phase's evidence —
mirroring the discipline `pulsekv-v2-progress-report.md` §2 used for v2's
three top-level criteria.

**Documentation deliverable:** `docs/pulsekv-semantic-context-progress-report.md`
— the Phase 10 counterpart to `pulsekv-v2-progress-report.md`.

## 14. What "done" looks like

Per master prompt §39, unedited: real SGLang/vLLM inference, two
surface-different registered-equivalent blocks mapping to one canonical
representation, identical resulting token prefix, a real cross-replica exact
hit through the unmodified adapters, dynamic user content untouched,
adversarial near-matches reliably rejected, cross-tenant matches structurally
impossible, gateway failure falling back to today's exact-only behavior, and
v2's own existing exact-cache tests staying green throughout every phase
above.
