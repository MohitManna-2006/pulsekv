# PulseKV semantic context — Phase 10.6 reconstructed summary

**Date of implementation:** 2026-08-21

**Status:** post-hoc reconstruction from merged Git history and preserved
Phase 10.6 runtime evidence. **This file was not part of the original Phase
10.6 merge.**

**Implementation commit:** `f8da035ce757e5e4c47aad2f36162d9655706bde`

**Merge:** PR #3, merge commit
`c61118f00b194fef6faecdbb85f8ff7ace6cc2e8`

## 1. Status and provenance

The original implementation plan required this summary, but neither PR #3 nor
any other reachable Git/GitHub history contains it. The preserved Phase 10.6
archive likewise contains no summary or report. This document therefore does
not recover original prose: it reconstructs the phase from the merged diff,
current tests and the selectively inspected text evidence in
`phase10_6_evidence.tar.gz` (SHA-256
`644a37d239c1b2ecc04e2142f66b070f05b4ed395ff45e4f1291b592b0d6934c`).

Claims below are deliberately separated into original design hypotheses,
implementation discoveries, merged changes, preserved runtime evidence and
remaining unknowns.

## 2. Objective

Phase 10.6 had to prove that two surface-different but registered-equivalent
requests could pass through independent gateway/SGLang processes, arrive at
the inference engine as the same canonical text, produce the same token
prefix, and reuse concrete PulseKV entries written by the other replica.

The original plan also hypothesized that the existing SGLang adapter would
work byte-for-byte unchanged. Real integration disproved that narrower
hypothesis without disproving the architecture.

## 3. What real integration discovered

The adapter written in Phase 7 did not implement the complete contract exposed
by the selected real SGLang release. SGLang 0.5.15's dynamic HiCache backend
factory, v1/v2 batch operations, pool transfers and tensor destinations
required a broader compatibility surface than the older adapter provided.

The run also exposed an operational threshold: SGLang's default
`prefetch_threshold=256` keeps shorter requests out of the external-storage
read path. The checked-in proof sets `prefetch_threshold` to `1` in the dynamic
backend extra configuration and uses
`--hicache-storage-prefetch-policy wait_complete`. Those settings make the
external read path observable for the proof; they are not Phase 10.8
performance recommendations.

## 4. Why the frozen-adapter hypothesis failed

**Original plan:** prove integration through a completely unmodified
`pulsekv_adapters.sglang` implementation and stop if a change appeared
necessary.

**As-built outcome:** the selected upstream API did require a mechanical
compatibility repair. Commit `f8da035` changed
`adapters/pulsekv_adapters/sglang.py` and added target-version contract tests.
PR #3 reviewed and merged that change as part of the Phase 10.6 proof.

**Architectural result:** semantic decomposition, matching and substitution
remain entirely in `gateway/`, before tokenization. The adapter contains no
semantic matcher, registry lookup or canonicalization policy. PulseKV key
semantics, storage, routing, control plane and protobuf contracts did not
change. Thus “adapter source never changes” is superseded; “adapters have no
semantic responsibility” remains a durable boundary.

## 5. Compatibility repair

The merged commit changed exactly these six paths:

- `adapters/pulsekv_adapters/sglang.py`
- `adapters/tests/test_sglang_0515_contract.py`
- `adapters/tests/test_sglang_adapter.py`
- `adapters/tests/test_sglang_integration.py`
- `gateway/tests/test_sglang_integration.py`
- `deploy/demo-semantic-sglang.sh`

The adapter repair covers dynamic-factory construction, target-location tensor
copies, v1 and v2 batch methods, pool hit policies/results, legacy opaque-key
behavior, optional JSONL tracing and operation without SGLang installed.
Compatibility code changes how PulseKV satisfies SGLang's storage interface;
it does not change how canonical text is selected or how PulseKV keys mean.

## 6. Real SGLang environment

The checked-in demo and preserved server arguments agree on:

| Item | Pinned value |
|---|---|
| SGLang | `0.5.15` |
| Model | `Qwen/Qwen2.5-1.5B-Instruct` |
| Hugging Face revision | `989aa7980e4cf806f80c7fef2b1adb7bc71aa306` |
| SGLang A | `127.0.0.1:30000` |
| SGLang B | `127.0.0.1:30001` |
| Gateway A | `127.0.0.1:8088` |
| Gateway B | `127.0.0.1:8089` |
| HiCache write policy | `write_through` |
| Storage backend | dynamic `pulsekv_adapters.sglang.PulseKVHiCacheStorage` |
| Prefetch policy | `wait_complete` |
| Extra configuration | `interface_v1=1`, `prefetch_threshold=1` |

This is a pinned integration environment, not a promise of compatibility with
every SGLang version or model.

## 7. Real two-replica topology

`deploy/demo-semantic-sglang.sh` starts the ordinary PulseKV dev cluster, then
two independent SGLang serving processes with separate trace files and two
gateway processes. Gateway A forwards to SGLang A; Gateway B forwards to
SGLang B. Both gateways share a registry containing one canonical system
policy and a surface-different registered alias.

The script captures the actual outbound request body at each gateway rather
than assuming that a match decision implies correct forwarding. Both real
`POST /v1/chat/completions` requests returned HTTP 200 in the preserved logs.

## 8. Direct external-cache evidence

Before interpreting the semantic run, the merged adapter and its contract
tests establish that SGLang 0.5.15 can instantiate the dynamic PulseKV backend,
write through it and read into the required target forms. The preserved trace
archive also contains direct, non-semantic adapter runs, but this reconstruction
does not publish aggregate figures from them: the trace includes warm-up
traffic, and no preserved report defines a defensible boundary between warm-up
and the two intended measurements.

That omission is evidence discipline, not a negative result. The semantic run
below has its own unambiguous A/B trace files and is sufficient for the Phase
10.6 claim.

## 9. Semantic canonicalization proof

The preserved semantic run supports the following scoped facts:

| Observation | Evidence |
|---|---:|
| Raw registered canonical and alias text differ | demo construction and captured inputs |
| Captured Gateway A and B outbound bodies are equal | byte-equivalent parsed JSON captures |
| SGLang A request tokens | 1,040 (`#new-token: 1040`) |
| SGLang B cached prefix | 1,039 tokens, with one new token |
| Replica A distinct successful single-key writes | 1,064 |
| Replica B distinct queried keys | 1,044 |
| Replica B successful storage reads | 1,044 |
| Replica B failed storage reads | 0 |
| A-write/B-successful-read exact-key intersection | 1,044 |
| Real inference response | HTTP 200 from both SGLang processes |

The checked-in demo independently tokenizes both captured canonical system
messages and requires token-list equality and equality of their SHA-256
projections before it can print `SEMANTIC_CROSS_REPLICA_PROOF=PASS`. The
preserved archive available to this reconstruction contains the equal outbound
bodies and server/adapter traces, but not that final command transcript; this
summary therefore does not reproduce a token hash that cannot be read back
from preserved evidence.

## 10. Evidence chain and the anti-latency rule

The proof is not “Replica B was faster.” Latency is not used as cache-hit
evidence. The chain is:

```text
different raw registered variants
  -> equal captured canonical outbound text
  -> checked-in proof requires equal independent tokenization
  -> concrete successful PulseKV writes from SGLang A
  -> concrete successful PulseKV reads from independent SGLang B
  -> 1,044-key non-empty exact intersection
  -> successful real inference on both processes
```

This directly correlates the gateway decision to storage keys. It is stronger
than inferring reuse from timing or from an in-process mock.

## 11. Tests and regression results

No implementation tests were rerun for this documentation reconstruction.
The preserved logs directly corroborate:

- gateway suite: **352 passed, 79 skipped, 0 failed**;
- adapter suite: **44 run, 12 skipped, 0 failed**—therefore **32 passed**;
- focused SGLang 0.5.15 contract surface: 12 test methods in the merged file;
- focused gateway semantic integration: 3 test functions in the merged file.

The two focused counts describe the checked-in test surfaces, not a separately
preserved focused-test command transcript. The adapter contract includes a
subprocess test that blocks SGLang imports and verifies fallback storage
behavior. It also verifies that an unwritable trace path logs once, disables
that trace sink and does not break storage operations.

## 12. Trace and observability behavior

`PULSEKV_SGLANG_TRACE_PATH` opt-in enables JSONL records containing process,
replica, operation, keys and result. The trace is evidence instrumentation,
not a request dependency: a write error disables that path after one logged
error. A trace failure must not turn a working cache operation into a serving
failure.

This instrumentation closes the proof-specific correlation gap for the demo,
but general production correlation between every gateway decision and every
engine cache outcome remains incomplete. That remains an observability risk,
not a reason to infer hits from latency.

## 13. Scope protection

Phase 10.6 changed no file in `node/`, `control/`, `proto/`, the gateway
implementation package, `key.py`, `vllm.py` or `vllm_key.py`. It did not add
semantic metadata to cache keys or teach the adapter to decide equivalence.
The only runtime-package change was the SGLang compatibility surface.

## 14. Fresh-cluster caveat

The provenance brief records a first-attempt distributed-read irregularity and
an unchanged successful retry on a fresh cluster. No corresponding smoke log
was found in the preserved archive inventory inspected for this reconstruction,
so this document does not publish its numerical breakdown or claim a fix. The
merged Phase 10.6 diff did not touch replication/startup readiness, and no such
issue should be conflated with the older, already-resolved Raft
restart-readiness defect.

## 15. What Phase 10.6 proved

- The Phase 10.5 gateway can forward real OpenAI-compatible traffic to two
  real SGLang 0.5.15 serving processes.
- Surface-different registered text can be captured leaving two gateways as
  identical canonical text.
- Independent SGLang processes can correlate concrete PulseKV writes and
  successful reads for the canonicalized prefix.
- The chosen run produced a 1,044-key exact cross-process intersection and
  successful inference.
- SGLang's real upstream storage contract can be satisfied without moving
  semantic responsibility or changing PulseKV core/key semantics.

## 16. What Phase 10.6 did not prove

- Compatibility with every SGLang release, model, tokenizer or deployment.
- Phase 10.7 vLLM compatibility.
- That a strict no-adapter-change rule will hold for future upstream APIs.
- Universal absence of semantic false positives; Phase 10.4's corpus results
  remain scoped measurements.
- Production-scale multi-tenant isolation or soak behavior; those remain
  Phase 10.9 work.
- Net performance benefit or a production bypass threshold. Phase 10.8 must
  still measure `T_gateway` against avoided prefill and exact-cache costs.

## 17. Handoff to Phase 10.7

Phase 10.7 is next and has not started. It should begin with a read-only audit
against an explicitly selected and pinned real vLLM version. Keeping
`vllm.py` and `vllm_key.py` unchanged is the desired initial compatibility
hypothesis, not an established fact. If the selected API does not match, the
phase must name and review the compatibility gap before authorizing any
mechanical adapter change; semantic responsibility and exact key semantics
remain protected boundaries.
