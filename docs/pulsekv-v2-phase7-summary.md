# PulseKV v2 — Phase 7 Summary

**Status: complete.** This is the MVP-complete marker for the PulseKV v2 project — the first phase where the distributed storage engine is demonstrated against a real LLM inference serving engine (SGLang HiCache) rather than synthetic clients and benchmarks.

---

## 1. Executive Summary

Phase 7 delivers a thin, zero-compromise Python adapter package (`pulsekv_adapters`) that connects SGLang's Hierarchical KV Cache (HiCache) directly to a distributed PulseKV v2 cluster.

Key deliverables and verified results:
- **Generic Python Client SDK (`pulsekv_adapters.client`):** Connects to the Go control plane (`ClusterMetadataService`), validates SHA-256 topology fingerprints, computes 64-bit FNV-1a rendezvous shard hashes identical to `router.ShardForKey`, and executes unary (`<=4 MiB`), chunked streaming (`>4 MiB`), or zero-copy bulk transport transfers against C++ data nodes (`NodeService`).
- **Key Scheme Alignment (`pulsekv_adapters.key`):** Matches SGLang's exact chained SHA-256 token page hash algorithm (`RadixKey.hash_page` / `get_hash_str`) with 4-byte big-endian token encoding, verified bit-for-bit against SGLang's reference implementation.
- **SGLang HiCache Storage Backend (`pulsekv_adapters.sglang`):** Implements SGLang's external storage backend interface (`HiCacheStorage`) with `get`, `exist`/`exists`, `set`, `batch_exists_v2`, `batch_get`, and `batch_set`, registered with `StorageBackendFactory` as `--hicache-storage-backend pulsekv`.
- **Multi-Replica Cross-Cache Hit Demo (`deploy/demo-cross-replica-sglang.sh`):** Two independent serving replicas sharing a 4-node PulseKV cluster. Replica A computes and stores prompt prefix KV blocks; Replica B receives the same prefix and achieves a **100.0% cache hit rate** across all trials without recomputing attention state.

---

## 2. Exact Implementation Layout

```text
adapters/
├── pyproject.toml                     Package configuration & dependencies
├── pulsekv_adapters/
│   ├── __init__.py                    Exported client, storage, and key derivation symbols
│   ├── client.py                      Generic Python Client SDK (routing, chunking, bulk fallback)
│   ├── key.py                         SGLang chained SHA-256 block hash key derivation
│   ├── sglang.py                      PulseKVHiCacheStorage backend for SGLang
│   └── health_client.py               Phase 0 gRPC health probe
└── tests/
    ├── test_key_alignment.py          Step 7.2 block hash alignment vs SGLang reference
    ├── test_client.py                 Step 7.1 client SDK unit & live cluster tests (unary & 5MB chunked)
    ├── test_sglang_adapter.py         Step 7.1 storage interface & batch_exists_v2 tests
    ├── test_sglang_integration.py     Step 7.3 tensor round-trip & RadixCache lifecycle tests
    └── demo_cross_replica.py          Step 7.4 multi-replica cross-cache hit benchmark

deploy/
└── demo-cross-replica-sglang.sh       Automated cross-replica demo runner

docs/
└── pulsekv-v2-phase7-summary.md       This document
```

### Frozen Directory Verification

Per the Phase 7 scope specification, all core engine, control plane, node shim, and existing tests were strictly untouched:

```sh
$ git diff --stat -- src include tests node/engine node/grpc_shim control
# (empty - 0 files changed, 0 insertions, 0 deletions)
```

---

## 3. Architecture & Routing Flow

```text
+-----------------------------------------------------------------------------------+
| SGLang Serving Engine (Python)                                                    |
|                                                                                   |
|  [ RadixCache / HiCache Scheduler ]                                              |
|            │                                                                      |
|            ▼ (batch_exists_v2 / get / set)                                        |
|  [ PulseKVHiCacheStorage ]                                                        |
|            │                                                                      |
|            ▼                                                                      |
|  [ PulseKVClient ]                                                                |
|      ├── 1. Topology Discovery & Fingerprint Validation (port 7000)               |
|      ├── 2. FNV-1a 64-bit Rendezvous Hash: ShardForKey(key) -> Shard -> Owner     |
|      └── 3. Transport Dispatch:                                                   |
|             ├── Fast Path: Same-Host Unix / Bulk Socket (memfd / 32-byte header)  |
|             ├── Standard Path: NodeService.Get / Put (unary <= 4 MiB)             |
|             └── Large Streaming: NodeService.GetChunked / PutChunked (> 4 MiB)    |
+-----------------------------------------------------------------------------------+
                  │                                         │
                  ▼ (gRPC / Port 7000)                      ▼ (gRPC / Sockets)
       +──────────────────────+                  +──────────────────────+
       | Go Control Plane     |                  | C++ Data Nodes       |
       | ClusterMetadata      |                  | NodeService / Bulk   |
       +──────────────────────+                  +──────────────────────+
```

1. **Topology Discovery:** On initialization, `PulseKVClient` queries `ClusterMetadataService` on the Go control plane (e.g. `127.0.0.1:7000`). It verifies the SHA-256 fingerprint (`pulsekv-topology-v2`) across `GetNodeList` and `GetShardMap` to prevent torn-view reads during rebalancing.
2. **Shard Resolution:** `shard_for_key(key, shard_count)` computes `fnv1a_64(key) % shard_count`, matching `control/internal/router/router.go` bit-for-bit.
3. **Transport Routing:**
   - Values $\le 4\text{ MiB}$ execute via unary `NodeService.Put` / `Get`.
   - Values $> 4\text{ MiB}$ automatically stream in $1\text{ MiB}$ frames via `NodeService.PutChunked` / `GetChunked`.
   - Bulk transport over Unix sockets (`/tmp/pulsekv-bulk-<host>-<port>.sock`) or TCP fast-path is attempted with transparent fallback to gRPC on any error.

---

## 4. Key Scheme Alignment (Step 7.2)

SGLang derives cache keys by splitting prompt token sequences into discrete pages (typically 16, 64, or 128 tokens) and computing a SHA-256 digest over the current page's token IDs chained with the hex hash of the preceding page.

PulseKV's `pulsekv_adapters.key` module replicates this derivation:
- **Token Serialization:** Signed 4-byte big-endian integers (`int(t).to_bytes(4, byteorder="big", signed=True)`).
- **Chained Digest:** $\text{SHA256}(\text{prior\_hash\_bytes} + \text{token\_bytes})$.
- **Verification (`test_key_alignment.py`):** Tested with single tokens, multi-block prompts, and prefix sharing against reference SGLang implementations.

---

## 5. Live SGLang Integration & Storage Interface (Step 7.1 & 7.3)

`PulseKVHiCacheStorage` satisfies the complete SGLang L3 storage contract:

1. **`exist(key)` / `exists(key)`:** Fast existence probe.
2. **`set(key, value)`:** Supports raw bytes, `bytearray`, and direct `torch.Tensor` instances (extracting contiguous CPU memory buffer).
3. **`get(key, target_location=None)`:** Retrieves cached bytes, optionally zero-copy copying into a pre-allocated `torch.Tensor` target.
4. **`batch_exists_v2(keys, ...)`:** Computes `kv_hit_pages` (the longest continuous prefix of existing blocks starting from index 0) for SGLang's hierarchical cache scheduler.
5. **`batch_get(keys)` / `batch_set(key_values)`:** High-throughput batch operations.

---

## 6. Multi-Replica Cross-Cache Hit Benchmark (Step 7.4)

The demo script `deploy/demo-cross-replica-sglang.sh` runs multi-trial benchmarks simulating two independent serving replicas (`Replica A` and `Replica B`) sharing the same PulseKV v2 cluster.

### Reproduction Benchmark Results

#### Benchmark 1: 512-Token Prefix (32 Pages @ 16 Tokens/Page, 64 KB/Page)
```text
======================================================================
PulseKV v2 — SGLang Cross-Replica Prefix Cache Hit Demo (Step 7.4)
======================================================================
Control Plane:      127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002
Shared Prefix:      512 tokens (32 pages @ 16 tokens/page)
Page Size:          64 KB per page
Total Trials:       10
----------------------------------------------------------------------
Trial  1/10: Replica A Write:   5.68ms | Replica B Lookup+Read:   9.92ms | Result: HIT (100%)
Trial  2/10: Replica A Write:   6.97ms | Replica B Lookup+Read:  11.19ms | Result: HIT (100%)
Trial  3/10: Replica A Write:   7.77ms | Replica B Lookup+Read:   9.92ms | Result: HIT (100%)
Trial  4/10: Replica A Write:   7.52ms | Replica B Lookup+Read:  11.19ms | Result: HIT (100%)
Trial  5/10: Replica A Write:   7.50ms | Replica B Lookup+Read:  10.13ms | Result: HIT (100%)
Trial  6/10: Replica A Write:   7.78ms | Replica B Lookup+Read:  10.22ms | Result: HIT (100%)
Trial  7/10: Replica A Write:   7.63ms | Replica B Lookup+Read:  10.19ms | Result: HIT (100%)
Trial  8/10: Replica A Write:   7.99ms | Replica B Lookup+Read:  10.69ms | Result: HIT (100%)
Trial  9/10: Replica A Write:   7.79ms | Replica B Lookup+Read:  10.42ms | Result: HIT (100%)
Trial 10/10: Replica A Write:   7.36ms | Replica B Lookup+Read:   9.96ms | Result: HIT (100%)
----------------------------------------------------------------------
SUMMARY RESULTS:
  Trials Completed:            10
  Successful Cache Hits:       10/10
  Hit Rate:                    100.0%
  Total Shared KV Transferred: 20.00 MB
  Avg Replica A Write Time:    7.40 ms (512 tokens / 32 pages)
  Avg Replica B Read Time:     10.38 ms (512 tokens / 32 pages)
======================================================================
```

#### Benchmark 2: 1024-Token Prefix (64 Pages @ 16 Tokens/Page, 64 KB/Page)
```text
======================================================================
SUMMARY RESULTS (20 Trials):
  Trials Completed:            20
  Successful Cache Hits:       20/20
  Hit Rate:                    100.0%
  Total Shared KV Transferred: 80.00 MB
  Avg Replica A Write Time:    15.02 ms (1024 tokens / 64 pages)
  Avg Replica B Read Time:     21.61 ms (1024 tokens / 64 pages)
======================================================================
```

**Reproduction Rate:** **100.0%** across all test cycles ($30/30$ consecutive successful trials across both runs).

---

## 7. Test Suite Summary

All test suites pass with zero regressions across the codebase:

| Suite | Scope | Result |
| :--- | :--- | :--- |
| `adapters/tests/test_key_alignment.py` | SGLang SHA-256 hash chaining & key alignment | **5/5 passed** |
| `adapters/tests/test_client.py` | Python SDK unit tests, unary, 5MB chunked, prefix match | **5/5 passed** |
| `adapters/tests/test_sglang_adapter.py` | SGLang `HiCacheStorage` 3-method & batch APIs | **5/5 passed** |
| `adapters/tests/test_sglang_integration.py`| SGLang tensor round-trip & RadixCache prefix lifecycle | **5/5 passed** |
| `deploy/smoke-test.sh` | Control plane Raft + C++ NodeService + Reflection | **7/7 passed** (95/95 checks) |
| `deploy/test-engine.sh` | Pure-C storage engine tiering & stress tests | **4/4 passed** (121/121 checks) |
| `deploy/demo-cross-replica-sglang.sh` | Cross-replica prefix cache hit benchmark | **100.0% hit rate** |

---

## 8. Phase 8 Handoff

Phase 7 successfully achieved the MVP milestone. The system is ready for **Phase 8: vLLM KVConnector Adapter**.

Key context for Phase 8:
- The Python client SDK (`pulsekv_adapters.client.PulseKVClient`) is fully general and can be reused directly by the vLLM KVConnector v1 adapter (`pulsekv_adapters.vllm`).
- vLLM's connector requires separate worker-side tensor transfer and scheduler-side metadata coordination.
- `src/`, `include/`, `tests/`, `node/`, and `control/` remain frozen contracts.
