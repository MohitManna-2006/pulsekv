# PulseKV v2 — Phase 6 Summary

**Status: complete.** Read this first if you are picking up Phase 7.

Phase 1 proved chunked gRPC framing is *correct* for multi-megabyte KV-cache
blocks. Phase 6 measured it, found it expensive, and added a second data path
beside it: a raw framed socket protocol with a shared-memory handoff, taken only
for large values and falling back to gRPC on any failure whatsoever.

The measured result, in one line: **the bulk transport beats the Phase 1 chunked
path by 1.06x–13.0x depending on payload size and concurrency, and the
shared-memory handoff is the best transport at 64 MiB (1.70x) while the raw
socket wins hardest on small values (up to 13.0x).** Full table in §5.

Two findings are as important as the speedups and are reported in full rather
than buried:

- A `vmsplice`/`splice` send path — the first thing the design doc names — was
  implemented, benchmarked well, and **corrupted data under concurrent readers**.
  It was removed. §6 has the proof.
- A literal `sendfile()` **from the NVMe spill tier could not be built at all**,
  because the engine boundary this phase was scoped away from is the only thing
  that knows where a spilled value lives. §7 quantifies what that costs.

---

## 1. Scope reconciliation, stated up front

The repo's Phase 5 handoff (`pulsekv-v2-phase5-summary.md` §9) says Phase 6
should begin by changing `node/engine/`'s value-copy boundary, calling it "the
one place where the engine header's contract will genuinely have to change".

This phase's prompt forbids modifying `node/engine/` and verifies it as an exit
criterion. The prompt won. That decision is load-bearing rather than cosmetic,
and §7 gives it numbers: everything below is what a bulk transport can do while
`pk_engine_get` remains the only way to reach a value, which is to say while
every transfer starts with a private heap copy the transport cannot avoid.

`proto/` was also left alone. It did not need to change — see §3 — which is why
`git diff --stat -- control` and `-- adapters` are both empty this phase rather
than showing the usual regenerated stubs.

---

## 2. Exact implementation layout

```text
node/grpc_shim/
├── bulk.h          269  wire format, endpoint naming, Blob, Server, Client
├── bulk.cc        1146  framing, memfd/SCM_RIGHTS, sendfile, the socket servers,
│                        and the removed-splice post-mortem
├── bulk_bench.cc   638  benchmark harness AND the synthetic adapter AND the
│                        exit-criterion-3 replication verifier
├── main.cpp             + bulk listener, + bulk fast path in replication
│                        forwarding and catch-up, + flags. ShardForKey moved
│                        into bulk.h so node and benchmark share one copy.
└── CMakeLists.txt       + pulsekv_bulk library, + pulsekv-bulk-bench target

deploy/
├── bench-bulk.sh              measure/change/remeasure runner, --sweep mode
└── verify-bulk-replication.sh exit criterion 3: large-value replication + catch-up

docs/pulsekv-v2-phase6-summary.md   this file
```

Untouched, and verified so: `node/engine/`, `control/`, `adapters/`, `src/`,
`include/`, `tests/`, `proto/`.

---

## 3. The transport (step 6.1)

A fixed 32-byte big-endian header, explicit lengths on everything, three
opcodes (`PING`, `GET`, `PUT`), bounds-checked against the node's
`--max-value-bytes` *before* a byte is allocated — the same discipline
`PutChunked` already enforces. It carries no protobuf at all.

Each node listens on two sockets:

| | address | used for |
|---|---|---|
| TCP | `--port + --bulk-port-offset` (default +1000) | node-to-node, possibly cross-host |
| Unix | `<--bulk-socket-dir>/pulsekv-bulk-<host>-<port>.sock` | same-host, and the only path that can hand over a memfd |

**Endpoint discovery is by convention, not by service discovery**, which step
6.1 explicitly asked for and which is also what kept `proto/` and `control/`
untouched. A peer's bulk endpoints are derived from its NodeService address. The
socket path is keyed by that address, so a client looking for a *remote* peer's
socket looks for a path that simply does not exist on its filesystem — which is
also the same-host test, obtained for free and correct by construction.

Convention alone would be unsafe, so the `PING` handshake returns the server's
node ID and the client rejects any endpoint whose ID is not the one the topology
names. A misrouted socket is caught before a single byte of data is read.

**Who uses it.** Phase 4's replication forwarding takes it for values above the
4 MiB unary limit, and Phase 4's newly-owned-shard catch-up takes it for every
`value_omitted` value it backfills — the single largest consumer of bulk
transfers in the system, since a node that gains 29 shards pulls every oversized
value in them. Both fall back to the gRPC chunked path per request on any
failure. A bulk `PUT` stores locally and never forwards, which is the same
loop-prevention rule `from_replication` encodes on the gRPC side.

---

## 4. The zero-copy path (step 6.2)

**Same-host shared memory.** On a `GET` over the Unix socket from a client that
advertised it can accept one, the server stages the value in a `memfd`, seals it
`F_SEAL_WRITE|SHRINK|GROW`, and passes the descriptor over `SCM_RIGHTS`. The
client `mmap`s it and reads in place. **No payload bytes cross the socket.** The
seal is what makes the shared mapping safe to read without a copy: the receiver
has a kernel-enforced guarantee the region cannot be rewritten under it.

This is the SGLang HiCache pattern the design doc points at — payload in a
shared region, control message carrying only the descriptor.

**NVMe-tier-to-network.** Not implementable as specified; see §7. What exists
instead is a `--bulk-send-mode sendfile` that stages the value in a memfd and
`sendfile()`s it — zero copies onto the socket, one copy to populate the region.
It is measured (§5) precisely to put a number on what a value that *already*
lived in a file would save.

**Fallback is total, and is the reason this is safe to enable by default.** Every
precondition failure — peer on another host, no `memfd_create`, refused
connection, protocol error, staging failure — returns false and the caller uses
gRPC. Verified: with `--no-bulk-transport`, the benchmark reports every bulk
transport `SKIPPED` with `connect: Connection refused`, gRPC serves every
transfer, and the full smoke suite passes 95/95.

---

## 5. Benchmark (step 6.3)

`deploy/bench-bulk.sh --sweep`, on a dedicated node with replication disabled so
the numbers are transport cost and nothing else. Every transfer verified
byte-for-byte; an unverified read fails the run. Ratios are against the gRPC
chunked baseline measured **in the same run**, on aggregate throughput
(bytes ÷ wall clock).

Default send mode (`write`):

| payload | readers | bulk TCP | bulk unix, inline | bulk unix, **memfd** |
|---|---:|---:|---:|---:|
| 1 MiB | 1 | **5.06x** | 4.00x | 2.34x |
| 1 MiB | 4 | 8.40x | **13.03x** | 7.82x |
| 1 MiB | 8 | 5.28x | **5.40x** | 4.61x |
| 8 MiB | 1 | **1.45x** | 1.09x | 1.06x |
| 8 MiB | 4 | 2.30x | 2.56x | **2.71x** |
| 8 MiB | 8 | 2.59x | 2.46x | **2.65x** |
| 64 MiB | 1 | 0.95x | 0.75x | **1.16x** |
| 64 MiB | 4 | 1.34x | 1.22x | **1.63x** |
| 64 MiB | 8 | 1.37x | 1.36x | **1.70x** |

Absolute figures for the 64 MiB × 8 case: gRPC chunked 346.7 ms p50 / 971 MiB/s
aggregate; memfd handoff 196.9 ms p50 / 1654 MiB/s aggregate.

**Reading this honestly:**

- **Small values are where the raw socket wins hugely** (up to 13x). Nothing
  zero-copy is involved. That is gRPC's per-request cost — HTTP/2 framing,
  protobuf, slice management — being removed, and at 1 MiB it dominates.
- **Concurrency is what makes the shared-memory path win.** At 8 MiB with one
  reader, memfd is 1.06x — barely anything. With 4–8 readers it is the best
  transport. Single-stream loopback measures link bandwidth, which is enormous;
  concurrency measures CPU per byte, which is what a storage server actually
  runs out of.
- **At 64 MiB with one reader the raw socket is slower than gRPC** (0.95x /
  0.75x). Being honest about that: one large `write()` has no pipelining, while
  gRPC's 1 MiB chunks overlap send and receive. The memfd path wins there
  (1.16x) precisely because it moves no bytes at all.
- **`sendfile` mode loses to `write` in every single cell** — 4.59x vs 5.06x at
  1 MiB×1, 0.76x vs 1.45x at 8 MiB×1, 1.74x vs 2.59x at 8 MiB×8. Exactly as
  expected: staging into a memfd adds a copy to save one. It is kept only as the
  measurement in §7.
- **Run-to-run variance is real.** The memfd handoff measured 2.34x at 1 MiB×1
  in one run and 9.93x in another. Cross-run comparisons are not meaningful
  here; every ratio above is against a baseline measured in the same run, and
  even those should be read as "roughly this much", not to two decimal places.

---

## 6. The splice path that was removed

The design doc names `sendfile`/`splice` first, so the obvious implementation
was: `vmsplice()` the engine's value buffer into a pipe, `splice()` the pipe into
the socket. It was written, it benchmarked well (9.88x at 1 MiB), and it passed
every single-threaded test.

Under concurrent readers it returned **wrong bytes** — 50 of 80 transfers failed
verification at 1 MiB × 8 readers.

The cause is documented kernel behaviour, not a mystery. `vmsplice()` does not
copy; it maps the caller's pages into the pipe *by reference*, and `splice()`
hands those same references to the socket's send queue. The kernel is still
referencing those pages after `splice()` returns, until TCP actually transmits.
The code frees the engine buffer immediately, the allocator gives those pages to
another request thread, that thread writes its own value into them, and the
kernel transmits whatever is there now. Single-threaded, nothing reuses the
memory fast enough to notice.

**Proven, not inferred.** With the value copied into a deliberately leaked buffer
whose pages could never be returned to the allocator, the identical case went
from 50/80 failing to **80/80 passing**. One variable changed.

A first fix — clearing `SPLICE_F_MORE` on the final segment — was also real and
also necessary (it was corking the last bytes of every transfer until a ~200 ms
TCP timer fired, showing up as p95 two orders of magnitude above p50). It did not
fix the corruption, because the corruption was never a framing problem.

Doing it safely needs `MSG_ZEROCOPY` with `SO_EE_ORIGIN_ZEROCOPY` completion
notifications so the buffer is held until the kernel is finished with it — which
is the `io_uring`-class machinery the design doc defers "until the simpler path
is measured". It now has been. `SPLICE_F_GIFT` is not an escape hatch: it makes
the "never touch this memory again" contract explicit rather than implicit, and
freeing to an allocator violates it just as hard.

The post-mortem lives in `node/grpc_shim/bulk.cc` where someone would go to
re-add it.

---

## 7. What the engine boundary costs, with a number

A literal `sendfile()` from the NVMe spill tier is the case the implementation
plan names first, and it **cannot be built without changing `node/engine/`**.
`pulsekv_engine.h` exposes no descriptor, path, or offset for a spilled value;
`pk_engine_get` allocates a private heap buffer and copies into it. There is no
file for `sendfile()` to read from.

So every transfer above starts with a copy the transport cannot remove, and the
shared-memory path pays a second one to get those bytes into a shareable region:

```
today          engine file ──copy──► heap buffer ──copy──► memfd ──fd──► adapter
with the       engine file ─────────────sendfile()──────────────────────► peer
engine change  engine mapping ─────────────fd───────────────────────────► adapter
```

`--bulk-send-mode sendfile` measures the difference directly. It is the `write`
path plus one staging copy, and it costs **0.76x vs 1.45x at 8 MiB (single
reader) and 1.74x vs 2.59x at 8 MiB × 8** — i.e. the staging copy alone gives up
roughly a third to a half of the transport's advantage. A value already in a
file skips that copy, so that margin is a reasonable first estimate of what
exposing the spill descriptor would return, before counting the heap copy it
would also remove.

That is the concrete case for the engine-header change the Phase 5 handoff
predicted, now with a measurement attached instead of an assertion.

---

## 8. Verification evidence

### Correctness

Four legs, one container session, each booting a fixture and stopping cleanly:

| Leg | Configuration | Result |
|---|---|---|
| 1 | 4 nodes, replication on, **bulk on** | smoke 95/95; large-value replication + catch-up verified |
| 2 | same, **bulk disabled** | smoke 95/95 (the fallback path) |
| 3 | 8 nodes, data-node chaos, bulk on | pre-smoke 175/175, chaos 3 cycles, post-smoke 175/175 |
| 4 | 8 nodes, concurrent leader + data-node chaos | chaos 2 cycles, smoke 175/175 |

Phase 1's chunked-transfer tests are inside the smoke suite and all still pass:
the 6 MiB `PutChunked`/`GetChunked` round-trip byte-for-byte on every node, the
oversized-unary refusal naming `PutChunked`, and all eight deliberately
malformed chunked framings rejected with the right status and leaving no key
behind. The engine suite is untouched and green (47/20/38/16 checks).

Phase 4's assertions all still hold with the bulk path carrying the bytes:
6 promotion proofs over 48 keys (leg 3) and 4 over 32 (leg 4), both including
`catch-up-after-rejoin`; replica agreement swept 422 and 429 times with **0
disagreements**.

**Exit criterion 3 specifically** — `deploy/verify-bulk-replication.sh` writes a
6 MiB value (above the unary limit, so it takes the bulk path) directly at its
primary, waits for every holder the control plane names to serve it
byte-for-byte, then **destroys the primary outright** and requires it to serve
the value again after restart. The engine has no WAL and purges its spill tier
at start, so the restarted node is genuinely empty: the only way it can answer
is catch-up pulling the value from a peer. Both checks pass.

### The one intermittent failure, reported rather than re-run away

The first full proof run had leg 4 fail: `generation did not increase: 8 -> 0`,
plus a downstream smoke routing failure. It did not reproduce.

The cause is identifiable from the logs and is **pre-existing Phase 5 behaviour,
not Phase 6's**: a restarted control-plane replica serves an
authoritative-looking *empty* topology — generation 0, zero live nodes — for the
~1.5 s between its process starting and Raft replaying its log. `cp-0` was the
leader the chaos scenario killed, and the chaos watcher's data-node stream reads
a fixed `replicas[0]`, so it sampled that window.

Checked rather than assumed: the leg was run **3 times with the bulk transport
enabled and 3 times with it disabled — 6/6 passed**, and the failure mechanism
involves only the control plane, which the bulk transport never contacts. A
re-run of the full proof passed all four legs.

Fixing it belongs to whoever next touches `control/`, which this phase may not:
a replica should not serve `ClusterMetadataService` until its FSM has caught up
with its own persisted log, and the chaos watcher should read topology through
the same multi-replica fallback the SDK already uses instead of pinning one
replica.

### Build

`-Wall -Wextra`, zero warnings, for `pulsekv-node`, `pulsekv_bulk`, and
`pulsekv-bulk-bench`.

---

## 9. Deliberate limits and honest interpretation

- **`io_uring` and RDMA-class transport remain deferred**, as the design doc
  directs. §6 is now the concrete argument for `MSG_ZEROCOPY` completions being
  the next real step rather than a speculative one.
- **A true NVMe-to-network `sendfile()` does not exist**, for the reason in §7.
  What ships is a staged-memfd `sendfile` that is measurably *slower* than a
  plain write and is kept as a measurement, not a recommendation.
- **The bulk server is thread-per-connection**, bounded at 64. Connections are
  cached per peer so the count is small, but this is not the epoll core v1 built
  and a fan-out much wider than a dev cluster would want one.
- **Endpoint discovery is a convention, not an advertisement.** Safe because the
  handshake verifies node identity, but it assumes every node uses the same
  `--bulk-port-offset` and `--bulk-socket-dir`. Promoting it to a `NodeInfo`
  field is a small additive change and is the right Phase 7 move.
- **Same-host detection is "the Unix socket path exists locally".** Correct by
  construction for a dev cluster and cheap, but a shared filesystem across hosts
  would defeat it — the node-ID handshake catches that, at the cost of a failed
  connection rather than a clean skip.
- **The memfd handoff copies twice on the server.** §7. It is still the fastest
  transport at 64 MiB, which says more about what gRPC costs than about how good
  two copies are.
- **Values are still buffered whole.** Phase 1's limitation is unchanged: nothing
  streams *into* the engine, because that is the boundary this phase did not
  cross. A 64 MiB value exists in full in the sender's heap, in full in the
  memfd, and in full in the receiver.
- **No new chaos scenario was added, deliberately.** The bulk transport
  introduces no new failure domain: it has no leader, no quorum, no persistent
  state, and no liveness property of its own. Its only failure mode is "this
  request did not work", whose blast radius is one fallback to gRPC. The
  existing suites exercise it because they now carry large values over it, which
  is the coverage that matters. Asked and answered rather than skipped.
- **Benchmarks are loopback, one machine, one container.** Cross-host numbers
  would look very different — the raw socket's advantage should grow and the
  shared-memory path stops applying entirely.

---

## 10. Phase 7 handoff

Phase 7 is the SGLang HiCache adapter, and it is the first phase that will drive
this transport from a real external caller.

1. **The synthetic adapter is already the shape of the real one.**
   `pulsekv-bulk-bench` connects over the Unix socket, receives a sealed memfd,
   and reads the tensor in place without copying. A HiCache backend's `get()`
   is that, plus handing the mapping to the framework instead of verifying it.
   Start from `bulk::Client::Get` and `bulk::Blob`.
2. **`Blob::mapped()` tells the caller whether it got a mapping or a buffer**, so
   an adapter can hand a zero-copy pointer to the GPU path when it has one and
   fall back to a copy when it does not, without branching on transport details.
3. **Promote endpoint discovery to `NodeInfo`.** An adapter should not have to
   know the port-offset convention. This is the additive metadata change Phase
   5's handoff already identified as the right home, and it is where the
   convention in §3 should end up.
4. **Expect to want the engine change.** §7 is the case, with numbers. An adapter
   pulling KV blocks is exactly the workload where the two staging copies hurt,
   and it is the first caller that will feel them.

The contracts to preserve now number seven — the six from Phase 5, plus: **the
bulk transport is never required.** Any code path that cannot complete a request
without it is a bug, not an optimisation.
