#!/usr/bin/env bash
#
# Node-level benchmark: the two scenarios Phase 1 has to record.
#
#   deploy/bench-node.sh [--ops N] [--concurrency N] [--value-size N] [--port N]
#
# Boots ONE dedicated node with a deliberately small RAM budget, runs the
# benchmark twice against it, then stops it:
#
#   fits-in-RAM   working set sized to sit inside the budget      -> no spilling
#   exceeds-RAM   working set several times the budget            -> constant spilling
#
# The difference between the two is the cost of the NVMe tier, measured rather
# than asserted. This is the single-node baseline Phase 9's cluster benchmark
# will compare distributed overhead against.
#
# WHY A DEDICATED NODE rather than one from deploy/run-local-cluster.sh: the
# scenarios are defined by the ratio of working set to RAM budget, so the budget
# has to be part of the experiment. Reusing a shared cluster node would mean
# either a 256 MiB working set on disk per run, or a result that depends on
# whatever else had already been written to that node.
#
# NOTE ON THE BUDGET. The engine divides ram_budget_bytes across 256 lock
# shards, so what actually decides spilling is (budget / 256) versus
# (working set / 256) per shard. The sizes below are chosen against the
# per-shard figure, not the headline one.
#
# NOTE ON WHERE THE SPILL DIRECTORY LIVES. It defaults to a container-local
# path (/tmp), NOT deploy/run/ under the repo. On the normal macOS + colima
# setup the repo is a virtiofs bind mount, and putting the NVMe tier there would
# measure the host-to-VM filesystem bridge rather than the tier. Override with
# --data-dir when benchmarking against real storage, and say which you used
# when reporting numbers.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

PORT=7200
OPS=40000
CONCURRENCY=16
VALUE_SIZE=16384
READ_RATIO=0.8
RAM_BUDGET=$((64 * 1024 * 1024))   # 64 MiB => 256 KiB per shard
BENCH_DATA="${TMPDIR:-/tmp}/pulsekv-bench-data"

while [ $# -gt 0 ]; do
    case "$1" in
        --port)         PORT="$2"; shift 2 ;;
        --ops)          OPS="$2"; shift 2 ;;
        --concurrency)  CONCURRENCY="$2"; shift 2 ;;
        --value-size)   VALUE_SIZE="$2"; shift 2 ;;
        --read-ratio)   READ_RATIO="$2"; shift 2 ;;
        --ram-budget)   RAM_BUDGET="$2"; shift 2 ;;
        --data-dir)     BENCH_DATA="$2"; shift 2 ;;
        -h|--help)      sed -n '2,34p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)              pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

# Per-shard budget is what matters. 64 MiB / 256 = 256 KiB per shard.
#   fits    : 2048 keys * 16 KiB = 32 MiB total, 8 keys (128 KiB) per shard  -> half the shard budget
#   exceeds : 16384 keys * 16 KiB = 256 MiB total, 64 keys (1 MiB) per shard -> 4x the shard budget
FITS_KEYS=2048
EXCEEDS_KEYS=16384

[ -x "$PULSEKV_BENCH_BIN" ] || pk_die "$(pk_relpath "$PULSEKV_BENCH_BIN") is missing; run deploy/run-local-cluster.sh first"
[ -x "$PULSEKV_NODE_BIN" ]  || pk_die "$(pk_relpath "$PULSEKV_NODE_BIN") is missing; run deploy/run-local-cluster.sh first"

BENCH_DIR="${PULSEKV_RUN_DIR}/bench"
BENCH_LOG="${BENCH_DIR}/node.log"
mkdir -p "$BENCH_DIR" "$BENCH_DATA"

NODE_PID=""
cleanup() {
    if [ -n "$NODE_PID" ] && kill -0 "$NODE_PID" 2>/dev/null; then
        kill -TERM "$NODE_PID" 2>/dev/null || true
        for _ in $(seq 1 50); do
            kill -0 "$NODE_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$NODE_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

pk_step "Starting a dedicated benchmark node on 127.0.0.1:${PORT}"
pk_info "ram-budget  $RAM_BUDGET bytes ($((RAM_BUDGET / 1024 / 1024)) MiB, $((RAM_BUDGET / 256 / 1024)) KiB per shard)"
pk_info "value size  $VALUE_SIZE bytes, concurrency $CONCURRENCY, read ratio $READ_RATIO"
pk_info "data-dir    $BENCH_DATA"
pk_info "log         $(pk_relpath "$BENCH_LOG")"
case "$BENCH_DATA" in
    "$PULSEKV_REPO_ROOT"/*)
        pk_warn "the spill directory is inside the repo. On macOS + colima that is a"
        pk_warn "virtiofs bind mount, and the NVMe-tier numbers will describe the"
        pk_warn "host filesystem bridge rather than the tier. Pass --data-dir /tmp/... instead."
        ;;
esac

: > "$BENCH_LOG"
"$PULSEKV_NODE_BIN" \
    --node-id bench-node --host 127.0.0.1 --port "$PORT" \
    --data-dir "$BENCH_DATA" \
    --ram-budget-bytes "$RAM_BUDGET" \
    --max-value-bytes $((64 * 1024 * 1024)) \
    >>"$BENCH_LOG" 2>&1 &
NODE_PID=$!

# Wait for it to answer rather than sleeping and hoping.
ready=0
for _ in $(seq 1 60); do
    if grep -q "NodeService listening" "$BENCH_LOG" 2>/dev/null; then ready=1; break; fi
    kill -0 "$NODE_PID" 2>/dev/null || break
    sleep 0.25
done
if [ "$ready" -ne 1 ]; then
    pk_err "the benchmark node did not come up. Log:"
    cat "$BENCH_LOG" >&2
    exit 1
fi
pk_ok "node up (pid $NODE_PID)"
echo

run_scenario() {
    local label="$1" keys="$2" prefix="$3"
    echo
    "$PULSEKV_BENCH_BIN" \
        --address "127.0.0.1:${PORT}" \
        --label "$label" \
        --keys "$keys" \
        --key-prefix "$prefix" \
        --value-size "$VALUE_SIZE" \
        --concurrency "$CONCURRENCY" \
        --ops "$OPS" \
        --warmup-ops $((OPS / 10)) \
        --read-ratio "$READ_RATIO"
}

rc=0
run_scenario "scenario 1 of 2: fits in RAM" "$FITS_KEYS" "fits" || rc=$?
if [ "$rc" -ne 0 ]; then
    pk_err "the fits-in-RAM scenario failed"
    exit "$rc"
fi

# A separate key namespace so scenario 2 does not read scenario 1's residency
# as its own warm cache.
run_scenario "scenario 2 of 2: several times RAM" "$EXCEEDS_KEYS" "exceeds" || rc=$?
if [ "$rc" -ne 0 ]; then
    pk_err "the exceeds-RAM scenario failed"
    exit "$rc"
fi

pk_step "Stopping the benchmark node"
cleanup
NODE_PID=""
pk_info "node's own final tier accounting:"
grep -E "^\[bench-node\] final:" "$BENCH_LOG" | sed 's/^/    /' || true
echo
printf '%s==> benchmark complete%s\n\n' "$PK_BOLD$PK_GREEN" "$PK_RESET"
