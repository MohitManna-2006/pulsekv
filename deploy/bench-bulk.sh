#!/usr/bin/env bash
#
# Phase 6 bulk-transfer benchmark: measure, change, remeasure.
#
#   deploy/bench-bulk.sh [--value-bytes N] [--iterations N] [--port N]
#                        [--concurrency N] [--sweep]
#
# Boots a DEDICATED node with replication disabled, so the numbers measure
# transport cost and nothing else -- a node with peers would be replicating
# every write in the background and the comparison would be against a moving
# target. Then runs the benchmark once per server-side send mode, because the
# send strategy is a property of the sender, not of the request.
#
# Every transfer is verified byte-for-byte inside the harness; an unverified
# read fails the run.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

VALUE_BYTES=$((8 * 1024 * 1024))
ITERATIONS=20
WARMUP=3
CONCURRENCY=1
SWEEP=0
PORT=7190
NODE_ID=bulkbench
SOCKET_DIR="${PULSEKV_RUN_DIR}/bulk-sockets"

while [ $# -gt 0 ]; do
    case "$1" in
        --value-bytes)  [ $# -ge 2 ] || pk_die "--value-bytes requires a size"; VALUE_BYTES="$2"; shift 2 ;;
        --value-bytes=*) VALUE_BYTES="${1#*=}"; shift ;;
        --iterations)   [ $# -ge 2 ] || pk_die "--iterations requires a count"; ITERATIONS="$2"; shift 2 ;;
        --iterations=*) ITERATIONS="${1#*=}"; shift ;;
        --port)         [ $# -ge 2 ] || pk_die "--port requires a port"; PORT="$2"; shift 2 ;;
        --port=*)       PORT="${1#*=}"; shift ;;
        --concurrency)  [ $# -ge 2 ] || pk_die "--concurrency requires a count"; CONCURRENCY="$2"; shift 2 ;;
        --concurrency=*) CONCURRENCY="${1#*=}"; shift ;;
        --sweep)        SWEEP=1; shift ;;
        -h|--help)      sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)              pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

BENCH_BIN="${PULSEKV_CMAKE_DIR}/pulsekv-bulk-bench"
DATA_DIR="${PULSEKV_RUN_DIR}/bulk-bench-data"
LOG_DIR="${PULSEKV_LOG_DIR}"

pk_step "Building the node and the bulk benchmark"
pk_require cmake "Install the v2 dev image (deploy/Dockerfile)."
mkdir -p "$PULSEKV_CMAKE_DIR" "$LOG_DIR" "$SOCKET_DIR"
cmake -S "$PULSEKV_REPO_ROOT/node/grpc_shim" -B "$PULSEKV_CMAKE_DIR" \
      -DCMAKE_BUILD_TYPE=Release >"${LOG_DIR}/bulk-cmake.log" 2>&1
cmake --build "$PULSEKV_CMAKE_DIR" -j "$(nproc 2>/dev/null || echo 4)" \
      --target pulsekv-node pulsekv-bulk-bench >>"${LOG_DIR}/bulk-cmake.log" 2>&1
pk_ok "$(pk_relpath "$PULSEKV_NODE_BIN")"
pk_ok "$(pk_relpath "$BENCH_BIN")"

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
trap cleanup EXIT

# run_case SEND_MODE EXTRA_NODE_FLAGS...
run_case() {
    local send_mode="$1"; shift
    local log="${LOG_DIR}/bulk-bench-${send_mode}.log"

    rm -rf "$DATA_DIR"
    mkdir -p "$DATA_DIR"
    : > "$log"

    # No --metadata-addr: replication stays off so the measurement is transport
    # cost alone.
    "$PULSEKV_NODE_BIN" --node-id "$NODE_ID" --host 127.0.0.1 --port "$PORT" \
        --data-dir "$DATA_DIR" \
        --bulk-socket-dir "$SOCKET_DIR" \
        --bulk-send-mode "$send_mode" \
        "$@" >>"$log" 2>&1 &
    NODE_PID=$!

    local ready=0
    for _ in $(seq 1 100); do
        if grep -q "NodeService listening" "$log" 2>/dev/null; then ready=1; break; fi
        kill -0 "$NODE_PID" 2>/dev/null || break
        sleep 0.1
    done
    [ "$ready" -eq 1 ] || { pk_err "node did not start"; tail -20 "$log" >&2; return 1; }

    local rc=0
    if [ "$SWEEP" -eq 1 ]; then
        # A sweep, because one payload size and one reader count cannot tell
        # you whether a transport wins on bandwidth or on CPU per byte -- and
        # for these transports the answer differs.
        local size concurrency
        for size in $((1024 * 1024)) $((8 * 1024 * 1024)) $((64 * 1024 * 1024)); do
            for concurrency in 1 4 8; do
                printf '\n--- payload %s MiB, %s reader(s) ---\n' \
                    "$((size / 1024 / 1024))" "$concurrency"
                "$BENCH_BIN" --node "127.0.0.1:${PORT}" --node-id "$NODE_ID" \
                    --socket-dir "$SOCKET_DIR" \
                    --value-bytes "$size" --iterations "$ITERATIONS" \
                    --warmup "$WARMUP" --concurrency "$concurrency" \
                    | grep -E "p50|verified byte" || rc=$?
            done
        done
    else
        "$BENCH_BIN" --node "127.0.0.1:${PORT}" --node-id "$NODE_ID" \
            --socket-dir "$SOCKET_DIR" \
            --value-bytes "$VALUE_BYTES" --iterations "$ITERATIONS" --warmup "$WARMUP" \
            --concurrency "$CONCURRENCY" || rc=$?
    fi

    kill -TERM "$NODE_PID" 2>/dev/null || true
    wait "$NODE_PID" 2>/dev/null || true
    NODE_PID=""
    return $rc
}

failed=0

pk_step "Send mode: write (the baseline bulk sender)"
run_case write || failed=1

pk_step "Send mode: sendfile (stage in a memfd, sendfile it)"
run_case sendfile || failed=1

pk_step "Control: bulk transport disabled (Phase 1 chunked gRPC only)"
run_case write --no-bulk-transport || failed=1

echo
if [ "$failed" -eq 0 ]; then
    printf '%s==> bulk benchmark complete%s\n' "$PK_BOLD$PK_GREEN" "$PK_RESET"
else
    printf '%s==> BULK BENCHMARK FAILED%s\n' "$PK_BOLD$PK_RED" "$PK_RESET"
    exit 1
fi
