#!/usr/bin/env bash

set -euo pipefail

server_binary=${1:-./build/pulsekv}
requests=${PULSEKV_BENCH_REQUESTS:-1000}
warmup=${PULSEKV_BENCH_WARMUP:-50}

online_cpus=$(nproc)
if (( online_cpus > 1 )); then
    default_workers=$((online_cpus - 1))
else
    default_workers=1
fi
if (( default_workers > 16 )); then
    default_workers=16
fi
workers=${PULSEKV_BENCH_WORKERS:-$default_workers}

bench_dir=$(mktemp -d /tmp/pulsekv-benchmark-XXXXXX)
server_pid=

cleanup() {
    if [[ -n ${server_pid} ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -INT "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    rm -rf -- "$bench_dir"
}
trap cleanup EXIT INT TERM

PULSEKV_QUIET=1 \
PULSEKV_THREADS="$workers" \
PULSEKV_WAL_PATH="$bench_dir/pulsekv.wal" \
"$server_binary" >"$bench_dir/server.log" 2>&1 &
server_pid=$!

ready=false
for _ in $(seq 1 200); do
    if { exec 3<>/dev/tcp/127.0.0.1/9999; } 2>/dev/null; then
        exec 3>&-
        exec 3<&-
        ready=true
        break
    fi
    sleep 0.05
done
if [[ $ready != true ]]; then
    printf '%s\n' "server did not become ready" >&2
    cat "$bench_dir/server.log" >&2
    exit 1
fi

printf '=== benchmark environment: %s CPUs, %s server workers ===\n' \
       "$online_cpus" "$workers"
for workload in read mixed write; do
    ./build/benchmark \
        --workload "$workload" \
        --requests "$requests" \
        --warmup "$warmup"
done

kill -INT "$server_pid"
wait "$server_pid"
server_pid=

printf '%s\n' '=== server summary ==='
cat "$bench_dir/server.log"
