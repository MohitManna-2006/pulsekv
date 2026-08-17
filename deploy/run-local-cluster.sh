#!/usr/bin/env bash
#
# Boot the PulseKV v2 local multi-process dev cluster.
#
#   deploy/run-local-cluster.sh [--config PATH] [--skip-build] [--restart]
#                               [--timeout SECONDS]
#
# Builds the Go control plane, Go gossip sidecar, and C++ grpc_shim node. It
# starts one control-plane gossip observer plus one data process and one
# membership sidecar per configured node. A sidecar starts only after all data
# services pass HealthCheck, so gossip never advertises an unready endpoint.
#
# PROCESS LIFETIME: processes are started in the background and their PIDs are
# written to deploy/run/cluster.pids. This script returns as soon as the
# cluster is healthy; the cluster keeps running until
# deploy/stop-local-cluster.sh. Per-process logs land in deploy/run/logs/.
#
# The alternative -- holding everything in the foreground -- would make it
# impossible to run deploy/smoke-test.sh against the cluster from the same
# shell, which is the normal workflow.
#
# Intended to run inside the v2 dev image (see deploy/Dockerfile):
#
#   docker run --rm -it -v "$PWD:/src" -w /src pulsekv-v2-dev bash
#
# For a single non-interactive run (CI shape):
#
#   docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev bash -c '
#     deploy/run-local-cluster.sh && deploy/smoke-test.sh; rc=$?
#     deploy/stop-local-cluster.sh; exit $rc'

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

SKIP_BUILD=0
RESTART=0
HEALTH_TIMEOUT=15

while [ $# -gt 0 ]; do
    case "$1" in
        --config)     [ $# -ge 2 ] || pk_die "--config requires a path"; PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)   PULSEKV_CONFIG="${1#*=}"; shift ;;
        --timeout)    [ $# -ge 2 ] || pk_die "--timeout requires seconds"; HEALTH_TIMEOUT="$2"; shift 2 ;;
        --timeout=*)  HEALTH_TIMEOUT="${1#*=}"; shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        --restart)    RESTART=1; shift ;;
        -h|--help)    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)            pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

case "$HEALTH_TIMEOUT" in
    ''|*[!0-9]*) pk_die "--timeout must be a positive integer, got: $HEALTH_TIMEOUT" ;;
esac
HEALTH_TIMEOUT=$((10#$HEALTH_TIMEOUT))
[ "$HEALTH_TIMEOUT" -gt 0 ] || pk_die "--timeout must be a positive integer, got: $HEALTH_TIMEOUT"

[ -f "$PULSEKV_CONFIG" ] || pk_die "config not found: $PULSEKV_CONFIG"

# ---------------------------------------------------------------------------
# Refuse to double-boot.
# ---------------------------------------------------------------------------
if pk_cluster_running; then
    if [ "$RESTART" -eq 1 ]; then
        pk_step "Stopping the cluster already running"
        "$PULSEKV_DEPLOY_DIR/stop-local-cluster.sh"
    else
        pk_err "a cluster is already running (see $(pk_relpath "$PULSEKV_PID_FILE"))"
        pk_cluster_pids | while IFS=$'\t' read -r label pid addr; do
            pk_pid_alive "$pid" && pk_info "$(printf '%-14s pid %-7s %s' "$label" "$pid" "$addr")"
        done
        pk_die "run deploy/stop-local-cluster.sh first, or pass --restart"
    fi
fi

mkdir -p "$PULSEKV_BIN_DIR" "$PULSEKV_CMAKE_DIR" "$PULSEKV_LOG_DIR"

# ---------------------------------------------------------------------------
# Build.
# ---------------------------------------------------------------------------
build_log() { printf '%s/%s\n' "$PULSEKV_LOG_DIR" "$1"; }

run_logged() {
    # run_logged LOGFILE DESCRIPTION -- CMD...
    local log="$1" desc="$2"; shift 2
    if ! "$@" >"$log" 2>&1; then
        pk_err "$desc failed. Last 40 lines of $(pk_relpath "$log"):"
        tail -n 40 "$log" >&2
        exit 1
    fi
}

if [ "$SKIP_BUILD" -eq 1 ]; then
    pk_step "Skipping build (--skip-build)"
    for bin in "$PULSEKV_CONTROLPLANE_BIN" "$PULSEKV_MEMBER_BIN" "$PULSEKV_SMOKE_BIN" \
               "$PULSEKV_BENCH_BIN" "$PULSEKV_CLUSTER_BENCH_BIN" \
               "$PULSEKV_CHAOS_BIN" "$PULSEKV_EXAMPLE_BIN" "$PULSEKV_NODE_BIN"; do
        [ -x "$bin" ] || pk_die "$(pk_relpath "$bin") is missing; run without --skip-build"
    done
else
    pk_step "Building the Go control plane and tools"
    pk_require go "Install the v2 dev image (deploy/Dockerfile)."
    (
        cd "$PULSEKV_REPO_ROOT/control"
        run_logged "$(build_log go-vet.log)" "go vet" go vet ./...
        run_logged "$(build_log go-build.log)" "go build (controlplane)" \
            go build -o "$PULSEKV_CONTROLPLANE_BIN" ./cmd/controlplane
        run_logged "$(build_log go-build-member.log)" "go build (pulsekv-member)" \
            go build -o "$PULSEKV_MEMBER_BIN" ./cmd/pulsekv-member
        run_logged "$(build_log go-build-smoke.log)" "go build (pulsekv-smoke)" \
            go build -o "$PULSEKV_SMOKE_BIN" ./cmd/pulsekv-smoke
        run_logged "$(build_log go-build-bench.log)" "go build (pulsekv-node-bench)" \
            go build -o "$PULSEKV_BENCH_BIN" ./cmd/pulsekv-node-bench
        run_logged "$(build_log go-build-cluster-bench.log)" "go build (pulsekv-cluster-bench)" \
            go build -o "$PULSEKV_CLUSTER_BENCH_BIN" ./cmd/pulsekv-cluster-bench
        run_logged "$(build_log go-build-chaos.log)" "go build (pulsekv-chaos)" \
            go build -o "$PULSEKV_CHAOS_BIN" ./cmd/pulsekv-chaos
        run_logged "$(build_log go-build-example.log)" "go build (pulsekv-example)" \
            go build -o "$PULSEKV_EXAMPLE_BIN" ./cmd/pulsekv-example
    )
    pk_ok "$(pk_relpath "$PULSEKV_CONTROLPLANE_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_MEMBER_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_SMOKE_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_BENCH_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_CLUSTER_BENCH_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_CHAOS_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_EXAMPLE_BIN")"

    # Validate the config before paying for the C++ build.
    pk_step "Validating $(pk_relpath "$PULSEKV_CONFIG")"
    if ! node_table="$(pk_config_read --print-nodes 2>&1)"; then
        pk_err "config rejected:"
        printf '%s\n' "$node_table" >&2
        exit 1
    fi
    if ! gossip_table="$(pk_config_read --print-gossip 2>&1)"; then
        pk_err "gossip config rejected:"
        printf '%s\n' "$gossip_table" >&2
        exit 1
    fi
    pk_ok "$(printf '%s\n' "$node_table" | grep -c .) node(s) defined"

    pk_step "Building the C++ grpc_shim node (first run takes ~30-60s)"
    pk_require cmake "Install the v2 dev image (deploy/Dockerfile)."
    run_logged "$(build_log cmake-configure.log)" "cmake configure" \
        cmake -S "$PULSEKV_REPO_ROOT/node/grpc_shim" -B "$PULSEKV_CMAKE_DIR" \
              -DCMAKE_BUILD_TYPE=Release
    # Only the node binary: the engine's four test binaries live in the same
    # CMake tree and there is no reason to compile them to boot a cluster.
    # deploy/test-engine.sh builds and runs those.
    run_logged "$(build_log cmake-build.log)" "cmake build" \
        cmake --build "$PULSEKV_CMAKE_DIR" -j "$(nproc 2>/dev/null || echo 4)" \
              --target pulsekv-node
    pk_ok "$(pk_relpath "$PULSEKV_NODE_BIN")"
    grep -E '^-- pulsekv:' "$(build_log cmake-configure.log)" | sed 's/^-- pulsekv: /    /' || true
fi

# ---------------------------------------------------------------------------
# Read the cluster shape through the control plane's own parser.
# ---------------------------------------------------------------------------
CP_ADDRESS="$(pk_controlplane_address)" || pk_die "could not read the control-plane address from $PULSEKV_CONFIG"

node_table="$(pk_config_read --print-nodes)" || pk_die "could not read nodes from $PULSEKV_CONFIG"
[ -n "$node_table" ] || pk_die "config defines no nodes"
mapfile -t NODE_LINES <<< "$node_table"
[ "${#NODE_LINES[@]}" -gt 0 ] || pk_die "config defines no nodes"

engine_line="$(pk_config_read --print-engine)" || pk_die "could not read engine settings from $PULSEKV_CONFIG"
IFS=$'\t' read -r RAM_BUDGET MAX_VALUE _ <<< "$engine_line"
[ -n "$RAM_BUDGET" ] && [ -n "$MAX_VALUE" ] || pk_die "config returned incomplete engine settings"
DATA_ROOT_ABS="$(pk_data_root_abs)" || pk_die "could not resolve engine.data_root from $PULSEKV_CONFIG"
mkdir -p "$DATA_ROOT_ABS"

# ---------------------------------------------------------------------------
# Start everything.
# ---------------------------------------------------------------------------
STARTED_LABELS=(); STARTED_PIDS=(); STARTED_ADDRS=(); STARTED_LOGS=()

: > "$PULSEKV_PID_FILE"

track_last_started() {
    STARTED_LABELS+=("$PK_LAST_LABEL"); STARTED_PIDS+=("$PK_LAST_PID")
    STARTED_ADDRS+=("$PK_LAST_ADDRESS"); STARTED_LOGS+=("$PK_LAST_LOG")
}

# ---------------------------------------------------------------------------
# Wait for health, or fail with something actionable.
# ---------------------------------------------------------------------------
report_failure_and_stop() {
    local i local_state
    pk_err "cluster did not become ready within ${HEALTH_TIMEOUT}s"
    echo >&2
    for i in "${!STARTED_LABELS[@]}"; do
        local_state="running"
        pk_pid_alive "${STARTED_PIDS[$i]}" || local_state="EXITED"
        printf '%s--- %-14s pid %-7s %-22s [%s]%s\n' \
            "$PK_BOLD" "${STARTED_LABELS[$i]}" "${STARTED_PIDS[$i]}" \
            "${STARTED_ADDRS[$i]}" "$local_state" "$PK_RESET" >&2
        if [ -s "${STARTED_LOGS[$i]}" ]; then
            tail -n 15 "${STARTED_LOGS[$i]}" | sed 's/^/    /' >&2
        else
            printf '    (no output in %s)\n' "$(pk_relpath "${STARTED_LOGS[$i]}")" >&2
        fi
        echo >&2
    done
    pk_err "stopping the partially-started cluster"
    "$PULSEKV_DEPLOY_DIR/stop-local-cluster.sh" >&2 || true
    exit 1
}

# Truncate only this cluster's process logs. Targeted restarts append markers
# and retain the failure evidence from earlier incarnations.
: > "$(pk_log_for_label controlplane)"
for line in "${NODE_LINES[@]}"; do
    IFS=$'\t' read -r node_id _ <<< "$line"
    : > "$(pk_log_for_label "data:${node_id}")"
    : > "$(pk_log_for_label "member:${node_id}")"
done

pk_step "Starting control plane and data nodes"
if ! pk_start_controlplane; then report_failure_and_stop; fi
track_last_started

for line in "${NODE_LINES[@]}"; do
    IFS=$'\t' read -r node_id _ <<< "$line"
    if ! pk_start_data_node "$node_id"; then report_failure_and_stop; fi
    track_last_started
done

pk_info "${#STARTED_PIDS[@]} process(es) launched, waiting for direct health checks"
pk_dim "engine: ram-budget=$RAM_BUDGET max-value=$MAX_VALUE data-root=$(pk_relpath "$DATA_ROOT_ABS")"

if ! "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
        --mode=wait --timeout="${HEALTH_TIMEOUT}s" 2>&1 | sed 's/^/    /'; then
    report_failure_and_stop
fi

pk_step "Starting per-node gossip sidecars"
for line in "${NODE_LINES[@]}"; do
    IFS=$'\t' read -r node_id _ <<< "$line"
    if ! pk_start_member "$node_id"; then report_failure_and_stop; fi
    track_last_started
done

EXPECTED_LIVE="$(pk_node_ids_csv)"
pk_info "waiting for gossip membership and shard ownership to converge"
if ! "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
        --mode=topology-wait --expect-live="$EXPECTED_LIVE" \
        --timeout="${HEALTH_TIMEOUT}s" 2>&1 | sed 's/^/    /'; then
    report_failure_and_stop
fi

# `set -o pipefail` makes the above catch the smoke binary's exit code through
# the pipe, but re-verify liveness: a process that died between the last poll
# and now should not be reported as ready.
for i in "${!STARTED_PIDS[@]}"; do
    if ! pk_pid_alive "${STARTED_PIDS[$i]}" ||
       ! pk_pid_matches_label "${STARTED_LABELS[$i]}" "${STARTED_PIDS[$i]}"; then
        report_failure_and_stop
    fi
done

# ---------------------------------------------------------------------------
# Banner.
# ---------------------------------------------------------------------------
echo
printf '%s==> cluster ready%s\n\n' "$PK_BOLD$PK_GREEN" "$PK_RESET"
printf '    %-14s %-8s %-22s %s\n' "PROCESS" "PID" "ADDRESS" "LOG"
printf '    %-14s %-8s %-22s %s\n' "--------------" "--------" "----------------------" "---"
for i in "${!STARTED_LABELS[@]}"; do
    printf '    %-14s %-8s %-22s %s\n' \
        "${STARTED_LABELS[$i]}" "${STARTED_PIDS[$i]}" "${STARTED_ADDRS[$i]}" \
        "$(pk_relpath "${STARTED_LOGS[$i]}")"
done
echo
pk_info "${#STARTED_PIDS[@]} process(es) running; health and gossip topology checks pass."
pk_info "config:    $(pk_relpath "$PULSEKV_CONFIG")"
pk_info "pids:      $(pk_relpath "$PULSEKV_PID_FILE")"
pk_info "spill dirs: $(pk_relpath "$DATA_ROOT_ABS")/<node-id>  (purged at node start and stop)"
echo
pk_info "smoke test:   deploy/smoke-test.sh"
pk_info "engine tests: deploy/test-engine.sh"
pk_info "example:      deploy/build/bin/pulsekv-example --control-plane ${CP_ADDRESS}"
pk_info "cluster bench: deploy/build/bin/pulsekv-cluster-bench --control-plane ${CP_ADDRESS}"
DEMO_NODE_INDEX=0
[ "${#NODE_LINES[@]}" -lt 2 ] || DEMO_NODE_INDEX=1
IFS=$'\t' read -r DEMO_NODE_ID _ <<< "${NODE_LINES[$DEMO_NODE_INDEX]}"
if [ "${#NODE_LINES[@]}" -ge 2 ]; then
    pk_info "chaos test:    deploy/chaos-test.sh --target ${DEMO_NODE_ID} --cycles 3 --seed 7"
else
    pk_info "chaos test:    requires at least two configured nodes"
fi
FIRST_NODE_HOST="$(printf '%s' "${NODE_LINES[0]}" | cut -f2)"
FIRST_NODE_PORT="$(printf '%s' "${NODE_LINES[0]}" | cut -f3)"
pk_info "node bench:   deploy/build/bin/pulsekv-node-bench --address $(pk_join_host_port "$FIRST_NODE_HOST" "$FIRST_NODE_PORT")"
pk_info "node control: deploy/local-node.sh status ${DEMO_NODE_ID}"
pk_info "stop:         deploy/stop-local-cluster.sh"
pk_info "poke by hand: grpcurl -plaintext ${CP_ADDRESS} list"
echo
