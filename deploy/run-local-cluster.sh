#!/usr/bin/env bash
#
# Boot the PulseKV v2 local multi-process dev cluster.
#
#   deploy/run-local-cluster.sh [--config PATH] [--skip-build] [--restart]
#                               [--timeout SECONDS]
#
# Builds the Go control plane and the C++ grpc_shim node binary, starts one
# control-plane process plus one node process per entry in
# deploy/cluster.config.yaml, and polls every process's HealthCheck until all
# report ok -- or fails loudly naming exactly which ones did not come up.
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
        --config)     PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)   PULSEKV_CONFIG="${1#*=}"; shift ;;
        --timeout)    HEALTH_TIMEOUT="$2"; shift 2 ;;
        --timeout=*)  HEALTH_TIMEOUT="${1#*=}"; shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        --restart)    RESTART=1; shift ;;
        -h|--help)    sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)            pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

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
    for bin in "$PULSEKV_CONTROLPLANE_BIN" "$PULSEKV_SMOKE_BIN" "$PULSEKV_NODE_BIN"; do
        [ -x "$bin" ] || pk_die "$(pk_relpath "$bin") is missing; run without --skip-build"
    done
else
    pk_step "Building the Go control plane"
    pk_require go "Install the v2 dev image (deploy/Dockerfile)."
    (
        cd "$PULSEKV_REPO_ROOT/control"
        run_logged "$(build_log go-vet.log)" "go vet" go vet ./...
        run_logged "$(build_log go-build.log)" "go build (controlplane)" \
            go build -o "$PULSEKV_CONTROLPLANE_BIN" ./cmd/controlplane
        run_logged "$(build_log go-build-smoke.log)" "go build (pulsekv-smoke)" \
            go build -o "$PULSEKV_SMOKE_BIN" ./cmd/pulsekv-smoke
    )
    pk_ok "$(pk_relpath "$PULSEKV_CONTROLPLANE_BIN")"
    pk_ok "$(pk_relpath "$PULSEKV_SMOKE_BIN")"

    # Validate the config before paying for the C++ build.
    pk_step "Validating $(pk_relpath "$PULSEKV_CONFIG")"
    if ! node_table="$(pk_config_read --print-nodes 2>&1)"; then
        pk_err "config rejected:"
        printf '%s\n' "$node_table" >&2
        exit 1
    fi
    pk_ok "$(printf '%s\n' "$node_table" | grep -c .) node(s) defined"

    pk_step "Building the C++ grpc_shim node (first run takes ~30-60s)"
    pk_require cmake "Install the v2 dev image (deploy/Dockerfile)."
    run_logged "$(build_log cmake-configure.log)" "cmake configure" \
        cmake -S "$PULSEKV_REPO_ROOT/node/grpc_shim" -B "$PULSEKV_CMAKE_DIR" \
              -DCMAKE_BUILD_TYPE=Release
    run_logged "$(build_log cmake-build.log)" "cmake build" \
        cmake --build "$PULSEKV_CMAKE_DIR" -j "$(nproc 2>/dev/null || echo 4)"
    pk_ok "$(pk_relpath "$PULSEKV_NODE_BIN")"
    grep -E '^-- pulsekv:' "$(build_log cmake-configure.log)" | sed 's/^-- pulsekv: /    /' || true
fi

# ---------------------------------------------------------------------------
# Read the cluster shape through the control plane's own parser.
# ---------------------------------------------------------------------------
IFS=$'\t' read -r CP_HOST CP_PORT < <(pk_config_read --print-control-plane)
CP_ADDRESS="${CP_HOST}:${CP_PORT}"

mapfile -t NODE_LINES < <(pk_config_read --print-nodes)
[ "${#NODE_LINES[@]}" -gt 0 ] || pk_die "config defines no nodes"

# ---------------------------------------------------------------------------
# Start everything.
# ---------------------------------------------------------------------------
STARTED_LABELS=(); STARTED_PIDS=(); STARTED_ADDRS=(); STARTED_LOGS=()

: > "$PULSEKV_PID_FILE"

start_process() {
    local label="$1" address="$2"; shift 2
    local log="${PULSEKV_LOG_DIR}/${label}.log"
    : > "$log"
    "$@" >>"$log" 2>&1 &
    local pid=$!
    printf '%s\t%s\t%s\n' "$label" "$pid" "$address" >> "$PULSEKV_PID_FILE"
    STARTED_LABELS+=("$label"); STARTED_PIDS+=("$pid")
    STARTED_ADDRS+=("$address"); STARTED_LOGS+=("$log")
}

pk_step "Starting the cluster"

start_process "controlplane" "$CP_ADDRESS" \
    "$PULSEKV_CONTROLPLANE_BIN" --config "$PULSEKV_CONFIG"

for line in "${NODE_LINES[@]}"; do
    IFS=$'\t' read -r node_id node_host node_port <<< "$line"
    start_process "$node_id" "${node_host}:${node_port}" \
        "$PULSEKV_NODE_BIN" --node-id "$node_id" --host "$node_host" --port "$node_port"
done

pk_info "$(printf '%s process(es) launched, waiting for health checks' "${#STARTED_PIDS[@]}")"

# ---------------------------------------------------------------------------
# Wait for health, or fail with something actionable.
# ---------------------------------------------------------------------------
report_failure_and_stop() {
    pk_err "cluster did not become healthy within ${HEALTH_TIMEOUT}s"
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

if ! "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
        --mode=wait --timeout="${HEALTH_TIMEOUT}s" 2>&1 | sed 's/^/    /'; then
    report_failure_and_stop
fi

# `set -o pipefail` makes the above catch the smoke binary's exit code through
# the pipe, but re-verify liveness: a process that died between the last poll
# and now should not be reported as ready.
for i in "${!STARTED_PIDS[@]}"; do
    pk_pid_alive "${STARTED_PIDS[$i]}" || report_failure_and_stop
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
pk_info "${#STARTED_PIDS[@]} process(es) running, all health checks passing."
pk_info "config:  $(pk_relpath "$PULSEKV_CONFIG")"
pk_info "pids:    $(pk_relpath "$PULSEKV_PID_FILE")"
echo
pk_info "smoke test:  deploy/smoke-test.sh"
pk_info "stop:        deploy/stop-local-cluster.sh"
pk_info "poke by hand: grpcurl -plaintext ${CP_ADDRESS} list"
echo
