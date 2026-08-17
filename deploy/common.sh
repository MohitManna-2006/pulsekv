#!/usr/bin/env bash
# Shared paths and helpers for the deploy/ scripts. Sourced, never executed.
#
# Every script here resolves its own location, so they all work whether you run
# them as `deploy/run-local-cluster.sh` from the repo root or `./run-local-cluster.sh`
# from inside deploy/.

# shellcheck shell=bash

PULSEKV_DEPLOY_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PULSEKV_REPO_ROOT="$(cd -- "${PULSEKV_DEPLOY_DIR}/.." && pwd)"

# Build output. Under deploy/ rather than the repo-root build/ on purpose: v1's
# `make clean` does `rm -rf build/`, and v2 artefacts vanishing when someone
# cleans v1 would be a confusing afternoon.
PULSEKV_BUILD_DIR="${PULSEKV_DEPLOY_DIR}/build"
PULSEKV_BIN_DIR="${PULSEKV_BUILD_DIR}/bin"
PULSEKV_CMAKE_DIR="${PULSEKV_BUILD_DIR}/cmake"

# Runtime state: PIDs and per-process logs.
PULSEKV_RUN_DIR="${PULSEKV_DEPLOY_DIR}/run"
PULSEKV_LOG_DIR="${PULSEKV_RUN_DIR}/logs"
PULSEKV_PID_FILE="${PULSEKV_RUN_DIR}/cluster.pids"

PULSEKV_CONFIG="${PULSEKV_CONFIG:-${PULSEKV_DEPLOY_DIR}/cluster.config.yaml}"

PULSEKV_CONTROLPLANE_BIN="${PULSEKV_BIN_DIR}/pulsekv-controlplane"
PULSEKV_SMOKE_BIN="${PULSEKV_BIN_DIR}/pulsekv-smoke"
PULSEKV_BENCH_BIN="${PULSEKV_BIN_DIR}/pulsekv-node-bench"
PULSEKV_CLUSTER_BENCH_BIN="${PULSEKV_BIN_DIR}/pulsekv-cluster-bench"
PULSEKV_EXAMPLE_BIN="${PULSEKV_BIN_DIR}/pulsekv-example"
PULSEKV_NODE_BIN="${PULSEKV_CMAKE_DIR}/pulsekv-node"

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
    PK_BOLD=$'\033[1m'; PK_RED=$'\033[31m'; PK_GREEN=$'\033[32m'
    PK_YELLOW=$'\033[33m'; PK_DIM=$'\033[2m'; PK_RESET=$'\033[0m'
else
    PK_BOLD=''; PK_RED=''; PK_GREEN=''; PK_YELLOW=''; PK_DIM=''; PK_RESET=''
fi

pk_step() { printf '%s==>%s %s\n' "$PK_BOLD" "$PK_RESET" "$*"; }
pk_info() { printf '    %s\n' "$*"; }
pk_dim()  { printf '    %s%s%s\n' "$PK_DIM" "$*" "$PK_RESET"; }
pk_ok()   { printf '    %sok%s   %s\n' "$PK_GREEN" "$PK_RESET" "$*"; }
pk_warn() { printf '%swarn:%s %s\n' "$PK_YELLOW" "$PK_RESET" "$*" >&2; }
pk_err()  { printf '%serror:%s %s\n' "$PK_RED" "$PK_RESET" "$*" >&2; }
pk_die()  { pk_err "$*"; exit 1; }

# pk_require CMD MESSAGE -- fail with an actionable message, not "command not found".
pk_require() {
    command -v "$1" >/dev/null 2>&1 || pk_die "$1 not found. $2"
}

pk_pid_alive() { kill -0 "$1" 2>/dev/null; }

# pk_relpath ABSOLUTE -- shorten to repo-relative for readable output.
pk_relpath() {
    case "$1" in
        "${PULSEKV_REPO_ROOT}/"*) printf '%s\n' "${1#"${PULSEKV_REPO_ROOT}/"}" ;;
        *) printf '%s\n' "$1" ;;
    esac
}

# pk_cluster_pids -- emit `label<TAB>pid<TAB>address` for every recorded process.
pk_cluster_pids() {
    [ -f "$PULSEKV_PID_FILE" ] || return 0
    grep -v '^[[:space:]]*$' "$PULSEKV_PID_FILE" || true
}

# pk_cluster_running -- true if the pid file names at least one live process.
pk_cluster_running() {
    local _label pid _addr
    while IFS=$'\t' read -r _label pid _addr; do
        [ -n "${pid:-}" ] || continue
        if pk_pid_alive "$pid"; then return 0; fi
    done < <(pk_cluster_pids)
    return 1
}

# pk_config_value --print-nodes | --print-control-plane
# Reads the cluster config through the control-plane binary's own parser, so
# the scripts and the server can never disagree about what the file says.
pk_config_read() {
    [ -x "$PULSEKV_CONTROLPLANE_BIN" ] || \
        pk_die "$(pk_relpath "$PULSEKV_CONTROLPLANE_BIN") not built yet; run deploy/run-local-cluster.sh"
    "$PULSEKV_CONTROLPLANE_BIN" --config "$PULSEKV_CONFIG" "$1"
}
