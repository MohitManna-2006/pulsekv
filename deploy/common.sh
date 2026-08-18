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

# Phase 4 boot-time overrides, exported so a targeted local-node.sh restart
# rebuilds the same command line the cluster was booted with. Empty means "use
# the config file", which is why the replication override cannot just default to
# 0 -- 0 is a real replication factor, not an absent one.
PULSEKV_REPLICATION_FACTOR="${PULSEKV_REPLICATION_FACTOR:-}"

PULSEKV_CONTROLPLANE_BIN="${PULSEKV_BIN_DIR}/pulsekv-controlplane"
PULSEKV_MEMBER_BIN="${PULSEKV_BIN_DIR}/pulsekv-member"
PULSEKV_SMOKE_BIN="${PULSEKV_BIN_DIR}/pulsekv-smoke"
PULSEKV_BENCH_BIN="${PULSEKV_BIN_DIR}/pulsekv-node-bench"
PULSEKV_CLUSTER_BENCH_BIN="${PULSEKV_BIN_DIR}/pulsekv-cluster-bench"
PULSEKV_CHAOS_BIN="${PULSEKV_BIN_DIR}/pulsekv-chaos"
PULSEKV_METRICS_BIN="${PULSEKV_BIN_DIR}/pulsekv-metrics"
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

pk_pid_alive() {
    local pid="${1:-}"
    case "$pid" in (*[!0-9]*|'') return 1 ;; esac
    kill -0 "$pid" 2>/dev/null || return 1

    # kill -0 also succeeds for an unreaped zombie. Treat it as gone so a
    # crashed node cannot look healthy forever in a long-running container.
    if [ -r "/proc/${pid}/stat" ]; then
        [ "$(awk '{print $3}' "/proc/${pid}/stat" 2>/dev/null)" != "Z" ] || return 1
    fi
    return 0
}

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

# PID records are rewritten through a temporary file in the same directory and
# renamed into place. A reader therefore sees the complete old state or the
# complete new state, never a half-written file. The deploy lifecycle is
# intentionally single-writer: chaos-test invokes local-node serially.
pk_pid_record_for() {
    local wanted="$1"
    [ -f "$PULSEKV_PID_FILE" ] || return 1
    awk -F '\t' -v wanted="$wanted" '
        $1 == wanted { print; found = 1; exit }
        END { if (!found) exit 1 }
    ' "$PULSEKV_PID_FILE"
}

pk_pid_set() {
    local label="$1" pid="$2" address="${3:-}" tmp
    mkdir -p "$PULSEKV_RUN_DIR"
    tmp="$(mktemp "${PULSEKV_PID_FILE}.tmp.XXXXXX")" || return 1
    if [ -f "$PULSEKV_PID_FILE" ]; then
        if ! awk -F '\t' -v label="$label" '$1 != label' "$PULSEKV_PID_FILE" >"$tmp"; then
            rm -f -- "$tmp"
            return 1
        fi
    fi
    if ! printf '%s\t%s\t%s\n' "$label" "$pid" "$address" >>"$tmp" ||
       ! mv -f -- "$tmp" "$PULSEKV_PID_FILE"; then
        rm -f -- "$tmp"
        return 1
    fi
}

pk_pid_remove() {
    local label="$1" tmp
    [ -f "$PULSEKV_PID_FILE" ] || return 0
    tmp="$(mktemp "${PULSEKV_PID_FILE}.tmp.XXXXXX")" || return 1
    if ! awk -F '\t' -v label="$label" '$1 != label' "$PULSEKV_PID_FILE" >"$tmp" ||
       ! mv -f -- "$tmp" "$PULSEKV_PID_FILE"; then
        rm -f -- "$tmp"
        return 1
    fi
}

# Remove LABEL only if it still names PID. This prevents a slow stop path from
# deleting a record installed by a concurrent/retried start.
pk_pid_remove_if() {
    local label="$1" expected_pid="$2" record current_pid
    record="$(pk_pid_record_for "$label" 2>/dev/null)" || return 0
    IFS=$'\t' read -r _ current_pid _ <<< "$record"
    [ "$current_pid" = "$expected_pid" ] || return 0
    pk_pid_remove "$label"
}

pk_expected_binary_for_label() {
    case "$1" in
        controlplane|controlplane:*) printf '%s\n' "$PULSEKV_CONTROLPLANE_BIN" ;;
        data:*)       printf '%s\n' "$PULSEKV_NODE_BIN" ;;
        member:*)     printf '%s\n' "$PULSEKV_MEMBER_BIN" ;;
        chaos)        printf '%s\n' "$PULSEKV_CHAOS_BIN" ;;
        # Backward compatibility for a Phase 2 pid file, whose data labels
        # were bare node IDs. No other unmanaged label is signal-safe.
        *)            printf '%s\n' "$PULSEKV_NODE_BIN" ;;
    esac
}

# Guard signals against stale PID reuse. /proc is available in the Linux dev
# image; ps supplies the same safety check if somebody runs a lifecycle command
# from the host by mistake.
pk_pid_matches_label() {
    local label="$1" pid="$2" expected cmd
    expected="$(pk_expected_binary_for_label "$label" 2>/dev/null)" || return 0
    if [ -r "/proc/${pid}/cmdline" ]; then
        cmd="$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null)" || return 1
    else
        cmd="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
    fi
    # The managed programs are exec'd directly, so the expected binary must be
    # argv[0], not merely a substring of some unrelated command. Pad the
    # command below as well so node-1 cannot match --node-id node-10.
    case "$cmd" in
        "$expected"|"$expected "*) ;;
        *) return 1 ;;
    esac
    cmd=" ${cmd} "
    case "$label" in
        data:*|member:*|controlplane:*)
            # Every managed process carries --node-id, so a stale record for
            # cp-1 cannot signal cp-2 (or node-1 match node-10).
            local node_id="${label#*:}"
            case "$cmd" in
                *" --node-id ${node_id} "*|*" --node-id=${node_id} "*) ;;
                *) return 1 ;;
            esac
            ;;
        controlplane|chaos) ;;
        *)
            local legacy_node_id="$label"
            case "$cmd" in
                *" --node-id ${legacy_node_id} "*|*" --node-id=${legacy_node_id} "*) ;;
                *) return 1 ;;
            esac
            ;;
    esac
    return 0
}

# pk_any_controlplane_alive -- true when at least one replica is running.
#
# "At least one" rather than "all": the metadata group tolerates losing a
# replica, so requiring every one of them would make the lifecycle scripts
# stricter than the system they manage -- and would refuse to work during
# exactly the failover the chaos harness creates.
pk_any_controlplane_alive() {
    local id
    while read -r id; do
        [ -n "$id" ] || continue
        pk_recorded_alive "controlplane:${id}" && return 0
    done < <(pk_controlplane_ids)
    # Pre-Phase-5 pid files used a bare `controlplane` label.
    pk_recorded_alive controlplane
}

pk_recorded_alive() {
    local label="$1" record pid
    record="$(pk_pid_record_for "$label" 2>/dev/null)" || return 1
    IFS=$'\t' read -r _ pid _ <<< "$record"
    pk_pid_alive "$pid" && pk_pid_matches_label "$label" "$pid"
}

# pk_cluster_running -- true if the pid file names at least one live process.
pk_cluster_running() {
    local label pid _addr
    while IFS=$'\t' read -r label pid _addr; do
        [ -n "${pid:-}" ] || continue
        if pk_pid_alive "$pid" && pk_pid_matches_label "$label" "$pid"; then return 0; fi
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

pk_node_line() {
    local wanted="$1" table
    table="$(pk_config_read --print-nodes)" || return 1
    awk -F '\t' -v wanted="$wanted" '
        $1 == wanted { print; found = 1; exit }
        END { if (!found) exit 1 }
    ' <<< "$table"
}

pk_gossip_line() {
    local wanted="$1" table
    table="$(pk_config_read --print-gossip)" || return 1
    awk -F '\t' -v wanted="$wanted" '
        NR > 1 && $1 == wanted { print; found = 1; exit }
        END { if (!found) exit 1 }
    ' <<< "$table"
}

pk_node_ids_csv() {
    local omit="${1:-}" id out="" table
    table="$(pk_config_read --print-nodes)" || return 1
    while IFS=$'\t' read -r id _; do
        [ -n "$id" ] || continue
        [ -n "$omit" ] && [ "$id" = "$omit" ] && continue
        if [ -n "$out" ]; then out="${out},${id}"; else out="$id"; fi
    done <<< "$table"
    printf '%s\n' "$out"
}

pk_join_host_port() {
    local host="$1" port="$2"
    case "$host" in
        \[*\]) printf '%s:%s\n' "$host" "$port" ;;
        *:*)   printf '[%s]:%s\n' "$host" "$port" ;;
        *)     printf '%s:%s\n' "$host" "$port" ;;
    esac
}

# pk_controlplane_ids -- one control-plane replica ID per line, in config order.
pk_controlplane_ids() {
    local table
    table="$(pk_config_read --print-control-plane)" || return 1
    printf '%s\n' "$table" | cut -f1
}

# pk_controlplane_line ID -- `node_id<TAB>host<TAB>port` for one replica.
pk_controlplane_line() {
    local wanted="$1" table
    table="$(pk_config_read --print-control-plane)" || return 1
    awk -F '\t' -v wanted="$wanted" '
        $1 == wanted { print; found = 1; exit }
        END { if (!found) exit 1 }
    ' <<< "$table"
}

# pk_controlplane_address [ID] -- one replica's gRPC address; the first replica
# when no ID is given. Callers that can talk to any replica should prefer
# pk_controlplane_endpoints, which does not care which one is up.
pk_controlplane_address() {
    local wanted="${1:-}" id host port line table
    if [ -n "$wanted" ]; then
        line="$(pk_controlplane_line "$wanted")" || return 1
    else
        table="$(pk_config_read --print-control-plane)" || return 1
        line="$(printf '%s\n' "$table" | head -1)"
    fi
    IFS=$'\t' read -r id host port <<< "$line"
    [ -n "$host" ] && [ -n "$port" ] || return 1
    pk_join_host_port "$host" "$port"
}

# pk_controlplane_endpoints -- every replica's address as one comma-separated
# string. This is what clients, data nodes, and tools are given: Phase 5 made
# the control plane a Raft group, so pinning anyone to a single replica would
# make that replica a single point of failure the group does not have.
pk_controlplane_endpoints() {
    local id host port out="" table address
    table="$(pk_config_read --print-control-plane)" || return 1
    while IFS=$'\t' read -r id host port; do
        [ -n "$id" ] || continue
        address="$(pk_join_host_port "$host" "$port")"
        if [ -n "$out" ]; then out="${out},${address}"; else out="$address"; fi
    done <<< "$table"
    [ -n "$out" ] || return 1
    printf '%s\n' "$out"
}

# pk_raft_data_root_abs -- absolute path of raft.data_dir, resolved by the
# control plane's own parser rather than re-derived in shell.
pk_raft_data_root_abs() {
    pk_config_read --print-raft | awk -F '\t' '$1 == "#" { print $2; found = 1 }
        END { if (!found) exit 1 }'
}

pk_log_for_label() {
    local safe="${1//[:\/]/-}"
    printf '%s/%s.log\n' "$PULSEKV_LOG_DIR" "$safe"
}

# pk_start_managed LABEL ADDRESS LOG -- COMMAND...
# Sets PK_LAST_{LABEL,PID,ADDRESS,LOG}. The caller decides whether to truncate
# LOG first; targeted restarts append so earlier crash evidence is retained.
pk_start_managed() {
    local label="$1" address="$2" log="$3" record old_pid
    shift 3

    if record="$(pk_pid_record_for "$label" 2>/dev/null)"; then
        IFS=$'\t' read -r _ old_pid _ <<< "$record"
        if pk_pid_alive "$old_pid" && pk_pid_matches_label "$label" "$old_pid"; then
            pk_err "$label is already running (pid $old_pid)"
            return 1
        fi
        pk_pid_remove "$label"
    fi

    mkdir -p "$PULSEKV_LOG_DIR"
    printf '\n=== %s start %s ===\n' "$label" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$log"
    "$@" >>"$log" 2>&1 &
    PK_LAST_LABEL="$label"
    PK_LAST_PID=$!
    PK_LAST_ADDRESS="$address"
    PK_LAST_LOG="$log"
    if ! pk_pid_set "$label" "$PK_LAST_PID" "$address"; then
        pk_err "could not record $label pid $PK_LAST_PID"
        kill -TERM "$PK_LAST_PID" 2>/dev/null || true
        if ! pk_wait_pid_gone "$PK_LAST_PID" 2; then
            kill -KILL "$PK_LAST_PID" 2>/dev/null || true
            pk_wait_pid_gone "$PK_LAST_PID" 2 || true
        fi
        return 1
    fi
}

pk_data_root_abs() {
    local _ram _max root config_dir line
    line="$(pk_config_read --print-engine)" || return 1
    IFS=$'\t' read -r _ram _max root <<< "$line"
    [ -n "$root" ] || return 1
    case "$root" in
        /*) printf '%s\n' "$root" ;;
        *)
            config_dir="$(cd -- "$(dirname -- "$PULSEKV_CONFIG")" && pwd)" || return 1
            printf '%s/%s\n' "$config_dir" "$root"
            ;;
    esac
}

# The effective replication factor: the boot-time override if one was given,
# otherwise whatever the config says, read through the control plane's own
# parser so the shell can never disagree with the server about it.
pk_replication_factor() {
    if [ -n "$PULSEKV_REPLICATION_FACTOR" ]; then
        printf '%s\n' "$PULSEKV_REPLICATION_FACTOR"
        return 0
    fi
    pk_config_read --print-replication
}

# pk_start_controlplane ID -- start one control-plane replica.
#
# Each replica is a separate managed process labelled `controlplane:<id>`, so
# the chaos harness can kill exactly the current Raft leader and the PID-identity
# guard can tell cp-0 from cp-1.
pk_start_controlplane() {
    local node_id="$1" address log
    address="$(pk_controlplane_address "$node_id")" || {
        pk_err "unknown configured control-plane replica: $node_id"
        return 1
    }
    log="$(pk_log_for_label "controlplane:${node_id}")"
    if [ -n "$PULSEKV_REPLICATION_FACTOR" ]; then
        pk_start_managed "controlplane:${node_id}" "$address" "$log" \
            "$PULSEKV_CONTROLPLANE_BIN" --config "$PULSEKV_CONFIG" \
            --node-id "$node_id" \
            --replication-factor "$PULSEKV_REPLICATION_FACTOR"
    else
        pk_start_managed "controlplane:${node_id}" "$address" "$log" \
            "$PULSEKV_CONTROLPLANE_BIN" --config "$PULSEKV_CONFIG" \
            --node-id "$node_id"
    fi
}

# Data nodes are handed the FULL control-plane endpoint list as --metadata-addr
# so they can read their own replica peers from whichever replica is up. They
# are NOT handed a replication factor: placement is the control plane's
# decision, and a node carrying its own copy of the number could disagree with
# the map it is being sent.
pk_start_data_node() {
    local node_id="$1" line host port ram_budget max_value _root data_root address log engine_line cp_address
    line="$(pk_node_line "$node_id")" || {
        pk_err "unknown configured node: $node_id"
        return 1
    }
    IFS=$'\t' read -r _ host port <<< "$line"
    engine_line="$(pk_config_read --print-engine)" || return 1
    IFS=$'\t' read -r ram_budget max_value _root <<< "$engine_line"
    [ -n "$ram_budget" ] && [ -n "$max_value" ] || return 1
    data_root="$(pk_data_root_abs)" || return 1
    cp_address="$(pk_controlplane_endpoints)" || return 1
    mkdir -p "${data_root}/${node_id}"
    address="$(pk_join_host_port "$host" "$port")"
    log="$(pk_log_for_label "data:${node_id}")"
    pk_start_managed "data:${node_id}" "$address" "$log" \
        "$PULSEKV_NODE_BIN" --node-id "$node_id" --host "$host" --port "$port" \
        --data-dir "${data_root}/${node_id}" \
        --ram-budget-bytes "$ram_budget" \
        --max-value-bytes "$max_value" \
        --metadata-addr "$cp_address"
}

pk_start_member() {
    local node_id="$1" line host port address log
    line="$(pk_gossip_line "$node_id")" || {
        pk_err "no gossip endpoint for configured node: $node_id"
        return 1
    }
    IFS=$'\t' read -r _ host port <<< "$line"
    address="$(pk_join_host_port "$host" "$port")"
    log="$(pk_log_for_label "member:${node_id}")"
    pk_start_managed "member:${node_id}" "$address" "$log" \
        "$PULSEKV_MEMBER_BIN" --config "$PULSEKV_CONFIG" --node-id "$node_id"
}

# Send SIGNAL to the exact process currently recorded for LABEL. Sets
# PK_LAST_PID. Return 2 if it is already gone, 3 if the PID belongs to a
# different command, and otherwise propagate kill's status.
pk_signal_managed() {
    local label="$1" signal="$2" record pid
    record="$(pk_pid_record_for "$label" 2>/dev/null)" || return 2
    IFS=$'\t' read -r _ pid _ <<< "$record"
    PK_LAST_PID="$pid"
    if ! pk_pid_alive "$pid"; then
        pk_pid_remove_if "$label" "$pid"
        return 2
    fi
    if ! pk_pid_matches_label "$label" "$pid"; then
        pk_err "refusing to signal stale $label record: pid $pid belongs to another command"
        return 3
    fi
    kill -"$signal" "$pid"
}

pk_wait_pid_gone() {
    local pid="$1" timeout="$2" deadline
    deadline=$((SECONDS + timeout))
    while pk_pid_alive "$pid"; do
        [ "$SECONDS" -ge "$deadline" ] && return 1
        sleep 0.1
    done
    return 0
}

# Graceful stop with a bounded hard-kill fallback. Missing/already-gone records
# are success because cleanup and failure recovery must be idempotent.
pk_stop_managed() {
    local label="$1" grace="${2:-10}" pid rc=0
    PK_LAST_STOP_FORCED=0
    pk_signal_managed "$label" TERM || rc=$?
    case "$rc" in
        0) pid="$PK_LAST_PID" ;;
        2) return 0 ;;
        *) return "$rc" ;;
    esac
    if ! pk_wait_pid_gone "$pid" "$grace"; then
        pk_warn "$label (pid $pid) ignored SIGTERM after ${grace}s, sending SIGKILL"
        PK_LAST_STOP_FORCED=1
        kill -KILL "$pid" 2>/dev/null || true
        pk_wait_pid_gone "$pid" 2 || return 1
    fi
    pk_pid_remove_if "$label" "$pid"
}

pk_tail_process_log() {
    local label="$1" lines="${2:-20}" log
    log="$(pk_log_for_label "$label")"
    if [ -s "$log" ]; then
        tail -n "$lines" "$log" | sed 's/^/    /' >&2
    else
        pk_info "no output in $(pk_relpath "$log")"
    fi
}
