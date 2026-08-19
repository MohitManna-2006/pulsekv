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
# complete new state, never a half-written file.
#
# The rename is atomic; the read-modify-write around it is not. This file used
# to say the lifecycle was "intentionally single-writer: chaos-test invokes
# local-node serially", and for chaos-test that is still true -- but soak-test's
# background injector is a second writer by construction (it cycles a data node
# and, every fourth cycle, a control-plane replica), and an injector that
# survives its parent's cleanup is a third. On 2026-08-19 that lost update cost
# an 80-minute total outage: see docs/pulsekv-v2-soak-collapse-analysis.md.
#
# So the assumption is now enforced rather than asserted. Every mutation takes
# the lock below, and deploy/test-lifecycle.sh fails without it.

# pk_registry_lock -- serialise registry mutations across processes.
#
# mkdir is the portable atomic test-and-set: it either creates the directory or
# fails, with no window in between, on every filesystem the dev image and a
# developer's machine are likely to use. flock would be tidier and is not
# portable enough (macOS ships no flock binary).
#
# Re-entrant within one shell, because pk_pid_remove_if legitimately calls
# pk_pid_remove while already inside a critical section.
pk_registry_lock() {
    if [ "${PULSEKV_REGISTRY_LOCK_DEPTH:-0}" -gt 0 ]; then
        PULSEKV_REGISTRY_LOCK_DEPTH=$((PULSEKV_REGISTRY_LOCK_DEPTH + 1))
        return 0
    fi

    local lock="${PULSEKV_PID_FILE}.lock" waited=0 token seen="" token_pid
    local limit=$(( ${PULSEKV_REGISTRY_LOCK_TIMEOUT:-10} * 50 ))
    mkdir -p -- "$(dirname -- "$PULSEKV_PID_FILE")" 2>/dev/null || true

    # The owner file carries a per-ACQUISITION token, not just a pid. Breaking a
    # stale lock has to distinguish "the holder died still holding it" from "the
    # holder released it and someone else took it" -- and a pid alone cannot,
    # because the same shell re-acquiring would look identical. Taking a lock
    # away on that mistake is worse than the wedge it is trying to fix: two
    # writers would then believe they hold it, which is exactly the lost update
    # this lock exists to prevent.
    while ! mkdir -- "$lock" 2>/dev/null; do
        token="$(cat -- "$lock/owner" 2>/dev/null || true)"
        if [ -z "$token" ]; then
            # Held, but the owner has not published itself yet. Never break on
            # this: it is the normal few-microsecond gap inside acquisition.
            seen=""
            sleep 0.02
            waited=$((waited + 1))
        elif [ "$token" != "$seen" ]; then
            # The lock changed hands. Whatever we were about to conclude about
            # the previous holder is void; start observing again.
            seen="$token"
            waited=0
            sleep 0.02
        else
            sleep 0.02
            waited=$((waited + 1))
            token_pid="${token%% *}"
            # Same acquisition throughout, and its owner is gone: safe to break.
            # `waited` guarantees we observed it twice, so the token cannot have
            # been published by an acquisition that has since been released.
            if [ "$waited" -ge 2 ] && ! pk_pid_alive "$token_pid"; then
                pk_warn "clearing a cluster.pids lock left behind by dead pid $token_pid"
                rm -rf -- "$lock" 2>/dev/null || true
                seen=""
                waited=0
            fi
        fi

        if [ "$waited" -ge "$limit" ]; then
            # Absolute backstop. A critical section here is an awk and a rename;
            # anything holding for this long is not going to finish.
            pk_warn "cluster.pids lock held by ${seen:-an unknown process} for ${PULSEKV_REGISTRY_LOCK_TIMEOUT:-10}s; breaking it"
            rm -rf -- "$lock" 2>/dev/null || true
            seen=""
            waited=0
        fi
    done

    # RANDOM alone repeats across forks seeded from the same shell; the pid and
    # a nanosecond stamp make the token unique per acquisition.
    printf '%s %s-%s\n' "$BASHPID" "$(date +%s%N 2>/dev/null || echo 0)" "$RANDOM" \
        >"$lock/owner" 2>/dev/null || true
    PULSEKV_REGISTRY_LOCK_DEPTH=1
}

pk_registry_unlock() {
    if [ "${PULSEKV_REGISTRY_LOCK_DEPTH:-0}" -gt 1 ]; then
        PULSEKV_REGISTRY_LOCK_DEPTH=$((PULSEKV_REGISTRY_LOCK_DEPTH - 1))
        return 0
    fi
    PULSEKV_REGISTRY_LOCK_DEPTH=0
    rm -rf -- "${PULSEKV_PID_FILE}.lock" 2>/dev/null || true
}

pk_pid_record_for() {
    local wanted="$1"
    [ -f "$PULSEKV_PID_FILE" ] || return 1
    awk -F '\t' -v wanted="$wanted" '
        $1 == wanted { print; found = 1; exit }
        END { if (!found) exit 1 }
    ' "$PULSEKV_PID_FILE"
}

pk_pid_set() {
    local label="$1" pid="$2" address="${3:-}" tmp rc=0
    mkdir -p "$PULSEKV_RUN_DIR"
    pk_registry_lock
    if ! tmp="$(mktemp "${PULSEKV_PID_FILE}.tmp.XXXXXX")"; then
        pk_registry_unlock
        return 1
    fi
    if [ -f "$PULSEKV_PID_FILE" ]; then
        if ! awk -F '\t' -v label="$label" '$1 != label' "$PULSEKV_PID_FILE" >"$tmp"; then
            rm -f -- "$tmp"
            pk_registry_unlock
            return 1
        fi
    fi
    if ! printf '%s\t%s\t%s\n' "$label" "$pid" "$address" >>"$tmp" ||
       ! mv -f -- "$tmp" "$PULSEKV_PID_FILE"; then
        rm -f -- "$tmp"
        rc=1
    fi
    pk_registry_unlock
    return "$rc"
}

pk_pid_remove() {
    local label="$1" tmp rc=0
    [ -f "$PULSEKV_PID_FILE" ] || return 0
    pk_registry_lock
    if ! tmp="$(mktemp "${PULSEKV_PID_FILE}.tmp.XXXXXX")"; then
        pk_registry_unlock
        return 1
    fi
    if ! awk -F '\t' -v label="$label" '$1 != label' "$PULSEKV_PID_FILE" >"$tmp" ||
       ! mv -f -- "$tmp" "$PULSEKV_PID_FILE"; then
        rm -f -- "$tmp"
        rc=1
    fi
    pk_registry_unlock
    return "$rc"
}

# Remove LABEL only if it still names PID. This prevents a slow stop path from
# deleting a record installed by a concurrent/retried start.
# Remove LABEL's record only if it still names EXPECTED_PID.
#
# Read and remove are one critical section. Without the lock, a concurrent
# restart could write a new pid between the two and this would delete the new
# record on the strength of having read the old one -- the same lost-update
# shape the lock exists to prevent, just spread across two calls.
pk_pid_remove_if() {
    local label="$1" expected_pid="$2" record current_pid rc=0
    pk_registry_lock
    if record="$(pk_pid_record_for "$label" 2>/dev/null)"; then
        IFS=$'\t' read -r _ current_pid _ <<< "$record"
        if [ "$current_pid" = "$expected_pid" ]; then
            pk_pid_remove "$label" || rc=$?
        fi
    fi
    pk_registry_unlock
    return "$rc"
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

# pk_regex_escape LITERAL -- quote a literal string for pgrep -f.
pk_regex_escape() {
    printf '%s' "$1" | sed 's/[][\.^$*+?(){}|\\]/\\&/g'
}

# pk_process_cmdline PID -- the process's argv, space-joined, or non-zero.
pk_process_cmdline() {
    local pid="$1" cmd
    if [ -r "/proc/${pid}/cmdline" ]; then
        # NUL-separated on Linux; the trailing separator becomes a trailing
        # space, so strip it to match a plain "$*".
        cmd="$(tr '\0' ' ' < "/proc/${pid}/cmdline" 2>/dev/null)" || return 1
        printf '%s\n' "${cmd% }"
        return 0
    fi
    cmd="$(ps -p "$pid" -o command= 2>/dev/null)" || return 1
    [ -n "$cmd" ] || return 1
    printf '%s\n' "$cmd"
}

# pk_pids_for_label LABEL -- every live pid whose command line matches LABEL,
# found by inspecting processes rather than by reading the registry.
#
# The registry is a cache of what this harness started. It is not, and was never
# entitled to be, the definition of what is running. Every question of the form
# "is X alive" needs an answer that survives the registry losing an entry --
# stop-local-cluster.sh has always known this (its orphan sweep works exactly
# this way); the rest of the lifecycle did not, and the 2026-08-19 outage is
# what that cost.
pk_pids_for_label() {
    local label="$1" expected pid
    expected="$(pk_expected_binary_for_label "$label" 2>/dev/null)" || return 0
    [ -n "$expected" ] || return 0
    command -v pgrep >/dev/null 2>&1 || return 0
    while read -r pid; do
        [ -n "$pid" ] || continue
        [ "$pid" = "$$" ] && continue
        pk_pid_alive "$pid" || continue
        # pk_pid_matches_label re-checks argv[0] and --node-id, so a pgrep hit
        # on an unrelated command cannot be mistaken for this label's process.
        pk_pid_matches_label "$label" "$pid" || continue
        printf '%s\n' "$pid"
    done < <(pgrep -f -- "$(pk_regex_escape "$expected")" 2>/dev/null || true)
}

# pk_process_alive_for_label LABEL -- true when a matching process exists,
# whatever the registry happens to say.
pk_process_alive_for_label() {
    local first
    first="$(pk_pids_for_label "$1" | head -1)"
    [ -n "$first" ]
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
    pk_recorded_alive controlplane && return 0

    # Registry said no. Ask the process table before believing it.
    #
    # local-node.sh refuses to start a data node when this returns false, so a
    # false negative here is not a cosmetic wrong answer: it is the difference
    # between a cluster that recovers and one that cannot. On 2026-08-19 all
    # three replicas were serving normally while this function said none was,
    # and no data node was startable again for the remaining 75 minutes.
    while read -r id; do
        [ -n "$id" ] || continue
        pk_process_alive_for_label "controlplane:${id}" && return 0
    done < <(pk_controlplane_ids)
    return 1
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

    # The registry may have lost this label's record while the process kept
    # running (see pk_registry_lock). Adopt it instead of launching a rival: the
    # rival cannot work -- it would die binding a port the survivor holds -- and
    # every attempt overwrote the registry with the doomed process's pid, which
    # is how the 2026-08-19 run convinced itself no control plane was running.
    #
    # Adoption is gated on the survivor running the EXACT command we were about
    # to run. A process started from a different config is not the process this
    # caller asked for, and quietly adopting it would trade a loud port-bind
    # failure for a cluster that silently disagrees with its own config file.
    local survivor wanted running
    wanted="$*"
    while read -r survivor; do
        [ -n "$survivor" ] || continue
        running="$(pk_process_cmdline "$survivor")" || continue
        [ "$running" = "$wanted" ] || continue
        pk_warn "$label is already running as pid $survivor but was missing from the registry; adopting it"
        PK_LAST_LABEL="$label"
        PK_LAST_PID="$survivor"
        PK_LAST_ADDRESS="$address"
        PK_LAST_LOG="$log"
        pk_pid_set "$label" "$survivor" "$address"
        return 0
    done < <(pk_pids_for_label "$label")

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
# are not, by themselves, success: the process table gets the last word.
#
# The old version returned success as soon as the registry had no live record
# for the label. That is a lie whenever the record was lost rather than the
# process stopped, and it is the lie that made the 2026-08-19 outage permanent.
# The caller stopped nothing, started a replacement, and the replacement died on
# `bind: address already in use` because the original was still holding the
# port -- seven times over, to the end of the run.
pk_stop_managed() {
    local label="$1" grace="${2:-10}" pid rc=0
    PK_LAST_STOP_FORCED=0
    pk_signal_managed "$label" TERM || rc=$?
    case "$rc" in
        0)
            pid="$PK_LAST_PID"
            if ! pk_wait_pid_gone "$pid" "$grace"; then
                pk_warn "$label (pid $pid) ignored SIGTERM after ${grace}s, sending SIGKILL"
                PK_LAST_STOP_FORCED=1
                kill -KILL "$pid" 2>/dev/null || true
                pk_wait_pid_gone "$pid" 2 || return 1
            fi
            pk_pid_remove_if "$label" "$pid"
            ;;
        2) : ;;                 # no live record -- the sweep below decides
        *) return "$rc" ;;
    esac
    pk_stop_unrecorded_for_label "$label" "$grace"
}

# pk_stop_unrecorded_for_label LABEL GRACE -- stop any process matching LABEL
# that the registry does not know about.
#
# Normally finds nothing and costs one pgrep. When it does find something, that
# process is by definition one this harness started and then lost track of, and
# leaving it running is what breaks the next start.
pk_stop_unrecorded_for_label() {
    local label="$1" grace="${2:-10}" pid found=0
    while read -r pid; do
        [ -n "$pid" ] || continue
        found=1
        pk_warn "$label: pid $pid is running but was missing from the registry; stopping it"
        kill -TERM "$pid" 2>/dev/null || true
        if ! pk_wait_pid_gone "$pid" "$grace"; then
            PK_LAST_STOP_FORCED=1
            kill -KILL "$pid" 2>/dev/null || true
            pk_wait_pid_gone "$pid" 2 || {
                pk_err "$label: pid $pid survived SIGKILL"
                return 1
            }
        fi
    done < <(pk_pids_for_label "$label")
    [ "$found" -eq 0 ] || pk_pid_remove "$label" || true
    return 0
}

# pk_kill_tree PID [SIGNAL] -- signal PID and every descendant, children first.
#
# `kill $pid` on a backgrounded subshell is not enough. The subshell spends
# almost all its time blocked in `sleep`, and bash defers the signal until that
# child returns; anything the subshell had already launched is not signalled at
# all. soak-test.sh's cleanup did exactly that, which is how injectors from
# earlier runs were still crashing data nodes hours later -- three of them at
# once, by the 2026-08-19 run.
pk_kill_tree() {
    local pid="${1:-}" signal="${2:-TERM}" child
    case "$pid" in (*[!0-9]*|'') return 0 ;; esac
    # Depth first: a parent killed before its children just orphans them.
    if command -v pgrep >/dev/null 2>&1; then
        while read -r child; do
            [ -n "$child" ] || continue
            pk_kill_tree "$child" "$signal"
        done < <(pgrep -P "$pid" 2>/dev/null || true)
    fi
    kill -"$signal" "$pid" 2>/dev/null || true
}

# pk_singleton_acquire NAME -- refuse to run when another instance is live.
#
# The run directory is shared mutable state: one cluster.pids, one set of logs,
# one cluster. Two soaks against it corrupt each other's view of what is
# running, and the only reason that was survivable before is that nobody
# noticed. Fails loudly instead, naming the process to stop.
pk_singleton_acquire() {
    # Declared separately: under `set -u`, referring to `name` in the same
    # `local` statement that introduces it reads an unset variable.
    local name="$1"
    local marker="${PULSEKV_RUN_DIR}/${name}.owner"
    local holder=""
    mkdir -p "$PULSEKV_RUN_DIR"
    if [ -f "$marker" ]; then
        holder="$(cat -- "$marker" 2>/dev/null || true)"
        if [ -n "$holder" ] && pk_pid_alive "$holder"; then
            pk_err "another ${name} is already running (pid $holder) against $(pk_relpath "$PULSEKV_RUN_DIR")."
            pk_err "stop it first, or run with a different run directory. Two of them share one"
            pk_err "cluster.pids and one cluster, and will fight over both."
            return 1
        fi
        pk_dim "clearing a stale ${name} marker from pid ${holder:-unknown}"
    fi
    printf '%s\n' "$$" >"$marker"
}

pk_singleton_release() {
    rm -f -- "${PULSEKV_RUN_DIR}/${1}.owner" 2>/dev/null || true
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
