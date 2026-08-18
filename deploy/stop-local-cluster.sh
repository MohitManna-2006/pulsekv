#!/usr/bin/env bash
#
# Stop everything deploy/run-local-cluster.sh started.
#
#   deploy/stop-local-cluster.sh [--grace SECONDS] [--keep-logs|--clean-logs]
#
# Stops processes in dependency order: chaos watcher, membership sidecars
# (which announce Leave), data nodes, and finally every control-plane replica.
# Each group receives SIGTERM concurrently, gets up to --grace seconds to exit,
# then has a SIGKILL fallback. The control-plane group goes last for the same
# reason it always did -- sidecars need somewhere to announce their Leave -- and
# the existing bounded discipline generalises to N replicas with no new ordering.
#
# Finishes with an orphan sweep: any managed PulseKV process still alive that
# is NOT in the pid file gets found and killed too,
# and reported. A stop script that leaves a process holding port 7100 makes the
# next boot fail for reasons that look nothing like the actual cause.
#
# Exits non-zero only if something could not be stopped.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

GRACE=10
KEEP_LOGS=1

while [ $# -gt 0 ]; do
    case "$1" in
        --grace)    [ $# -ge 2 ] || pk_die "--grace requires seconds"; GRACE="$2"; shift 2 ;;
        --grace=*)  GRACE="${1#*=}"; shift ;;
        --keep-logs) KEEP_LOGS=1; shift ;;
        --clean-logs) KEEP_LOGS=0; shift ;;
        -h|--help)  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)          pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

case "$GRACE" in
    ''|*[!0-9]*) pk_die "--grace must be a non-negative integer, got: $GRACE" ;;
esac
GRACE=$((10#$GRACE))

# Escape a literal string for use as a pgrep -f extended regex.
pk_regex_escape() {
    printf '%s' "$1" | sed 's/[][\.^$*+?(){}|\\]/\\&/g'
}

find_orphans() {
    {
        pgrep -f -- "$(pk_regex_escape "$PULSEKV_CONTROLPLANE_BIN")" || true
        pgrep -f -- "$(pk_regex_escape "$PULSEKV_MEMBER_BIN")" || true
        pgrep -f -- "$(pk_regex_escape "$PULSEKV_NODE_BIN")" || true
        pgrep -f -- "$(pk_regex_escape "$PULSEKV_CHAOS_BIN")" || true
    } | sort -u
}

# ---------------------------------------------------------------------------
# Collect what we are expected to stop.
# ---------------------------------------------------------------------------
LABELS=(); PIDS=(); ADDRS=()
while IFS=$'\t' read -r label pid addr; do
    [ -n "${pid:-}" ] || continue
    LABELS+=("$label"); PIDS+=("$pid"); ADDRS+=("${addr:-}")
done < <(pk_cluster_pids)

known_orphans="$(find_orphans)"

if [ "${#PIDS[@]}" -eq 0 ] && [ -z "$known_orphans" ]; then
    pk_step "Nothing to stop"
    [ -f "$PULSEKV_PID_FILE" ] && rm -f "$PULSEKV_PID_FILE"
    pk_info "no recorded PIDs and no stray pulsekv processes"
    if [ "$KEEP_LOGS" -eq 0 ]; then
        rm -rf "$PULSEKV_LOG_DIR"
        pk_info "removed $(pk_relpath "$PULSEKV_LOG_DIR")"
    fi
    exit 0
fi

# ---------------------------------------------------------------------------
# Ordered graceful stop of recorded processes.
# ---------------------------------------------------------------------------
pk_step "Stopping the cluster"

declare -A STOP_STATE=()
failed=0

select_group() {
    local group="$1" i label
    GROUP_INDICES=()
    for i in "${!LABELS[@]}"; do
        label="${LABELS[$i]}"
        case "$group:$label" in
            chaos:chaos) GROUP_INDICES+=("$i") ;;
            member:member:*) GROUP_INDICES+=("$i") ;;
            data:data:*|data:node-*) GROUP_INDICES+=("$i") ;;
            control:controlplane|control:controlplane:*) GROUP_INDICES+=("$i") ;;
            other:*)
                case "$label" in
                    chaos|member:*|data:*|node-*|controlplane|controlplane:*) ;;
                    *) GROUP_INDICES+=("$i") ;;
                esac
                ;;
        esac
    done
}

stop_group() {
    local group="$1" i label pid deadline
    local active=() still=()
    select_group "$group"
    [ "${#GROUP_INDICES[@]}" -gt 0 ] || return 0

    for i in "${GROUP_INDICES[@]}"; do
        label="${LABELS[$i]}"; pid="${PIDS[$i]}"
        if ! pk_pid_alive "$pid"; then
            STOP_STATE[$i]="was already gone"
            pk_pid_remove_if "$label" "$pid"
            continue
        fi
        if ! pk_pid_matches_label "$label" "$pid"; then
            STOP_STATE[$i]="stale PID record (not signalled)"
            pk_err "$label records pid $pid, but that PID belongs to another command"
            failed=1
            continue
        fi
        if kill -TERM "$pid" 2>/dev/null; then
            active+=("$i")
        elif ! pk_pid_alive "$pid"; then
            STOP_STATE[$i]="stopped during SIGTERM"
            pk_pid_remove_if "$label" "$pid"
        else
            STOP_STATE[$i]="SIGTERM failed"
            failed=1
        fi
    done

    deadline=$((SECONDS + GRACE))
    while [ "${#active[@]}" -gt 0 ]; do
        still=()
        for i in "${active[@]}"; do
            pk_pid_alive "${PIDS[$i]}" && still+=("$i")
        done
        active=("${still[@]}")
        [ "${#active[@]}" -eq 0 ] && break
        [ "$SECONDS" -ge "$deadline" ] && break
        sleep 0.1
    done

    for i in "${active[@]}"; do
        pk_warn "${LABELS[$i]} (pid ${PIDS[$i]}) ignored SIGTERM after ${GRACE}s, sending SIGKILL"
        kill -KILL "${PIDS[$i]}" 2>/dev/null || true
        STOP_STATE[$i]="killed (SIGKILL)"
    done
    [ "${#active[@]}" -eq 0 ] || sleep 0.2

    for i in "${GROUP_INDICES[@]}"; do
        label="${LABELS[$i]}"; pid="${PIDS[$i]}"
        [ -n "${STOP_STATE[$i]:-}" ] || STOP_STATE[$i]="stopped"
        if pk_pid_alive "$pid"; then
            STOP_STATE[$i]="still running"
            failed=1
        else
            pk_pid_remove_if "$label" "$pid"
        fi
    done
}

# Keep the observer alive while sidecars announce Leave. Data services remain
# available until they are no longer advertised.
stop_group chaos
stop_group member
stop_group data
stop_group other
stop_group control

for i in "${!PIDS[@]}"; do
    state="${STOP_STATE[$i]:-not processed}"
    if [ "$state" = "still running" ] || [ "$state" = "SIGTERM failed" ] || \
       [ "$state" = "stale PID record (not signalled)" ]; then
        pk_err "$(printf '%-18s pid %-7s %-22s %s' \
            "${LABELS[$i]}" "${PIDS[$i]}" "${ADDRS[$i]}" "$state")"
    else
        pk_ok "$(printf '%-18s pid %-7s %-22s %s' \
            "${LABELS[$i]}" "${PIDS[$i]}" "${ADDRS[$i]}" "$state")"
    fi
done

# ---------------------------------------------------------------------------
# Orphan sweep -- processes we did not start, or lost track of.
# ---------------------------------------------------------------------------
orphans="$(find_orphans)"
if [ -n "$orphans" ]; then
    pk_warn "found pulsekv process(es) not recorded in $(pk_relpath "$PULSEKV_PID_FILE"):"
    while read -r pid; do
        [ -n "$pid" ] || continue
        cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || echo '?')"
        pk_info "pid $pid  $cmd"
        kill -TERM "$pid" 2>/dev/null || true
    done <<< "$orphans"

    sleep 0.5
    remaining="$(find_orphans)"
    if [ -n "$remaining" ]; then
        while read -r pid; do
            [ -n "$pid" ] || continue
            kill -KILL "$pid" 2>/dev/null || true
        done <<< "$remaining"
        sleep 0.3
    fi

    if [ -n "$(find_orphans)" ]; then
        pk_err "orphaned pulsekv processes survived SIGKILL: $(find_orphans | tr '\n' ' ')"
        failed=1
    else
        pk_ok "orphaned process(es) cleaned up"
    fi
else
    pk_ok "no orphaned pulsekv processes"
fi

rm -f "$PULSEKV_PID_FILE"

if [ "$KEEP_LOGS" -eq 0 ]; then
    rm -rf "$PULSEKV_LOG_DIR"
    pk_info "removed $(pk_relpath "$PULSEKV_LOG_DIR")"
else
    pk_info "logs kept in $(pk_relpath "$PULSEKV_LOG_DIR") (--clean-logs to remove)"
fi

if [ "$failed" -ne 0 ]; then
    pk_die "cluster did not stop cleanly"
fi

pk_step "Cluster stopped"
