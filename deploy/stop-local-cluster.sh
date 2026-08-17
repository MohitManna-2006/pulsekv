#!/usr/bin/env bash
#
# Stop everything deploy/run-local-cluster.sh started.
#
#   deploy/stop-local-cluster.sh [--grace SECONDS] [--keep-logs]
#
# Sends SIGTERM to every PID in deploy/run/cluster.pids, waits up to --grace
# seconds (default 10) for a clean exit, then SIGKILLs whatever is left. Both
# servers handle SIGTERM properly -- the Go control plane calls GracefulStop
# and the C++ shim shuts the gRPC server down through a self-pipe -- so the
# SIGKILL path should never fire in practice.
#
# Finishes with an orphan sweep: any pulsekv-controlplane or pulsekv-node
# process still alive that is NOT in the pid file gets found and killed too,
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
        --grace)    GRACE="$2"; shift 2 ;;
        --grace=*)  GRACE="${1#*=}"; shift ;;
        --keep-logs) KEEP_LOGS=1; shift ;;
        --clean-logs) KEEP_LOGS=0; shift ;;
        -h|--help)  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)          pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

# Escape a literal string for use as a pgrep -f extended regex.
pk_regex_escape() {
    printf '%s' "$1" | sed 's/[][\.^$*+?(){}|\\]/\\&/g'
}

find_orphans() {
    {
        pgrep -f -- "$(pk_regex_escape "$PULSEKV_CONTROLPLANE_BIN")" || true
        pgrep -f -- "$(pk_regex_escape "$PULSEKV_NODE_BIN")" || true
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
    exit 0
fi

# ---------------------------------------------------------------------------
# Graceful stop of the recorded processes.
# ---------------------------------------------------------------------------
pk_step "Stopping the cluster"

terminated=(); already_gone=()
for i in "${!PIDS[@]}"; do
    if pk_pid_alive "${PIDS[$i]}"; then
        kill -TERM "${PIDS[$i]}" 2>/dev/null || true
        terminated+=("$i")
    else
        already_gone+=("$i")
    fi
done

# Poll rather than sleep for the full grace period: a healthy cluster stops in
# well under a second and there is no reason to make the common case slow.
deadline=$(( $(date +%s) + GRACE ))
while [ "${#terminated[@]}" -gt 0 ]; do
    still_running=()
    for i in ${terminated[@]+"${terminated[@]}"}; do
        pk_pid_alive "${PIDS[$i]}" && still_running+=("$i")
    done
    terminated=(${still_running[@]+"${still_running[@]}"})
    [ "${#terminated[@]}" -eq 0 ] && break
    [ "$(date +%s)" -ge "$deadline" ] && break
    sleep 0.2
done

killed_hard=()
for i in ${terminated[@]+"${terminated[@]}"}; do
    pk_warn "${LABELS[$i]} (pid ${PIDS[$i]}) ignored SIGTERM after ${GRACE}s, sending SIGKILL"
    kill -KILL "${PIDS[$i]}" 2>/dev/null || true
    killed_hard+=("$i")
done
[ "${#killed_hard[@]}" -gt 0 ] && sleep 0.3

# ---------------------------------------------------------------------------
# Report.
# ---------------------------------------------------------------------------
failed=0
for i in "${!PIDS[@]}"; do
    if pk_pid_alive "${PIDS[$i]}"; then
        pk_err "$(printf '%-14s pid %-7s %-22s still running' \
            "${LABELS[$i]}" "${PIDS[$i]}" "${ADDRS[$i]}")"
        failed=1
        continue
    fi
    state="stopped"
    for g in ${already_gone[@]+"${already_gone[@]}"}; do
        [ "$g" = "$i" ] && state="was already gone"
    done
    for k in ${killed_hard[@]+"${killed_hard[@]}"}; do
        [ "$k" = "$i" ] && state="killed (SIGKILL)"
    done
    pk_ok "$(printf '%-14s pid %-7s %-22s %s' \
        "${LABELS[$i]}" "${PIDS[$i]}" "${ADDRS[$i]}" "$state")"
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
