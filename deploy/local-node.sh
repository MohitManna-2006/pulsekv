#!/usr/bin/env bash
#
# Targeted lifecycle control for one node in the local Phase 3 cluster.
#
#   deploy/local-node.sh [--config PATH] [--timeout SECONDS] COMMAND NODE_ID
#
# Commands:
#   status      show recorded data/sidecar process state
#   leave       gossip Leave, wait for removal, then stop the data process
#   crash       SIGKILL data + sidecar; SWIM must detect the failed member
#   node-crash  SIGKILL only data; watchdog withdraws it and waits for recovery
#   start       data health first, then sidecar join, then topology convergence
#   restart     graceful leave followed by start
#
# This script deliberately permits only configured node IDs. It never accepts
# an arbitrary PID and never targets the control plane.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

TIMEOUT=15

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --config)    [ $# -ge 2 ] || pk_die "--config requires a path"; PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)  PULSEKV_CONFIG="${1#*=}"; shift ;;
        --timeout)   [ $# -ge 2 ] || pk_die "--timeout requires seconds"; TIMEOUT="$2"; shift 2 ;;
        --timeout=*) TIMEOUT="${1#*=}"; shift ;;
        -h|--help)   usage; exit 0 ;;
        --)          shift; break ;;
        -*)          pk_die "unknown argument: $1 (try --help)" ;;
        *)           break ;;
    esac
done

[ $# -eq 2 ] || { usage >&2; pk_die "expected COMMAND and NODE_ID"; }
ACTION="$1"
NODE_ID="$2"

case "$TIMEOUT" in
    ''|*[!0-9]*) pk_die "--timeout must be a positive integer, got: $TIMEOUT" ;;
esac
TIMEOUT=$((10#$TIMEOUT))
[ "$TIMEOUT" -gt 0 ] || pk_die "--timeout must be a positive integer, got: $TIMEOUT"
case "$ACTION" in
    status|leave|crash|node-crash|start|restart) ;;
    *) pk_die "unknown command: $ACTION (try --help)" ;;
esac

[ -f "$PULSEKV_CONFIG" ] || pk_die "config not found: $PULSEKV_CONFIG"
[ -x "$PULSEKV_CONTROLPLANE_BIN" ] || pk_die "$(pk_relpath "$PULSEKV_CONTROLPLANE_BIN") is missing"
pk_node_line "$NODE_ID" >/dev/null 2>&1 || pk_die "unknown configured node: $NODE_ID"

DATA_LABEL="data:${NODE_ID}"
MEMBER_LABEL="member:${NODE_ID}"
ALL_LIVE="$(pk_node_ids_csv)"
WITHOUT_TARGET="$(pk_node_ids_csv "$NODE_ID")"

wait_direct_health() {
    "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
        --mode=wait --timeout="${TIMEOUT}s" 2>&1 | sed 's/^/    /'
}

wait_topology() {
    local expected="$1"
    "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
        --mode=topology-wait --expect-live="$expected" \
        --timeout="${TIMEOUT}s" 2>&1 | sed 's/^/    /'
}

require_cluster_runtime() {
    [ -x "$PULSEKV_NODE_BIN" ] || pk_die "$(pk_relpath "$PULSEKV_NODE_BIN") is missing"
    [ -x "$PULSEKV_MEMBER_BIN" ] || pk_die "$(pk_relpath "$PULSEKV_MEMBER_BIN") is missing"
    [ -x "$PULSEKV_SMOKE_BIN" ] || pk_die "$(pk_relpath "$PULSEKV_SMOKE_BIN") is missing"
    pk_recorded_alive controlplane || pk_die "the local control plane is not running"
}

require_pair_alive() {
    pk_recorded_alive "$DATA_LABEL" || pk_die "$DATA_LABEL is not running"
    pk_recorded_alive "$MEMBER_LABEL" || pk_die "$MEMBER_LABEL is not running"
}

print_record() {
    local label="$1" record pid address state
    if ! record="$(pk_pid_record_for "$label" 2>/dev/null)"; then
        printf '    %-18s %-8s %s\n' "$label" "-" "not recorded"
        return 1
    fi
    IFS=$'\t' read -r _ pid address <<< "$record"
    state="stopped"
    if pk_pid_alive "$pid" && pk_pid_matches_label "$label" "$pid"; then
        state="running"
    elif pk_pid_alive "$pid"; then
        state="STALE PID"
    fi
    printf '    %-18s %-8s %-22s %s\n' "$label" "$pid" "$address" "$state"
    [ "$state" = "running" ]
}

rollback_started_pair() {
    local started_data="$1" started_member="$2"
    if [ "$started_member" -eq 1 ]; then
        pk_stop_managed "$MEMBER_LABEL" 2 || true
    fi
    if [ "$started_data" -eq 1 ]; then
        pk_stop_managed "$DATA_LABEL" 2 || true
    fi
}

start_pair() {
    require_cluster_runtime
    local data_alive=0 member_alive=0 started_data=0 started_member=0
    pk_recorded_alive "$DATA_LABEL" && data_alive=1
    pk_recorded_alive "$MEMBER_LABEL" && member_alive=1
    if [ "$data_alive" -eq 1 ] && [ "$member_alive" -eq 1 ]; then
        pk_die "$NODE_ID is already running; use status or restart"
    fi

    if [ "$data_alive" -eq 0 ]; then
        pk_step "Starting $NODE_ID data service"
        if ! pk_start_data_node "$NODE_ID"; then
            pk_tail_process_log "$DATA_LABEL"
            return 1
        fi
        started_data=1
        pk_info "$DATA_LABEL pid $PK_LAST_PID; waiting for direct health"
    else
        pk_step "$NODE_ID data service is already running; verifying direct health"
    fi

    if ! wait_direct_health; then
        pk_err "$NODE_ID did not pass direct health before membership join"
        pk_tail_process_log "$DATA_LABEL"
        rollback_started_pair "$started_data" "$started_member"
        return 1
    fi

    if [ "$member_alive" -eq 0 ]; then
        pk_step "Joining $NODE_ID to gossip membership"
        if ! pk_start_member "$NODE_ID"; then
            pk_tail_process_log "$MEMBER_LABEL"
            rollback_started_pair "$started_data" "$started_member"
            return 1
        fi
        started_member=1
        pk_info "$MEMBER_LABEL pid $PK_LAST_PID; waiting for full topology"
    else
        pk_step "$MEMBER_LABEL is monitoring for recovery; waiting for it to rejoin"
    fi

    if ! wait_topology "$ALL_LIVE"; then
        pk_err "$NODE_ID did not rejoin within ${TIMEOUT}s"
        pk_tail_process_log "$MEMBER_LABEL"
        pk_tail_process_log "$DATA_LABEL"
        rollback_started_pair "$started_data" "$started_member"
        return 1
    fi
    # Topology convergence is necessary but not sufficient: an unrecorded
    # stale sidecar could already be advertising this ID while the process we
    # just launched failed to bind. Require this exact managed pair to survive
    # the convergence window before declaring the restart successful.
    if ! pk_recorded_alive "$DATA_LABEL" || ! pk_recorded_alive "$MEMBER_LABEL"; then
        pk_err "$NODE_ID topology converged, but the newly managed process pair is not alive"
        pk_tail_process_log "$MEMBER_LABEL"
        pk_tail_process_log "$DATA_LABEL"
        rollback_started_pair "$started_data" "$started_member"
        return 1
    fi
    pk_ok "$NODE_ID joined; data and membership processes are healthy"
}

leave_pair() {
    require_cluster_runtime
    require_pair_alive
    [ -n "$WITHOUT_TARGET" ] || pk_die "cannot remove the cluster's only data node"

    pk_step "Gracefully removing $NODE_ID from gossip membership"
    local member_rc=0 topology_rc=0 data_rc=0
    pk_stop_managed "$MEMBER_LABEL" "$TIMEOUT" || member_rc=$?
    if [ "${PK_LAST_STOP_FORCED:-0}" -eq 1 ]; then
        pk_err "$MEMBER_LABEL did not complete a graceful leave; SIGKILL was required"
        member_rc=1
    fi

    if wait_topology "$WITHOUT_TARGET"; then
        :
    else
        topology_rc=$?
        pk_err "topology did not remove $NODE_ID within ${TIMEOUT}s"
    fi

    # Stop data even when convergence failed: the sidecar is already gone and
    # leaving an unadvertised engine behind makes recovery ambiguous.
    pk_stop_managed "$DATA_LABEL" "$TIMEOUT" || data_rc=$?
    if [ "$member_rc" -ne 0 ] || [ "$topology_rc" -ne 0 ] || [ "$data_rc" -ne 0 ]; then
        pk_tail_process_log "$MEMBER_LABEL"
        pk_tail_process_log "$DATA_LABEL"
        return 1
    fi
    pk_ok "$NODE_ID left membership before its data service stopped"
}

crash_pair() {
    require_cluster_runtime
    require_pair_alive
    [ -n "$WITHOUT_TARGET" ] || pk_die "cannot remove the cluster's only data node"

    pk_step "Crashing $NODE_ID data and membership processes"
    local member_pid data_pid
    pk_signal_managed "$MEMBER_LABEL" KILL
    member_pid="$PK_LAST_PID"
    pk_signal_managed "$DATA_LABEL" KILL
    data_pid="$PK_LAST_PID"

    pk_wait_pid_gone "$member_pid" 3 || pk_die "$MEMBER_LABEL survived SIGKILL"
    pk_wait_pid_gone "$data_pid" 3 || pk_die "$DATA_LABEL survived SIGKILL"
    pk_pid_remove_if "$MEMBER_LABEL" "$member_pid"
    pk_pid_remove_if "$DATA_LABEL" "$data_pid"

    wait_topology "$WITHOUT_TARGET" || {
        pk_tail_process_log controlplane
        return 1
    }
    pk_ok "$NODE_ID crash detected and removed from shard ownership"
}

crash_data_only() {
    require_cluster_runtime
    require_pair_alive
    [ -n "$WITHOUT_TARGET" ] || pk_die "cannot remove the cluster's only data node"

    pk_step "Crashing only the $NODE_ID data process"
    local data_pid
    pk_signal_managed "$DATA_LABEL" KILL
    data_pid="$PK_LAST_PID"
    pk_wait_pid_gone "$data_pid" 3 || pk_die "$DATA_LABEL survived SIGKILL"
    pk_pid_remove_if "$DATA_LABEL" "$data_pid"

    wait_topology "$WITHOUT_TARGET" || {
        pk_tail_process_log "$MEMBER_LABEL"
        return 1
    }

    # The transition proves the watchdog withdrew the endpoint. The sidecar
    # must stay alive so starting the data process is enough to rejoin.
    pk_recorded_alive "$MEMBER_LABEL" || {
        pk_err "$MEMBER_LABEL withdrew topology but did not stay up to monitor recovery"
        pk_tail_process_log "$MEMBER_LABEL"
        return 1
    }
    pk_ok "$NODE_ID data failure triggered withdrawal; sidecar is monitoring recovery"
}

case "$ACTION" in
    status)
        pk_step "$NODE_ID local process status"
        status_rc=0
        print_record "$DATA_LABEL" || status_rc=1
        print_record "$MEMBER_LABEL" || status_rc=1
        exit "$status_rc"
        ;;
    leave) leave_pair ;;
    crash) crash_pair ;;
    node-crash) crash_data_only ;;
    start) start_pair ;;
    restart)
        leave_pair
        start_pair
        ;;
esac
