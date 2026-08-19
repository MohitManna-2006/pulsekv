#!/usr/bin/env bash
#
# soak-chaos-injector.sh -- the background fault injector soak-test.sh runs.
#
#   deploy/soak-chaos-injector.sh --config PATH --interval SECONDS
#                                 --log PATH --parent PID [--grace SECONDS]
#
# This used to be an inline `( while true; ... ) &` subshell inside
# soak-test.sh. It is a real script for three reasons, all of them consequences
# of the 2026-08-19 incident (docs/pulsekv-v2-soak-collapse-analysis.md):
#
#   1. It has a name. `pgrep -f soak-chaos-injector.sh` finds an injector
#      that outlived its parent; a subshell is indistinguishable from the script
#      that forked it, so nothing could find the three that were running at once.
#   2. It watches its parent. If the soak process is gone -- killed outright, a
#      closed terminal, a torn-down container -- this exits at the top of the
#      next iteration instead of crashing data nodes for the rest of the day.
#   3. It takes ONE lifecycle action at a time. The old loop could run its
#      "cycle the Raft leader" step while its own node restart was still
#      settling, which is concurrent lifecycle mutation from a single injector,
#      before any orphan is involved.
#
# It deliberately does not decide when to stop: the parent kills it, or the
# parent's absence does.
#

set -uo pipefail

source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

INTERVAL=45
LOG=""
PARENT=""
HOLD_DOWN=6

while [ $# -gt 0 ]; do
    case "$1" in
        --config)   PULSEKV_CONFIG="$2"; shift 2 ;;
        --interval) INTERVAL="$2"; shift 2 ;;
        --log)      LOG="$2"; shift 2 ;;
        --parent)   PARENT="$2"; shift 2 ;;
        --hold-down) HOLD_DOWN="$2"; shift 2 ;;
        *) pk_die "unknown argument: $1" ;;
    esac
done

[ -n "$LOG" ] || pk_die "--log is required"
[ -n "$PARENT" ] || pk_die "--parent is required"
[ -f "$PULSEKV_CONFIG" ] || pk_die "config not found: $PULSEKV_CONFIG"

node_table="$(pk_config_read --print-nodes)" || pk_die "could not read nodes from config"
mapfile -t NODE_IDS < <(printf '%s\n' "$node_table" | cut -f1)
[ "${#NODE_IDS[@]}" -gt 0 ] || pk_die "config defines no data nodes"

log_event() {
    printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*" >>"$LOG"
}

# The parent check is the whole orphan defence. It costs one kill -0 per
# iteration and makes "the soak died without cleaning up" self-correcting.
parent_gone() {
    ! pk_pid_alive "$PARENT"
}

trap 'log_event "[injector] terminating on signal"; exit 0' INT TERM

log_event "[injector] started (pid $$, parent $PARENT, interval ${INTERVAL}s, nodes: ${NODE_IDS[*]})"

# Initial grace period: let the benchmark's populate and warmup phases finish
# before injecting anything.
for _ in $(seq 1 30); do
    parent_gone && { log_event "[injector] parent $PARENT is gone during grace period; exiting"; exit 0; }
    sleep 1
done

cycle=0
num_nodes="${#NODE_IDS[@]}"

while true; do
    for _ in $(seq 1 "$INTERVAL"); do
        parent_gone && { log_event "[injector] parent $PARENT is gone; exiting after $cycle cycle(s)"; exit 0; }
        sleep 1
    done

    cycle=$((cycle + 1))
    target_node="${NODE_IDS[$(( (cycle - 1) % num_nodes ))]}"

    log_event "[chaos-cycle $cycle] Crashing target data node: $target_node"
    "$PULSEKV_DEPLOY_DIR/local-node.sh" --config "$PULSEKV_CONFIG" --timeout 15 \
        crash "$target_node" >>"$LOG" 2>&1 || true

    # Hold the node down long enough for SWIM failure detection and shard
    # reassignment to fire.
    sleep "$HOLD_DOWN"

    log_event "[chaos-cycle $cycle] Restarting target data node: $target_node"
    "$PULSEKV_DEPLOY_DIR/local-node.sh" --config "$PULSEKV_CONFIG" --timeout 20 \
        start "$target_node" >>"$LOG" 2>&1 || true

    # Every fourth cycle, cycle the Raft leader -- but only after the data node
    # above is actually back. Two lifecycle operations in flight at once is the
    # condition that started the 2026-08-19 outage, and this injector will not
    # create it by itself.
    if [ $((cycle % 4)) -eq 0 ]; then
        if ! "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" --mode=wait \
                --min-control-plane=2 >>"$LOG" 2>&1; then
            log_event "[chaos-cycle $cycle] skipping Raft leader cycle: cluster has not settled yet"
            continue
        fi

        cp_count="$(pk_controlplane_ids | grep -c . || echo 0)"
        if [ "$cp_count" -ge 3 ]; then
            leader_id="$("$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" --mode=leader 2>/dev/null \
                | grep -o 'cp-[0-9]' || true)"
            if [ -n "$leader_id" ]; then
                log_event "[chaos-cycle $cycle] Cycling Raft leader: $leader_id"
                pk_stop_managed "controlplane:${leader_id}" 10 >>"$LOG" 2>&1 || true
                sleep 4
                pk_start_controlplane "$leader_id" >>"$LOG" 2>&1 || true
                "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" --mode=wait \
                    --min-control-plane=3 >>"$LOG" 2>&1 || true
            fi
        fi
    fi
done
