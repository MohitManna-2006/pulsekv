#!/usr/bin/env bash
#
# Deterministic Phase 3/4 kill/restart harness.
#
#   deploy/chaos-test.sh [--config PATH] [--target NODE_ID]
#                        [--cycles N] [--seed N]
#                        [--failure-mode alternating|crash|node-crash|leave]
#                        [--transition-timeout SECONDS]
#                        [--promotion-keys N]
#                        [--ready-file PATH] [--report PATH]
#
# pulsekv-chaos owns the correctness workload and topology-transition checks.
# This wrapper owns only local process mutation. It starts one watcher for the
# whole run, waits for its readiness file, executes exactly one remove/rejoin
# pair per cycle, and waits for the watcher to verify all 2*N transitions.
#
# PHASE 4: the watcher additionally seeds keys on shards the TARGET primaries,
# using require_replica_acks so they are provably on every replica before the
# kill, and then asserts the promoted replica serves them byte-for-byte. That
# proof is skipped, and said to be skipped, when the running cluster's
# replication factor is 0 -- which is a legal configuration the suite is
# expected to be run at. Boot such a cluster with:
#
#   deploy/run-local-cluster.sh --config deploy/cluster.chaos.config.yaml \
#       --replication-factor 0
#
# Every Phase 3 assertion is unchanged and still runs in both cases.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

TARGET=""
CYCLES=3
SEED=7
FAILURE_MODE=alternating
TRANSITION_TIMEOUT=15
PROMOTION_KEYS=8
READY_FILE=""
REPORT=""

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --config) [ $# -ge 2 ] || pk_die "--config requires a path"; PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*) PULSEKV_CONFIG="${1#*=}"; shift ;;
        --target) [ $# -ge 2 ] || pk_die "--target requires a node ID"; TARGET="$2"; shift 2 ;;
        --target=*) TARGET="${1#*=}"; shift ;;
        --cycles) [ $# -ge 2 ] || pk_die "--cycles requires a number"; CYCLES="$2"; shift 2 ;;
        --cycles=*) CYCLES="${1#*=}"; shift ;;
        --seed) [ $# -ge 2 ] || pk_die "--seed requires a number"; SEED="$2"; shift 2 ;;
        --seed=*) SEED="${1#*=}"; shift ;;
        --failure-mode) [ $# -ge 2 ] || pk_die "--failure-mode requires a value"; FAILURE_MODE="$2"; shift 2 ;;
        --failure-mode=*) FAILURE_MODE="${1#*=}"; shift ;;
        --transition-timeout) [ $# -ge 2 ] || pk_die "--transition-timeout requires seconds"; TRANSITION_TIMEOUT="$2"; shift 2 ;;
        --transition-timeout=*) TRANSITION_TIMEOUT="${1#*=}"; shift ;;
        --promotion-keys) [ $# -ge 2 ] || pk_die "--promotion-keys requires a number"; PROMOTION_KEYS="$2"; shift 2 ;;
        --promotion-keys=*) PROMOTION_KEYS="${1#*=}"; shift ;;
        --ready-file) [ $# -ge 2 ] || pk_die "--ready-file requires a path"; READY_FILE="$2"; shift 2 ;;
        --ready-file=*) READY_FILE="${1#*=}"; shift ;;
        --report) [ $# -ge 2 ] || pk_die "--report requires a path"; REPORT="$2"; shift 2 ;;
        --report=*) REPORT="${1#*=}"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

case "$CYCLES" in ''|*[!0-9]*) pk_die "--cycles must be a positive integer, got: $CYCLES" ;; esac
case "$SEED" in ''|*[!0-9]*) pk_die "--seed must be a non-negative integer, got: $SEED" ;; esac
case "$TRANSITION_TIMEOUT" in
    ''|*[!0-9]*) pk_die "--transition-timeout must be a positive integer, got: $TRANSITION_TIMEOUT" ;;
esac
# 0 is legal: it turns the promotion proof off explicitly.
case "$PROMOTION_KEYS" in
    ''|*[!0-9]*) pk_die "--promotion-keys must be a non-negative integer, got: $PROMOTION_KEYS" ;;
esac
CYCLES=$((10#$CYCLES))
SEED=$((10#$SEED))
TRANSITION_TIMEOUT=$((10#$TRANSITION_TIMEOUT))
PROMOTION_KEYS=$((10#$PROMOTION_KEYS))
[ "$CYCLES" -gt 0 ] || pk_die "--cycles must be a positive integer, got: $CYCLES"
[ "$TRANSITION_TIMEOUT" -gt 0 ] || \
    pk_die "--transition-timeout must be a positive integer, got: $TRANSITION_TIMEOUT"
case "$FAILURE_MODE" in
    alternating|crash|node-crash|leave) ;;
    *) pk_die "--failure-mode must be alternating, crash, node-crash, or leave" ;;
esac

[ -f "$PULSEKV_CONFIG" ] || pk_die "config not found: $PULSEKV_CONFIG"
for bin in "$PULSEKV_CONTROLPLANE_BIN" "$PULSEKV_MEMBER_BIN" "$PULSEKV_SMOKE_BIN" \
           "$PULSEKV_CHAOS_BIN" "$PULSEKV_NODE_BIN"; do
    [ -x "$bin" ] || pk_die "$(pk_relpath "$bin") is missing; run deploy/run-local-cluster.sh"
done
pk_recorded_alive controlplane || pk_die "the local control plane is not running"

node_table="$(pk_config_read --print-nodes)" || pk_die "could not read nodes from $PULSEKV_CONFIG"
[ -n "$node_table" ] || pk_die "config defines no nodes"
mapfile -t NODE_IDS < <(printf '%s\n' "$node_table" | cut -f1)
[ "${#NODE_IDS[@]}" -ge 2 ] || pk_die "chaos testing requires at least two configured nodes"

if [ -z "$TARGET" ]; then
    TARGET="${NODE_IDS[$((SEED % ${#NODE_IDS[@]}))]}"
fi
pk_node_line "$TARGET" >/dev/null 2>&1 || pk_die "unknown configured target: $TARGET"
pk_recorded_alive "data:${TARGET}" || pk_die "data:${TARGET} is not running"
pk_recorded_alive "member:${TARGET}" || pk_die "member:${TARGET} is not running"

CHAOS_DIR="${PULSEKV_RUN_DIR}/chaos"
mkdir -p "$CHAOS_DIR"
[ -n "$READY_FILE" ] || READY_FILE="${CHAOS_DIR}/ready"
[ -n "$REPORT" ] || REPORT="${CHAOS_DIR}/report.json"
[ "$READY_FILE" != "$REPORT" ] || pk_die "--ready-file and --report must differ"
[ ! -d "$READY_FILE" ] || pk_die "ready-file path is a directory: $READY_FILE"
[ ! -d "$REPORT" ] || pk_die "report path is a directory: $REPORT"
mkdir -p "$(dirname -- "$READY_FILE")" "$(dirname -- "$REPORT")"
rm -f -- "$READY_FILE" "$REPORT"

CONTROL_PLANE="$(pk_controlplane_address)"
ALL_LIVE="$(pk_node_ids_csv)"
WITHOUT_TARGET="$(pk_node_ids_csv "$TARGET")"
LOCAL_NODE="${PULSEKV_DEPLOY_DIR}/local-node.sh"
CHAOS_LOG="$(pk_log_for_label chaos)"
: > "$CHAOS_LOG"

WATCHER_PID=""
RESTORE_NEEDED=0

restore_target() {
    if pk_recorded_alive "data:${TARGET}" && pk_recorded_alive "member:${TARGET}"; then
        if "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
                --mode=topology-wait --expect-live="$ALL_LIVE" \
                --timeout="${TRANSITION_TIMEOUT}s" >/dev/null 2>&1; then
            return 0
        fi

        # Both processes survived but did not publish a coherent full epoch.
        # Drive a complete leave/rejoin instead of layering a second sidecar
        # incarnation over membership state that may still be converging.
        pk_warn "$TARGET processes are alive without a full topology; restarting them cleanly"
        "$LOCAL_NODE" --config "$PULSEKV_CONFIG" --timeout "$TRANSITION_TIMEOUT" \
            restart "$TARGET"
        return
    fi

    pk_warn "restoring $TARGET after an interrupted/failed chaos cycle"
    pk_stop_managed "member:${TARGET}" 2 || true
    pk_stop_managed "data:${TARGET}" 2 || true
    "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
        --mode=topology-wait --expect-live="$WITHOUT_TARGET" \
        --timeout="${TRANSITION_TIMEOUT}s" >/dev/null 2>&1 || return 1
    "$LOCAL_NODE" --config "$PULSEKV_CONFIG" --timeout "$TRANSITION_TIMEOUT" start "$TARGET"
}

cleanup() {
    local rc=$?
    trap - EXIT
    set +e
    if [ "$rc" -ne 0 ] && [ -n "$WATCHER_PID" ] && pk_pid_alive "$WATCHER_PID"; then
        kill -TERM "$WATCHER_PID" 2>/dev/null
        pk_wait_pid_gone "$WATCHER_PID" 2 || kill -KILL "$WATCHER_PID" 2>/dev/null
    fi
    if [ "$RESTORE_NEEDED" -eq 1 ]; then
        restore_target || {
            pk_err "automatic restoration of $TARGET failed"
            rc=1
        }
    fi
    if [ -n "$WATCHER_PID" ]; then
        pk_pid_remove_if chaos "$WATCHER_PID"
    fi
    exit "$rc"
}

on_signal() { exit 130; }
trap cleanup EXIT
trap on_signal INT TERM

pk_step "Checking the initial full topology"
"$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" \
    --mode=topology-wait --expect-live="$ALL_LIVE" \
    --timeout="${TRANSITION_TIMEOUT}s" 2>&1 | sed 's/^/    /'

pk_step "Starting the sustained correctness watcher"
pk_start_managed chaos "$CONTROL_PLANE" "$CHAOS_LOG" \
    "$PULSEKV_CHAOS_BIN" \
        --control-plane "$CONTROL_PLANE" \
        --target "$TARGET" \
        --cycles "$CYCLES" \
        --seed "$SEED" \
        --ready-file "$READY_FILE" \
        --report "$REPORT" \
        --promotion-keys "$PROMOTION_KEYS" \
        --transition-timeout "${TRANSITION_TIMEOUT}s"
WATCHER_PID="$PK_LAST_PID"
pk_info "watcher pid $WATCHER_PID; waiting for seeded stable-key workload"
# Reported from the config for context only. The watcher reads the factor from
# the LIVE topology and decides there whether promotion applies, so a config the
# running cluster was not actually booted with cannot mislead the assertions.
pk_dim "configured replication factor: $(pk_replication_factor 2>/dev/null || echo '?'); promotion keys: $PROMOTION_KEYS"

read_transition_count() {
    PK_TRANSITION_COUNT=""
    [ -f "$READY_FILE" ] || return 1
    IFS= read -r PK_TRANSITION_COUNT < "$READY_FILE" || [ -n "$PK_TRANSITION_COUNT" ]
    case "$PK_TRANSITION_COUNT" in
        ''|*[!0-9]*) return 1 ;;
    esac
    PK_TRANSITION_COUNT=$((10#$PK_TRANSITION_COUNT))
}

wait_transition_count() {
    local expected="$1" description="$2" deadline
    deadline=$((SECONDS + TRANSITION_TIMEOUT))
    while :; do
        # Read before checking liveness: after the final verified transition,
        # the watcher may publish the count and exit between two polls.
        if read_transition_count && [ "$PK_TRANSITION_COUNT" -ge "$expected" ]; then
            pk_ok "$description (verified transitions=$PK_TRANSITION_COUNT)"
            return 0
        fi
        if ! pk_pid_alive "$WATCHER_PID"; then
            pk_tail_process_log chaos 40
            pk_die "pulsekv-chaos exited before $description"
        fi
        [ "$SECONDS" -lt "$deadline" ] || {
            pk_tail_process_log chaos 40
            pk_die "pulsekv-chaos did not confirm $description within ${TRANSITION_TIMEOUT}s"
        }
        sleep 0.05
    done
}

wait_transition_count 0 "stable-key workload ready"
pk_ok "watcher ready; target=$TARGET cycles=$CYCLES seed=$SEED"

mode_for_cycle() {
    local cycle="$1"
    if [ "$FAILURE_MODE" != "alternating" ]; then
        printf '%s\n' "$FAILURE_MODE"
        return
    fi
    case $(((cycle - 1) % 3)) in
        0) printf '%s\n' crash ;;
        1) printf '%s\n' node-crash ;;
        2) printf '%s\n' leave ;;
    esac
}

for ((cycle = 1; cycle <= CYCLES; cycle++)); do
    mode="$(mode_for_cycle "$cycle")"
    pk_step "Chaos cycle ${cycle}/${CYCLES}: $mode $TARGET"
    pk_pid_alive "$WATCHER_PID" || {
        pk_tail_process_log chaos 40
        pk_die "pulsekv-chaos exited before cycle $cycle"
    }

    # Set this before mutation so the EXIT trap repairs even a half-completed
    # lifecycle command.
    RESTORE_NEEDED=1
    "$LOCAL_NODE" --config "$PULSEKV_CONFIG" --timeout "$TRANSITION_TIMEOUT" \
        "$mode" "$TARGET"
    wait_transition_count "$((2 * cycle - 1))" \
        "cycle $cycle removal of $TARGET"

    "$LOCAL_NODE" --config "$PULSEKV_CONFIG" --timeout "$TRANSITION_TIMEOUT" \
        start "$TARGET"
    wait_transition_count "$((2 * cycle))" \
        "cycle $cycle rejoin of $TARGET"
    RESTORE_NEEDED=0
done

pk_step "Waiting for the watcher to verify all topology transitions"
watcher_rc=0
wait "$WATCHER_PID" || watcher_rc=$?
pk_pid_remove_if chaos "$WATCHER_PID"
if [ "$watcher_rc" -ne 0 ]; then
    pk_tail_process_log chaos 60
    pk_die "pulsekv-chaos failed (exit $watcher_rc)"
fi
[ -s "$REPORT" ] || pk_die "pulsekv-chaos succeeded but wrote no report to $REPORT"

pk_ok "chaos test passed; report: $(pk_relpath "$REPORT")"
# Surface the promotion outcome rather than leaving it buried in JSON: a run
# that skipped the Phase 4 proof and a run that passed it both exit 0, and the
# difference is exactly what an operator needs to see.
if command -v python3 >/dev/null 2>&1; then
    python3 - "$REPORT" <<'PY' || true
import json, sys
with open(sys.argv[1]) as handle:
    report = json.load(handle)
skipped = report.get("promotion_skipped", "")
if skipped:
    print(f"    replication factor {report.get('replication_factor', '?')}: "
          f"promotion proof SKIPPED ({skipped})")
else:
    proofs = [t["promotion_proof"] for t in report.get("transitions", []) if t.get("promotion_proof")]
    keys = sum(p["keys_verified"] for p in proofs)
    kinds = sorted({p["kind"] for p in proofs})
    print(f"    replication factor {report.get('replication_factor', '?')}: "
          f"{len(proofs)} promotion proof(s) over {keys} key(s) [{', '.join(kinds)}]")
PY
fi
pk_dim "watcher log: $(pk_relpath "$CHAOS_LOG")"
