#!/usr/bin/env bash
#
# Long-duration soak and fault-injection harness for PulseKV v2 (Phase 9.4).
#
#   deploy/soak-test.sh [--config PATH] [--duration DURATION]
#                       [--interval DURATION] [--workers N]
#                       [--keys N] [--value-size BYTES]
#                       [--key-distribution uniform|zipf] [--zipf-s S]
#                       [--replicas N] [--chaos-interval DURATION]
#                       [--no-chaos] [--metrics-listen ADDR]
#                       [--report PATH] [--metrics-output PATH]
#                       [--skip-build] [--skip-cluster-lifecycle]
#
# Orchestrates:
#   1. Clean cluster boot with Raft metadata group (unless --skip-cluster-lifecycle).
#   2. Background Prometheus metrics exporter (pulsekv-metrics).
#   3. Background interleaved chaos injector (node crash/recovery and Raft leader stepdown).
#   4. Foreground sustained load generator (pulsekv-cluster-bench in --duration mode
#      with Zipf key skew, N independent clients, --continue-on-error, and time-series reporting).
#   5. End-of-run metrics snapshot, drift computation, structured JSON report, and clean shutdown.
#

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

DURATION="4h"
INTERVAL="30s"
WORKERS=16
KEYS=20000
VALUE_SIZE=65536
KEY_DISTRIBUTION="zipf"
ZIPF_S=1.1
REPLICAS=4
CHAOS_INTERVAL="45s"
ENABLE_CHAOS=1
METRICS_LISTEN="127.0.0.1:9095"
REPORT="${PULSEKV_RUN_DIR}/soak-report.json"
METRICS_OUTPUT="${PULSEKV_RUN_DIR}/soak-metrics.prom"
SKIP_BUILD=0
SKIP_CLUSTER_LIFECYCLE=0

usage() {
    sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --config)
            [ $# -ge 2 ] || pk_die "--config requires a path"
            PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)
            PULSEKV_CONFIG="${1#*=}"; shift ;;
        --duration)
            [ $# -ge 2 ] || pk_die "--duration requires a duration string (e.g. 4h, 30m, 90s)"
            DURATION="$2"; shift 2 ;;
        --duration=*)
            DURATION="${1#*=}"; shift ;;
        --interval)
            [ $# -ge 2 ] || pk_die "--interval requires a duration string"
            INTERVAL="$2"; shift 2 ;;
        --interval=*)
            INTERVAL="${1#*=}"; shift ;;
        --workers)
            [ $# -ge 2 ] || pk_die "--workers requires a number"
            WORKERS="$2"; shift 2 ;;
        --workers=*)
            WORKERS="${1#*=}"; shift ;;
        --keys)
            [ $# -ge 2 ] || pk_die "--keys requires a number"
            KEYS="$2"; shift 2 ;;
        --keys=*)
            KEYS="${1#*=}"; shift ;;
        --value-size)
            [ $# -ge 2 ] || pk_die "--value-size requires bytes"
            VALUE_SIZE="$2"; shift 2 ;;
        --value-size=*)
            VALUE_SIZE="${1#*=}"; shift ;;
        --key-distribution)
            [ $# -ge 2 ] || pk_die "--key-distribution requires uniform or zipf"
            KEY_DISTRIBUTION="$2"; shift 2 ;;
        --key-distribution=*)
            KEY_DISTRIBUTION="${1#*=}"; shift ;;
        --zipf-s)
            [ $# -ge 2 ] || pk_die "--zipf-s requires a float"
            ZIPF_S="$2"; shift 2 ;;
        --zipf-s=*)
            ZIPF_S="${1#*=}"; shift ;;
        --replicas)
            [ $# -ge 2 ] || pk_die "--replicas requires a number"
            REPLICAS="$2"; shift 2 ;;
        --replicas=*)
            REPLICAS="${1#*=}"; shift ;;
        --chaos-interval)
            [ $# -ge 2 ] || pk_die "--chaos-interval requires a duration string"
            CHAOS_INTERVAL="$2"; shift 2 ;;
        --chaos-interval=*)
            CHAOS_INTERVAL="${1#*=}"; shift ;;
        --no-chaos)
            ENABLE_CHAOS=0; shift ;;
        --metrics-listen)
            [ $# -ge 2 ] || pk_die "--metrics-listen requires ADDR:PORT"
            METRICS_LISTEN="$2"; shift 2 ;;
        --metrics-listen=*)
            METRICS_LISTEN="${1#*=}"; shift ;;
        --report)
            [ $# -ge 2 ] || pk_die "--report requires a path"
            REPORT="$2"; shift 2 ;;
        --report=*)
            REPORT="${1#*=}"; shift ;;
        --metrics-output)
            [ $# -ge 2 ] || pk_die "--metrics-output requires a path"
            METRICS_OUTPUT="$2"; shift 2 ;;
        --metrics-output=*)
            METRICS_OUTPUT="${1#*=}"; shift ;;
        --skip-build)
            SKIP_BUILD=1; shift ;;
        --skip-cluster-lifecycle)
            SKIP_CLUSTER_LIFECYCLE=1; shift ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

[ -f "$PULSEKV_CONFIG" ] || pk_die "config not found: $PULSEKV_CONFIG"
mkdir -p "$(dirname -- "$REPORT")" "$(dirname -- "$METRICS_OUTPUT")" "${PULSEKV_LOG_DIR}"

METRICS_PID=""
CHAOS_PID=""
STARTED_CLUSTER=0

cleanup() {
    local rc=$?
    trap - EXIT INT TERM

    if [ -n "$CHAOS_PID" ] && pk_pid_alive "$CHAOS_PID"; then
        pk_dim "Stopping background chaos injector (pid $CHAOS_PID)"
        kill "$CHAOS_PID" 2>/dev/null || true
        wait "$CHAOS_PID" 2>/dev/null || true
    fi

    if [ -n "$METRICS_PID" ] && pk_pid_alive "$METRICS_PID"; then
        pk_dim "Stopping metrics exporter (pid $METRICS_PID)"
        kill "$METRICS_PID" 2>/dev/null || true
        wait "$METRICS_PID" 2>/dev/null || true
    fi

    if [ "$STARTED_CLUSTER" -eq 1 ]; then
        pk_step "Stopping local cluster"
        "$PULSEKV_DEPLOY_DIR/stop-local-cluster.sh" >/dev/null 2>&1 || true
    fi

    if [ "$rc" -eq 0 ]; then
        pk_ok "Soak test execution completed successfully"
    else
        pk_err "Soak test exited with error code $rc"
    fi
    exit "$rc"
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# 1. Start cluster if needed.
# ---------------------------------------------------------------------------
if [ "$SKIP_CLUSTER_LIFECYCLE" -eq 0 ]; then
    pk_step "Starting local cluster for soak test ($DURATION)"
    CLUSTER_ARGS=(--config "$PULSEKV_CONFIG")
    if [ "$SKIP_BUILD" -eq 1 ]; then
        CLUSTER_ARGS+=(--skip-build)
    fi
    "$PULSEKV_DEPLOY_DIR/run-local-cluster.sh" "${CLUSTER_ARGS[@]}"
    STARTED_CLUSTER=1
else
    pk_step "Using already running cluster (--skip-cluster-lifecycle)"
    pk_any_controlplane_alive || pk_die "no control-plane replica is alive; run without --skip-cluster-lifecycle"
fi

CP_ENDPOINTS="$(pk_controlplane_endpoints)" || pk_die "could not read controlplane endpoints"
node_table="$(pk_config_read --print-nodes)" || pk_die "could not read nodes from config"
mapfile -t NODE_IDS < <(printf '%s\n' "$node_table" | cut -f1)
[ "${#NODE_IDS[@]}" -gt 0 ] || pk_die "config defines no data nodes"

# ---------------------------------------------------------------------------
# 2. Launch pulsekv-metrics daemon.
# ---------------------------------------------------------------------------
pk_step "Launching Prometheus metrics exporter on http://${METRICS_LISTEN}/metrics"
METRICS_LOG="$(pk_log_for_label "metrics")"
"$PULSEKV_METRICS_BIN" \
    --control-plane="$CP_ENDPOINTS" \
    --listen="$METRICS_LISTEN" \
    --interval=5s \
    --probe=true \
    --probe-interval=15s \
    >"$METRICS_LOG" 2>&1 &
METRICS_PID="$!"

# Verify metrics endpoint starts listening within 10s.
METRICS_READY=0
for _ in $(seq 1 20); do
    if curl -s -f -m 1 "http://${METRICS_LISTEN}/metrics" >/dev/null 2>&1; then
        METRICS_READY=1
        break
    fi
    pk_pid_alive "$METRICS_PID" || break
    sleep 0.5
done
if [ "$METRICS_READY" -eq 1 ]; then
    pk_ok "Prometheus exporter live (pid $METRICS_PID)"
else
    pk_warn "Prometheus exporter failed to respond within 10s (check $(pk_relpath "$METRICS_LOG"))"
fi

# ---------------------------------------------------------------------------
# 3. Launch background chaos injector if enabled.
# ---------------------------------------------------------------------------
if [ "$ENABLE_CHAOS" -eq 1 ]; then
    pk_step "Starting background chaos injector (interval: $CHAOS_INTERVAL)"
    CHAOS_LOG="$(pk_log_for_label "soak-chaos")"

    # Convert chaos interval string (e.g. 45s, 1m) into seconds.
    CHAOS_SLEEP_SEC=45
    if [[ "$CHAOS_INTERVAL" =~ ^([0-9]+)s$ ]]; then
        CHAOS_SLEEP_SEC="${BASH_REMATCH[1]}"
    elif [[ "$CHAOS_INTERVAL" =~ ^([0-9]+)m$ ]]; then
        CHAOS_SLEEP_SEC=$((${BASH_REMATCH[1]} * 60))
    elif [[ "$CHAOS_INTERVAL" =~ ^([0-9]+)$ ]]; then
        CHAOS_SLEEP_SEC="$CHAOS_INTERVAL"
    fi
    [ "$CHAOS_SLEEP_SEC" -gt 0 ] || CHAOS_SLEEP_SEC=45

    (
        cycle=0
        num_nodes="${#NODE_IDS[@]}"
        # Initial grace period: allow benchmark populate and warmup phases to complete before injecting faults.
        sleep 30
        while true; do
            sleep "$CHAOS_SLEEP_SEC"
            cycle=$((cycle + 1))
            target_node="${NODE_IDS[$(( (cycle - 1) % num_nodes ))]}"

            printf '[%s] [chaos-cycle %d] Crashing target data node: %s\n' \
                "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$target_node" >> "$CHAOS_LOG"

            # 1. Crash data node.
            "$PULSEKV_DEPLOY_DIR/local-node.sh" --config "$PULSEKV_CONFIG" --timeout 15 \
                crash "$target_node" >> "$CHAOS_LOG" 2>&1 || true

            # Hold node down for 5-8s so SWIM failure detection and shard reassignment fire.
            sleep 6

            printf '[%s] [chaos-cycle %d] Restarting target data node: %s\n' \
                "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$target_node" >> "$CHAOS_LOG"

            # 2. Restart data node.
            "$PULSEKV_DEPLOY_DIR/local-node.sh" --config "$PULSEKV_CONFIG" --timeout 20 \
                start "$target_node" >> "$CHAOS_LOG" 2>&1 || true

            # 3. Every 4 cycles, step down Raft leader if 3+ control plane replicas exist.
            if [ $((cycle % 4)) -eq 0 ]; then
                cp_count="$(pk_controlplane_ids | grep -c . || echo 0)"
                if [ "$cp_count" -ge 3 ]; then
                    leader_id="$("$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" --mode=leader 2>/dev/null | grep -o 'cp-[0-9]' || true)"
                    if [ -n "$leader_id" ]; then
                        printf '[%s] [chaos-cycle %d] Cycling Raft leader: %s\n' \
                            "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$cycle" "$leader_id" >> "$CHAOS_LOG"
                        pk_stop_managed "controlplane:${leader_id}" 2 >> "$CHAOS_LOG" 2>&1 || true
                        sleep 4
                        pk_start_controlplane "$leader_id" >> "$CHAOS_LOG" 2>&1 || true
                        "$PULSEKV_SMOKE_BIN" --config "$PULSEKV_CONFIG" --mode=wait --min-control-plane=3 >> "$CHAOS_LOG" 2>&1 || true
                    fi
                fi
            fi
        done
    ) &
    CHAOS_PID="$!"
    pk_ok "Chaos injector active (pid $CHAOS_PID, logs at $(pk_relpath "$CHAOS_LOG"))"
else
    pk_step "Fault injection disabled (--no-chaos)"
fi

# ---------------------------------------------------------------------------
# 4. Run sustained cluster-bench workload in foreground.
# ---------------------------------------------------------------------------
pk_step "Running sustained load generator for $DURATION"
pk_info "Parameters: workers=$WORKERS, keys=$KEYS, value_size=$VALUE_SIZE bytes, dist=$KEY_DISTRIBUTION (s=$ZIPF_S), replicas=$REPLICAS"

"$PULSEKV_CLUSTER_BENCH_BIN" \
    --control-plane="$CP_ENDPOINTS" \
    --concurrency="$WORKERS" \
    --keys="$KEYS" \
    --value-size="$VALUE_SIZE" \
    --duration="$DURATION" \
    --interval="$INTERVAL" \
    --key-distribution="$KEY_DISTRIBUTION" \
    --zipf-s="$ZIPF_S" \
    --replicas="$REPLICAS" \
    --continue-on-error=true \
    --json="$REPORT"

# ---------------------------------------------------------------------------
# 5. Snapshot Prometheus metrics and summarize.
# ---------------------------------------------------------------------------
pk_step "Capturing final Prometheus metrics snapshot to $(pk_relpath "$METRICS_OUTPUT")"
if curl -s -f -m 3 "http://${METRICS_LISTEN}/metrics" > "$METRICS_OUTPUT" 2>/dev/null; then
    series_count="$(grep -v '^#' "$METRICS_OUTPUT" | grep -c . || echo 0)"
    pk_ok "Captured $series_count metrics series"
else
    pk_warn "Could not fetch final metrics snapshot from http://${METRICS_LISTEN}/metrics"
fi

pk_step "Soak run report summary"
if [ -f "$REPORT" ]; then
    pk_ok "JSON report written to $(pk_relpath "$REPORT")"
else
    pk_warn "Report file not found at $(pk_relpath "$REPORT")"
fi
