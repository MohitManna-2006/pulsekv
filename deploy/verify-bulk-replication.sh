#!/usr/bin/env bash
#
# Phase 6 exit criterion 3: Phase 4's replication forwarding and
# newly-owned-shard catch-up must stay byte-correct for LARGE values --
# the ones that now take the bulk transport instead of chunked gRPC.
#
#   deploy/verify-bulk-replication.sh [--config PATH] [--value-bytes N]
#
# Requires a running, replicated cluster. Deliberately uses a value ABOVE the
# 4 MiB unary limit, because that is exactly the threshold at which replication
# switches from a unary Put to the chunked/bulk path -- a smaller value would
# prove nothing about this phase.
#
# Two things are checked, both by reading the physical nodes directly rather
# than through the SDK:
#
#   1. after a strong-ack write, every replica holds the value byte-for-byte;
#   2. after the primary is destroyed and restarted with an empty engine,
#      catch-up refills it byte-for-byte from a peer.

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

VALUE_BYTES=$((6 * 1024 * 1024))
TIMEOUT=30

while [ $# -gt 0 ]; do
    case "$1" in
        --config) [ $# -ge 2 ] || pk_die "--config requires a path"; PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*) PULSEKV_CONFIG="${1#*=}"; shift ;;
        --value-bytes) [ $# -ge 2 ] || pk_die "--value-bytes requires a size"; VALUE_BYTES="$2"; shift 2 ;;
        --value-bytes=*) VALUE_BYTES="${1#*=}"; shift ;;
        -h|--help) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

pk_cluster_running || pk_die "no cluster is running; start one with deploy/run-local-cluster.sh"

CP_ENDPOINTS="$(pk_controlplane_endpoints)"
RF="$(pk_replication_factor)"
[ "$RF" -ge 1 ] || pk_die "this check needs replication_factor >= 1; the cluster reports $RF"

pk_step "Large-value replication and catch-up (${VALUE_BYTES} bytes, replication factor ${RF})"

BENCH_BIN="${PULSEKV_CMAKE_DIR}/pulsekv-bulk-bench"
[ -x "$BENCH_BIN" ] || pk_die "$(pk_relpath "$BENCH_BIN") is missing; build it with deploy/bench-bulk.sh"

KEY="bulk-replication-check"

# 1. Forwarding. Write the oversized value at its primary and wait for every
#    holder the control plane names to serve it byte-for-byte.
pk_step "Forwarding: an oversized value must reach every replica"
"$BENCH_BIN" --mode verify-replication --control-plane "$CP_ENDPOINTS" \
    --key "$KEY" --value-bytes "$VALUE_BYTES" --verify-timeout "$TIMEOUT" \
    2>&1 | sed 's/^/    /'

# Which node is the primary? The verifier prints the holder list; take the first.
TARGET="$("$BENCH_BIN" --mode verify-only --control-plane "$CP_ENDPOINTS" \
    --key "$KEY" --value-bytes "$VALUE_BYTES" --verify-timeout "$TIMEOUT" 2>/dev/null \
    | awk '/^holders/ { print $2 }' | tr -d ',')"
[ -n "$TARGET" ] || pk_die "could not determine the primary for $KEY"

# 2. Catch-up. Destroy the primary outright -- the engine has no WAL and purges
#    its spill tier at start, so it comes back genuinely empty. The only way it
#    can serve this value again is newly-owned-shard catch-up pulling it from a
#    peer, which for a value this size goes over the bulk path.
pk_step "Catch-up: destroying primary ${TARGET} and requiring it to refill"
"${PULSEKV_DEPLOY_DIR}/local-node.sh" --config "$PULSEKV_CONFIG" --timeout "$TIMEOUT" \
    crash "$TARGET" 2>&1 | sed 's/^/    /'
"${PULSEKV_DEPLOY_DIR}/local-node.sh" --config "$PULSEKV_CONFIG" --timeout "$TIMEOUT" \
    start "$TARGET" 2>&1 | sed 's/^/    /'

pk_step "Catch-up: every holder must serve the value again, byte-for-byte"
"$BENCH_BIN" --mode verify-only --control-plane "$CP_ENDPOINTS" \
    --key "$KEY" --value-bytes "$VALUE_BYTES" --verify-timeout "$TIMEOUT" \
    2>&1 | sed 's/^/    /'

pk_ok "large-value replication and catch-up are byte-correct over the bulk path"
