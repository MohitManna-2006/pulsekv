#!/usr/bin/env bash
#
# PulseKV v2 Phase 7 Demo: SGLang Cross-Replica Prefix Cache Hit (Step 7.4).
#
#   deploy/demo-cross-replica-sglang.sh [--config PATH] [--trials N]
#                                       [--prefix-tokens N] [--page-size N]
#
# Exercises two independent SGLang HiCache replica storage instances against
# the live PulseKV cluster. Replica A computes and stores KV blocks for a shared
# prompt prefix, and Replica B achieves a verified 100% prefix cache hit from
# PulseKV without recomputing attention state.
#
# Intended to run inside the v2 dev image:
#
#   docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev bash -c '
#     deploy/run-local-cluster.sh && deploy/demo-cross-replica-sglang.sh; rc=$?
#     deploy/stop-local-cluster.sh; exit $rc'

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

TRIALS=10
PREFIX_TOKENS=512
PAGE_SIZE=16

while [ $# -gt 0 ]; do
    case "$1" in
        --config)        [ $# -ge 2 ] || pk_die "--config requires a path"; PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)      PULSEKV_CONFIG="${1#*=}"; shift ;;
        --trials)        [ $# -ge 2 ] || pk_die "--trials requires a number"; TRIALS="$2"; shift 2 ;;
        --trials=*)      TRIALS="${1#*=}"; shift ;;
        --prefix-tokens) [ $# -ge 2 ] || pk_die "--prefix-tokens requires a number"; PREFIX_TOKENS="$2"; shift 2 ;;
        --prefix-tokens=*) PREFIX_TOKENS="${1#*=}"; shift ;;
        --page-size)     [ $# -ge 2 ] || pk_die "--page-size requires a number"; PAGE_SIZE="$2"; shift 2 ;;
        --page-size=*)   PAGE_SIZE="${1#*=}"; shift ;;
        -h|--help)       sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)               pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

pk_cluster_running || pk_die "no cluster is running; start one with deploy/run-local-cluster.sh"

CP_ENDPOINTS="$(pk_controlplane_endpoints)"

pk_step "Running SGLang Cross-Replica Cache Hit Demo (${TRIALS} trials, ${PREFIX_TOKENS} tokens prefix)"

PYTHONPATH="${PULSEKV_REPO_ROOT}/adapters" python3 \
    "${PULSEKV_REPO_ROOT}/adapters/tests/demo_cross_replica.py" \
    --control-plane "$CP_ENDPOINTS" \
    --trials "$TRIALS" \
    --prefix-tokens "$PREFIX_TOKENS" \
    --page-size "$PAGE_SIZE"

pk_ok "SGLang cross-replica prefix cache hit demo verified successfully"
