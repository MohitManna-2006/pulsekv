#!/usr/bin/env bash
#
# PulseKV v2 Phase 8 Demo: vLLM Cross-Replica Multi-Layer Cache Hit (Step 8.4).
#
#   deploy/demo-cross-replica-vllm.sh [--config PATH] [--trials N]
#                                     [--prefix-tokens N] [--layers N]
#                                     [--block-size N] [--model NAME]
#
# Exercises two independent vLLM KVConnector replica instances against
# the live PulseKV cluster. Replica A computes and stores KV blocks for a shared
# prompt prefix across multiple transformer layers, and Replica B achieves a
# verified 100% prefix cache hit from PulseKV without recomputing attention state.
#
# Intended to run inside the v2 dev image:
#
#   docker run --rm -v "$PWD:/src" -w /src pulsekv-v2-dev bash -c '
#     deploy/run-local-cluster.sh && deploy/demo-cross-replica-vllm.sh; rc=$?
#     deploy/stop-local-cluster.sh; exit $rc'

set -euo pipefail

# shellcheck source=deploy/common.sh
source "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/common.sh"

TRIALS=10
PREFIX_TOKENS=512
LAYERS=16
BLOCK_SIZE=16
MODEL="meta-llama/Llama-3-8B-Instruct"

while [ $# -gt 0 ]; do
    case "$1" in
        --config)        [ $# -ge 2 ] || pk_die "--config requires a path"; PULSEKV_CONFIG="$2"; shift 2 ;;
        --config=*)      PULSEKV_CONFIG="${1#*=}"; shift ;;
        --trials)        [ $# -ge 2 ] || pk_die "--trials requires a number"; TRIALS="$2"; shift 2 ;;
        --trials=*)      TRIALS="${1#*=}"; shift ;;
        --prefix-tokens) [ $# -ge 2 ] || pk_die "--prefix-tokens requires a number"; PREFIX_TOKENS="$2"; shift 2 ;;
        --prefix-tokens=*) PREFIX_TOKENS="${1#*=}"; shift ;;
        --layers)        [ $# -ge 2 ] || pk_die "--layers requires a number"; LAYERS="$2"; shift 2 ;;
        --layers=*)      LAYERS="${1#*=}"; shift ;;
        --block-size)    [ $# -ge 2 ] || pk_die "--block-size requires a number"; BLOCK_SIZE="$2"; shift 2 ;;
        --block-size=*)  BLOCK_SIZE="${1#*=}"; shift ;;
        --model)         [ $# -ge 2 ] || pk_die "--model requires a name"; MODEL="$2"; shift 2 ;;
        --model=*)       MODEL="${1#*=}"; shift ;;
        -h|--help)       sed -n '2,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *)               pk_die "unknown argument: $1 (try --help)" ;;
    esac
done

pk_cluster_running || pk_die "no cluster is running; start one with deploy/run-local-cluster.sh"

CP_ENDPOINTS="$(pk_controlplane_endpoints)"

pk_step "Running vLLM Cross-Replica Multi-Layer Cache Hit Demo (${TRIALS} trials, ${PREFIX_TOKENS} tokens, ${LAYERS} layers)"

PYTHONPATH="${PULSEKV_REPO_ROOT}/adapters" python3 \
    "${PULSEKV_REPO_ROOT}/adapters/tests/demo_cross_replica_vllm.py" \
    --control-plane "$CP_ENDPOINTS" \
    --trials "$TRIALS" \
    --prefix-tokens "$PREFIX_TOKENS" \
    --layers "$LAYERS" \
    --block-size "$BLOCK_SIZE" \
    --model "$MODEL"

pk_ok "vLLM cross-replica prefix cache hit demo verified successfully"
