"""PulseKV v2 Phase 8 Demo: vLLM Cross-Replica Multi-Layer Cache Hit (Step 8.4).

Demonstrates and measures reproducible prefix cache sharing between two
independent vLLM serving replicas backed by a shared PulseKV v2 cluster across
all transformer layers.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import List, Tuple

from pulsekv_adapters.vllm import PulseKVKVConnector
from pulsekv_adapters.vllm_key import derive_vllm_block_hashes


def run_vllm_cross_replica_trial(
    connector_a: PulseKVKVConnector,
    connector_b: PulseKVKVConnector,
    prefix_tokens: List[int],
    query_a_tokens: List[int],
    query_b_tokens: List[int],
    num_layers: int = 16,
    block_size: int = 16,
    layer_block_bytes: int = 4096,  # 4 KB per block per layer
) -> Tuple[bool, int, int, float, float, float]:
    """Execute one vLLM cross-replica cache sharing cycle across transformer layers.

    Returns:
        (success, prefix_tokens, matched_tokens, write_time_ms, match_time_ms, load_time_ms)
    """
    prompt_a = prefix_tokens + query_a_tokens
    prompt_b = prefix_tokens + query_b_tokens

    prefix_len = len(prefix_tokens)
    expected_matched_tokens = (prefix_len // block_size) * block_size

    hashes_a = derive_vllm_block_hashes(prompt_a, block_size=block_size)
    hashes_b = derive_vllm_block_hashes(prompt_b, block_size=block_size)

    # 1. Replica A processes Prompt A:
    # Simulates forward prefill pass computing KV cache across all layers and saving to PulseKV
    num_blocks_a = len(hashes_a)
    mock_layer_data = [os.urandom(num_blocks_a * layer_block_bytes) for _ in range(num_layers)]

    t0 = time.perf_counter()
    for layer_id in range(num_layers):
        connector_a.save_kv_layer(
            layer_id=layer_id,
            block_hashes=hashes_a,
            kv_data=mock_layer_data[layer_id],
        )
    connector_a.request_finished("req-replica-a")
    t1 = time.perf_counter()
    write_time_ms = (t1 - t0) * 1000.0

    # 2. Replica B receives Prompt B (which shares the exact same prefix_tokens):
    # Scheduler checks matched tokens
    t2 = time.perf_counter()
    matched_tokens = connector_b.get_num_new_matched_tokens("req-replica-b", prompt_b)
    t3 = time.perf_counter()
    match_time_ms = (t3 - t2) * 1000.0

    # Worker loads matched prefix blocks across all transformer layers
    matched_blocks = matched_tokens // block_size
    matched_hashes_b = hashes_b[:matched_blocks]

    t4 = time.perf_counter()
    all_layers_loaded_ok = True
    if matched_blocks > 0:
        for layer_id in range(num_layers):
            loaded = connector_b.load_kv_layer(
                layer_id=layer_id,
                block_hashes=matched_hashes_b,
            )
            expected_bytes = matched_blocks * layer_block_bytes
            if loaded is None or len(loaded) != expected_bytes:
                all_layers_loaded_ok = False
                break
    else:
        all_layers_loaded_ok = False
    connector_b.request_finished("req-replica-b")
    t5 = time.perf_counter()
    load_time_ms = (t5 - t4) * 1000.0

    success = (matched_tokens == expected_matched_tokens) and all_layers_loaded_ok
    return success, prefix_len, matched_tokens, write_time_ms, match_time_ms, load_time_ms


def main() -> int:
    parser = argparse.ArgumentParser(
        description="PulseKV v2 vLLM Cross-Replica Multi-Layer Cache Hit Demo"
    )
    parser.add_argument(
        "--control-plane",
        default=os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000"),
        help="Control plane address",
    )
    parser.add_argument("--trials", type=int, default=10, help="Number of demo trials to run")
    parser.add_argument(
        "--prefix-tokens",
        type=int,
        default=512,
        help="Number of shared prompt prefix tokens (default: 512)",
    )
    parser.add_argument(
        "--layers",
        type=int,
        default=16,
        help="Number of transformer layers to simulate (default: 16)",
    )
    parser.add_argument(
        "--block-size",
        type=int,
        default=16,
        help="Tokens per KV cache block (default: 16)",
    )
    parser.add_argument(
        "--block-kb",
        type=int,
        default=4,
        help="Kilobytes per KV block per layer (default: 4 KB)",
    )
    parser.add_argument(
        "--model",
        default="meta-llama/Llama-3-8B-Instruct",
        help="Model name identifier",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("PulseKV v2 — vLLM Cross-Replica Multi-Layer Cache Hit Demo (Step 8.4)")
    print("=" * 70)
    print(f"Control Plane:      {args.control_plane}")
    print(f"Model Identifier:   {args.model}")
    print(f"Simulated Layers:   {args.layers} transformer layers")
    print(
        f"Shared Prefix:      {args.prefix_tokens} tokens ({args.prefix_tokens // args.block_size} blocks @ {args.block_size} tokens/block)"
    )
    print(f"Block Size (Layer): {args.block_kb} KB per block/layer")
    print(f"Total Trials:       {args.trials}")
    print("-" * 70)

    # Initialize two independent vLLM KVConnector replica instances
    replica_a = PulseKVKVConnector(
        control_plane_address=args.control_plane,
        model_name=args.model,
        block_size=args.block_size,
    )
    replica_b = PulseKVKVConnector(
        control_plane_address=args.control_plane,
        model_name=args.model,
        block_size=args.block_size,
    )

    total_trials = args.trials
    successful_trials = 0
    total_matched_tokens = 0
    total_expected_tokens = 0
    write_latencies: List[float] = []
    match_latencies: List[float] = []
    load_latencies: List[float] = []

    expected_prefix_tokens = (args.prefix_tokens // args.block_size) * args.block_size

    for trial in range(1, total_trials + 1):
        # Unique prefix tokens per trial to prevent cross-trial cache collision
        base_token_id = trial * 200000
        prefix_tokens = list(range(base_token_id, base_token_id + args.prefix_tokens))
        query_a = [base_token_id + 90001, base_token_id + 90002, base_token_id + 90003, base_token_id + 90004]
        query_b = [base_token_id + 90051, base_token_id + 90052, base_token_id + 90053, base_token_id + 90054]

        success, pref_tok, matched_tok, w_time, m_time, l_time = run_vllm_cross_replica_trial(
            replica_a,
            replica_b,
            prefix_tokens,
            query_a,
            query_b,
            num_layers=args.layers,
            block_size=args.block_size,
            layer_block_bytes=args.block_kb * 1024,
        )

        if success:
            successful_trials += 1
        total_matched_tokens += matched_tok
        total_expected_tokens += expected_prefix_tokens
        write_latencies.append(w_time)
        match_latencies.append(m_time)
        load_latencies.append(l_time)

        status_str = "HIT (100%)" if success else f"MISS ({matched_tok}/{pref_tok})"
        print(
            f"Trial {trial:2d}/{total_trials:2d}: "
            f"Replica A Write ({args.layers}L): {w_time:6.2f}ms | "
            f"Replica B Match: {m_time:5.2f}ms | "
            f"Replica B Load ({args.layers}L): {l_time:6.2f}ms | "
            f"Result: {status_str}"
        )

    replica_a.close()
    replica_b.close()

    print("-" * 70)
    hit_rate = (successful_trials / total_trials) * 100.0
    avg_write = sum(write_latencies) / len(write_latencies)
    avg_match = sum(match_latencies) / len(match_latencies)
    avg_load = sum(load_latencies) / len(load_latencies)
    blocks_per_layer = args.prefix_tokens // args.block_size
    total_data_mb = (blocks_per_layer * args.block_kb * args.layers * total_trials) / 1024.0

    print("SUMMARY RESULTS:")
    print(f"  Trials Completed:            {total_trials}")
    print(f"  Successful Cache Hits:       {successful_trials}/{total_trials}")
    print(f"  Hit Rate:                    {hit_rate:.1f}%")
    print(f"  Total KV Cache Transferred:  {total_data_mb:.2f} MB")
    print(f"  Avg Replica A Write Time:    {avg_write:.2f} ms ({args.layers} layers)")
    print(f"  Avg Replica B Match Time:    {avg_match:.2f} ms (Scheduler probe)")
    print(f"  Avg Replica B Load Time:     {avg_load:.2f} ms ({args.layers} layers)")
    print("=" * 70)

    return 0 if successful_trials == total_trials else 1


if __name__ == "__main__":
    sys.exit(main())
