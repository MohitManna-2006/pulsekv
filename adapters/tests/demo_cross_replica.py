"""PulseKV v2 Phase 7 Demo: Multi-Replica Cross-Cache Hit (Step 7.4).

Demonstrates and measures reproducible prefix cache sharing between two
independent SGLang serving replicas backed by a shared PulseKV v2 cluster.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import List, Tuple

from pulsekv_adapters.key import derive_prefix_keys
from pulsekv_adapters.sglang import PulseKVHiCacheStorage


def run_cross_replica_trial(
    storage_a: PulseKVHiCacheStorage,
    storage_b: PulseKVHiCacheStorage,
    prefix_tokens: List[int],
    query_a_tokens: List[int],
    query_b_tokens: List[int],
    page_size: int = 16,
    page_bytes: int = 65536,  # 64 KB per page
) -> Tuple[bool, int, int, float, float]:
    """Execute one cross-replica cache sharing cycle.

    Returns:
        (success, prefix_pages, hit_pages, write_time_ms, read_time_ms)
    """
    prompt_a = prefix_tokens + query_a_tokens
    prompt_b = prefix_tokens + query_b_tokens

    keys_a = derive_prefix_keys(prompt_a, page_size=page_size, pool_name="kv")
    keys_b = derive_prefix_keys(prompt_b, page_size=page_size, pool_name="kv")

    prefix_pages = len(prefix_tokens) // page_size

    # 1. Replica A processes Prompt A:
    # Computes KV cache and writes blocks to PulseKV L3
    mock_payloads = [os.urandom(page_bytes) for _ in range(len(keys_a))]
    
    t0 = time.perf_counter()
    for k, p in zip(keys_a, mock_payloads):
        storage_a.set(k, p)
    t1 = time.perf_counter()
    write_time_ms = (t1 - t0) * 1000.0

    # 2. Replica B receives Prompt B (which shares the exact same prefix_tokens)
    t2 = time.perf_counter()
    # Check L3 cache for Prompt B's keys
    res_b = storage_b.batch_exists_v2(keys_b)
    hit_pages = res_b.kv_hit_pages

    # Fetch the shared cached pages
    if hit_pages > 0:
        fetched = storage_b.batch_get(keys_b[:hit_pages])
        fetch_ok = all(f is not None and len(f) == page_bytes for f in fetched)
    else:
        fetch_ok = False
    t3 = time.perf_counter()
    read_time_ms = (t3 - t2) * 1000.0

    success = (hit_pages == prefix_pages) and fetch_ok
    return success, prefix_pages, hit_pages, write_time_ms, read_time_ms


def main() -> int:
    parser = argparse.ArgumentParser(description="PulseKV v2 SGLang Cross-Replica Cache Hit Demo")
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
        "--page-size",
        type=int,
        default=16,
        help="Tokens per KV cache block (default: 16)",
    )
    parser.add_argument(
        "--page-kb",
        type=int,
        default=64,
        help="Kilobytes per KV page (default: 64 KB)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("PulseKV v2 — SGLang Cross-Replica Prefix Cache Hit Demo (Step 7.4)")
    print("=" * 70)
    print(f"Control Plane:      {args.control_plane}")
    print(f"Shared Prefix:      {args.prefix_tokens} tokens ({args.prefix_tokens // args.page_size} pages @ {args.page_size} tokens/page)")
    print(f"Page Size:          {args.page_kb} KB per page")
    print(f"Total Trials:       {args.trials}")
    print("-" * 70)

    # Initialize two independent SGLang HiCache replica storage instances
    replica_a = PulseKVHiCacheStorage(control_plane_address=args.control_plane)
    replica_b = PulseKVHiCacheStorage(control_plane_address=args.control_plane)

    total_trials = args.trials
    successful_trials = 0
    total_hit_pages = 0
    total_expected_pages = 0
    write_latencies: List[float] = []
    read_latencies: List[float] = []

    expected_prefix_pages = args.prefix_tokens // args.page_size

    for trial in range(1, total_trials + 1):
        # Generate unique prefix tokens for this trial to avoid stale cache from previous trials
        base_token_id = trial * 100000
        prefix_tokens = list(range(base_token_id, base_token_id + args.prefix_tokens))
        query_a = [base_token_id + 90001, base_token_id + 90002, base_token_id + 90003, base_token_id + 90004]
        query_b = [base_token_id + 90051, base_token_id + 90052, base_token_id + 90053, base_token_id + 90054]

        success, pref_pages, hit_pages, w_time, r_time = run_cross_replica_trial(
            replica_a,
            replica_b,
            prefix_tokens,
            query_a,
            query_b,
            page_size=args.page_size,
            page_bytes=args.page_kb * 1024,
        )

        if success:
            successful_trials += 1
        total_hit_pages += hit_pages
        total_expected_pages += pref_pages
        write_latencies.append(w_time)
        read_latencies.append(r_time)

        status_str = "HIT (100%)" if success else f"MISS ({hit_pages}/{pref_pages})"
        print(
            f"Trial {trial:2d}/{total_trials:2d}: "
            f"Replica A Write: {w_time:6.2f}ms | "
            f"Replica B Lookup+Read: {r_time:6.2f}ms | "
            f"Result: {status_str}"
        )

    replica_a.close()
    replica_b.close()

    print("-" * 70)
    reproduction_rate = (successful_trials / total_trials) * 100.0
    avg_write = sum(write_latencies) / len(write_latencies)
    avg_read = sum(read_latencies) / len(read_latencies)
    total_data_mb = (expected_prefix_pages * args.page_kb * total_trials) / 1024.0

    print("SUMMARY RESULTS:")
    print(f"  Trials Completed:         {total_trials}")
    print(f"  Successful Cache Hits:    {successful_trials}/{total_trials}")
    print(f"  Hit Rate:                 {reproduction_rate:.1f}%")
    print(f"  Total Shared KV Transferred: {total_data_mb:.2f} MB")
    print(f"  Avg Replica A Write Time: {avg_write:.2f} ms ({args.prefix_tokens} tokens)")
    print(f"  Avg Replica B Read Time:  {avg_read:.2f} ms ({args.prefix_tokens} tokens)")
    print("=" * 70)

    if successful_trials == total_trials:
        print(">> DEMO PASSED: 100% cross-replica prefix cache hit reproduction rate verified!")
        return 0
    else:
        print(f">> DEMO FAILED: reproduction rate was {reproduction_rate:.1f}% (< 100%)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
