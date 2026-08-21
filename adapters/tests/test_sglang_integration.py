"""SGLang HiCache integration test (Step 7.3).

Verifies that PulseKVHiCacheStorage integrates seamlessly with SGLang's storage
backend conventions, supports tensor page serialization/deserialization,
and handles hierarchical cache lifecycle events against a live PulseKV cluster.
"""

import os
import unittest
from typing import List

from pulsekv_adapters.key import derive_prefix_keys
from pulsekv_adapters.sglang import (
    HAS_SGLANG,
    PulseKVHiCacheStorage,
    register_sglang_backend,
)

try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


class TestSGLangIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cp_addr = os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000")
        try:
            cls.storage = PulseKVHiCacheStorage(control_plane_address=cp_addr)
            cls.storage.exist("test_probe")
            cls.has_cluster = True
        except Exception:
            cls.has_cluster = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "has_cluster", False) and cls.storage:
            cls.storage.close()

    def setUp(self):
        if not self.has_cluster:
            self.skipTest("No live PulseKV cluster available at PULSEKV_CONTROL_PLANE_ADDRESS")

    def test_backend_registration(self):
        """Test that register_sglang_backend completes without error."""
        self.assertTrue(register_sglang_backend())

    def test_tensor_kv_page_round_trip(self):
        """Test storing and retrieving real or simulated multi-dimensional KV cache tensors."""
        # A typical KV page tensor shape: [num_layers, 2 (k,v), page_size, num_kv_heads, head_dim]
        # e.g., [4, 2, 16, 8, 64] in FP16 = 4 * 2 * 16 * 8 * 64 * 2 bytes = 131,072 bytes (128 KB)
        token_ids = list(range(10000, 10064))  # 64 tokens = 4 pages of 16
        keys = derive_prefix_keys(token_ids, page_size=16, pool_name="kv", model_name="llama-test")

        if HAS_TORCH:
            # Generate random KV page tensors
            tensors = [torch.randn(4, 2, 16, 8, 64, dtype=torch.float16) for _ in range(4)]
            for k, t in zip(keys, tensors):
                ok = self.storage.set(k, t)
                self.assertTrue(ok)

            # Retrieve into pre-allocated memory target
            for k, original_t in zip(keys, tensors):
                dest_tensor = torch.zeros_like(original_t)
                res = self.storage.get(k, target_location=dest_tensor)
                self.assertIsNotNone(res)
                self.assertTrue(torch.equal(dest_tensor, original_t))
        else:
            # Raw byte buffer test mimicking tensor memory layout
            page_payloads = [os.urandom(131072) for _ in range(4)]
            for k, p in zip(keys, page_payloads):
                ok = self.storage.set(k, p)
                self.assertTrue(ok)

            for k, p in zip(keys, page_payloads):
                got = self.storage.get(k)
                self.assertEqual(got, p)

    def test_hierarchical_radix_cache_lifecycle(self):
        """Simulate SGLang RadixCache insertion, hit checking, and eviction from L1/L2 to L3 (PulseKV)."""
        system_prompt_tokens = list(range(3000, 3048))  # 48 tokens = 3 pages
        user_prompt_1 = system_prompt_tokens + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        user_prompt_2 = system_prompt_tokens + [91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106]

        keys_1 = derive_prefix_keys(user_prompt_1, page_size=16, pool_name="kv")
        keys_2 = derive_prefix_keys(user_prompt_2, page_size=16, pool_name="kv")

        self.assertEqual(len(keys_1), 4)
        self.assertEqual(len(keys_2), 4)
        # Shared system prompt prefix
        self.assertEqual(keys_1[:3], keys_2[:3])

        # Step 1: Prompt 1 arrives. Check L3 cache -> 0 hits initially
        res_initial = self.storage.batch_exists_v2(keys_1)
        self.assertEqual(res_initial.kv_hit_pages, 0)

        # Step 2: Prefill executes on engine, writes computed KV pages to PulseKV L3
        for i, k in enumerate(keys_1):
            mock_kv_data = f"kv_data_layer_block_{i}_{k[:16]}".encode("utf-8") * 100
            self.storage.set(k, mock_kv_data)

        # Step 3: Prompt 2 arrives with identical system prompt prefix
        # SGLang checks L3 cache for prompt 2 keys
        res_prompt_2 = self.storage.batch_exists_v2(keys_2)
        # Should achieve 3 hit pages (the 48-token shared prefix)!
        self.assertEqual(res_prompt_2.kv_hit_pages, 3)

        # Step 4: SGLang loads the 3 shared pages from PulseKV without recomputing attention
        shared_pages = self.storage.batch_get(keys_2[:3])
        self.assertEqual(len(shared_pages), 3)
        for i, page in enumerate(shared_pages):
            self.assertIsNotNone(page)
            expected_data = f"kv_data_layer_block_{i}_{keys_2[i][:16]}".encode("utf-8") * 100
            self.assertEqual(page, expected_data)


if __name__ == "__main__":
    unittest.main()
