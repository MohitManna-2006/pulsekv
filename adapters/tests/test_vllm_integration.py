"""vLLM KVConnector integration test (Phase 8 Step 8.4).

Verifies multi-layer KV cache tensor saving and loading across transformer layers,
scheduler-worker coordination lifecycle, and prefix reuse against a live PulseKV cluster.
"""

import os
import unittest
from typing import List

from pulsekv_adapters.vllm import (
    HAS_VLLM,
    PulseKVKVConnector,
    register_vllm_connector,
)
from pulsekv_adapters.vllm_key import (
    derive_vllm_block_hashes,
    derive_vllm_layer_keys,
)

try:
    import torch
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False


class TestVLLMIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cp_addr = os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000")
        try:
            cls.connector = PulseKVKVConnector(
                control_plane_address=cp_addr,
                model_name="meta-llama/Llama-3-8B-Instruct",
                block_size=16,
            )
            # Connectivity probe
            cls.connector.client.exist("test_vllm_probe")
            cls.has_cluster = True
        except Exception:
            cls.has_cluster = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "has_cluster", False) and cls.connector:
            cls.connector.close()

    def setUp(self):
        if not self.has_cluster:
            self.skipTest("No live PulseKV cluster available at PULSEKV_CONTROL_PLANE_ADDRESS")

    def test_connector_registration(self):
        """Test that register_vllm_connector executes cleanly."""
        register_vllm_connector()

    def test_multi_layer_tensor_kv_roundtrip(self):
        """Test storing and loading KV cache across multiple transformer layers."""
        num_layers = 8
        tokens = list(range(20000, 20064))  # 64 tokens = 4 blocks @ 16 tokens/block
        block_hashes = derive_vllm_block_hashes(tokens, block_size=16)

        if HAS_TORCH:
            # Synthetic KV tensors for 8 layers: shape [2 (k,v), num_blocks=4, block_size=16, num_heads=8, head_dim=64]
            # Size per layer: 2 * 4 * 16 * 8 * 64 * 2 (FP16) = 131,072 bytes (128 KB)
            layer_tensors = [
                torch.randn(2, 4, 16, 8, 64, dtype=torch.float16) for _ in range(num_layers)
            ]

            # Worker saves each layer during prefill forward pass
            for layer_idx, tensor in enumerate(layer_tensors):
                ok = self.connector.save_kv_layer(
                    layer_id=layer_idx,
                    block_hashes=block_hashes,
                    kv_data=tensor,
                )
                self.assertTrue(ok, f"save_kv_layer failed for layer {layer_idx}")

            # Worker loads each layer into pre-allocated memory
            for layer_idx, original_tensor in enumerate(layer_tensors):
                dest_tensor = torch.zeros_like(original_tensor)
                res = self.connector.load_kv_layer(
                    layer_id=layer_idx,
                    block_hashes=block_hashes,
                    target_location=dest_tensor,
                )
                self.assertIsNotNone(res, f"load_kv_layer returned None for layer {layer_idx}")
                self.assertTrue(
                    torch.equal(dest_tensor, original_tensor),
                    f"Tensor data mismatch on layer {layer_idx}",
                )
        else:
            # Fallback byte buffers test
            layer_payloads = [os.urandom(65536) for _ in range(num_layers)]
            for layer_idx, payload in enumerate(layer_payloads):
                ok = self.connector.save_kv_layer(
                    layer_id=layer_idx,
                    block_hashes=block_hashes,
                    kv_data=payload,
                )
                self.assertTrue(ok)

            for layer_idx, payload in enumerate(layer_payloads):
                got = self.connector.load_kv_layer(
                    layer_id=layer_idx,
                    block_hashes=block_hashes,
                )
                self.assertEqual(got, payload)

    def test_scheduler_worker_coordination_lifecycle(self):
        """Simulate vLLM scheduler matching prefix tokens and worker loading cached layers."""
        shared_prefix_tokens = list(range(30000, 30048))  # 48 tokens = 3 blocks
        user_prompt_a = shared_prefix_tokens + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        user_prompt_b = shared_prefix_tokens + [99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84]

        hashes_a = derive_vllm_block_hashes(user_prompt_a, block_size=16)
        hashes_b = derive_vllm_block_hashes(user_prompt_b, block_size=16)

        self.assertEqual(len(hashes_a), 4)
        self.assertEqual(len(hashes_b), 4)
        self.assertEqual(hashes_a[:3], hashes_b[:3])

        # Step 1: Prompt A arrives -> Cold cache (0 matched tokens)
        matched_a = self.connector.get_num_new_matched_tokens("req-vllm-a", user_prompt_a)
        self.assertEqual(matched_a, 0)

        # Step 2: Worker prefill forward pass executes and writes 4 layers of KV
        num_layers = 4
        if HAS_TORCH:
            tensors_a = [torch.randn(2, 4, 16, 4, 64, dtype=torch.float16) for _ in range(num_layers)]
            for layer_idx in range(num_layers):
                self.connector.save_kv_layer(layer_idx, hashes_a, tensors_a[layer_idx])
        else:
            tensors_a = [os.urandom(32768) for _ in range(num_layers)]
            for layer_idx in range(num_layers):
                self.connector.save_kv_layer(layer_idx, hashes_a, tensors_a[layer_idx])

        # Step 3: Request A completes
        self.connector.request_finished("req-vllm-a")

        # Step 4: Prompt B arrives -> Scheduler matches first 3 blocks (48 tokens)
        matched_b = self.connector.get_num_new_matched_tokens("req-vllm-b", user_prompt_b)
        self.assertEqual(matched_b, 48)

        # Step 5: Worker loads cached 3 blocks for all layers
        matched_hashes_b = hashes_b[:3]
        for layer_idx in range(num_layers):
            cached_data = self.connector.load_kv_layer(layer_idx, matched_hashes_b)
            self.assertIsNotNone(cached_data)


if __name__ == "__main__":
    unittest.main()
