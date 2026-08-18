"""Unit and integration tests for PulseKVKVConnector adapter (Phase 8).
"""

import os
import unittest
from typing import Dict, List, Optional, Tuple

from pulsekv_adapters.client import PulseKVClient
from pulsekv_adapters.vllm import PulseKVKVConnector
from pulsekv_adapters.vllm_key import (
    derive_vllm_block_hashes,
    format_vllm_kv_key,
)


class MockPulseKVClient:
    """In-memory mock PulseKV client for deterministic offline testing."""

    def __init__(self) -> None:
        self.store: Dict[bytes, bytes] = {}

    def get(self, key: bytes | str) -> Tuple[Optional[bytes], bool]:
        k = key.encode("utf-8") if isinstance(key, str) else key
        if k in self.store:
            return self.store[k], True
        return None, False

    def exist(self, key: bytes | str) -> bool:
        k = key.encode("utf-8") if isinstance(key, str) else key
        return k in self.store

    def exists(self, key: bytes | str) -> bool:
        return self.exist(key)

    def set(self, key: bytes | str, value: bytes) -> bool:
        k = key.encode("utf-8") if isinstance(key, str) else key
        self.store[k] = bytes(value)
        return True

    def close(self) -> None:
        pass


class TestVLLMAdapterUnit(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_client = MockPulseKVClient()
        self.connector = PulseKVKVConnector(
            client=self.mock_client,
            model_name="meta-llama/Llama-3-8B",
            block_size=16,
            rank=0,
        )

    def tearDown(self) -> None:
        self.connector.close()

    def test_scheduler_matching_cold_cache(self) -> None:
        prompt_tokens = list(range(100, 164))  # 64 tokens = 4 blocks of 16
        matched = self.connector.get_num_new_matched_tokens("req-1", prompt_tokens)
        self.assertEqual(matched, 0)
        self.assertEqual(self.connector.get_matched_block_hashes(prompt_tokens), [])

    def test_scheduler_matching_prefix_progression(self) -> None:
        prompt_tokens = list(range(100, 164))  # 4 blocks: 0, 1, 2, 3
        hashes = derive_vllm_block_hashes(prompt_tokens, block_size=16)
        self.assertEqual(len(hashes), 4)

        # Populate block 0 and block 1 for layer 0
        self.connector.save_kv_block(layer_id=0, block_hash=hashes[0], data=b"kv_b0")
        self.connector.save_kv_block(layer_id=0, block_hash=hashes[1], data=b"kv_b1")

        # Now scheduler should match 2 blocks (32 tokens)
        matched = self.connector.get_num_new_matched_tokens("req-2", prompt_tokens)
        self.assertEqual(matched, 32)
        self.assertEqual(self.connector.get_matched_block_hashes(prompt_tokens), hashes[:2])

        # Add block 3 (leaving gap at block 2) -> still only matches first 2 blocks (prefix invariant)
        self.connector.save_kv_block(layer_id=0, block_hash=hashes[3], data=b"kv_b3")
        matched_gap = self.connector.get_num_new_matched_tokens("req-3", prompt_tokens)
        self.assertEqual(matched_gap, 32)

        # Fill block 2 -> now full 64 tokens matched
        self.connector.save_kv_block(layer_id=0, block_hash=hashes[2], data=b"kv_b2")
        matched_full = self.connector.get_num_new_matched_tokens("req-4", prompt_tokens)
        self.assertEqual(matched_full, 64)
        self.assertEqual(self.connector.get_matched_block_hashes(prompt_tokens), hashes)

    def test_request_lifecycle(self) -> None:
        prompt_tokens = list(range(32))
        self.connector.get_num_new_matched_tokens("req-finish-test", prompt_tokens)
        self.assertIn("req-finish-test", self.connector._active_requests)

        self.connector.request_finished("req-finish-test")
        self.assertNotIn("req-finish-test", self.connector._active_requests)

    def test_worker_layer_save_and_load(self) -> None:
        prompt_tokens = list(range(32))
        hashes = derive_vllm_block_hashes(prompt_tokens, block_size=16)
        self.assertEqual(len(hashes), 2)

        # Save layer 0 data (two 10-byte blocks)
        layer0_data = b"0123456789abcdefghij"
        ok = self.connector.save_kv_layer(layer_id=0, block_hashes=hashes, kv_data=layer0_data)
        self.assertTrue(ok)

        # Load layer 0 data
        loaded = self.connector.load_kv_layer(layer_id=0, block_hashes=hashes)
        self.assertEqual(loaded, layer0_data)

        # Load missing layer 1 -> returns None
        loaded_missing = self.connector.load_kv_layer(layer_id=1, block_hashes=hashes)
        self.assertIsNone(loaded_missing)

    def test_model_namespace_isolation(self) -> None:
        prompt_tokens = list(range(32))
        hashes = derive_vllm_block_hashes(prompt_tokens, block_size=16)

        # Connector for Model A
        connector_a = PulseKVKVConnector(
            client=self.mock_client,
            model_name="model-alpha",
            block_size=16,
        )
        # Connector for Model B
        connector_b = PulseKVKVConnector(
            client=self.mock_client,
            model_name="model-beta",
            block_size=16,
        )

        # Store for Model A
        connector_a.save_kv_block(layer_id=0, block_hash=hashes[0], data=b"data_alpha")

        # Model A should match 1 block (16 tokens)
        self.assertEqual(connector_a.get_num_new_matched_tokens("req-a", prompt_tokens), 16)
        # Model B must NOT match Model A's cache (0 tokens)
        self.assertEqual(connector_b.get_num_new_matched_tokens("req-b", prompt_tokens), 0)


class TestVLLMAdapterLive(unittest.TestCase):
    """Live integration tests against running PulseKV cluster (if available)."""

    def setUp(self) -> None:
        cp_addr = os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000")
        try:
            self.client = PulseKVClient(control_plane_addresses=cp_addr, timeout=2.0)
            self.connector = PulseKVKVConnector(
                client=self.client,
                model_name="test-live-model",
                block_size=16,
            )
            self.live = True
        except Exception:
            self.live = False

    def tearDown(self) -> None:
        if self.live:
            self.connector.close()

    def test_live_cluster_save_and_load(self) -> None:
        if not self.live:
            self.skipTest("Live PulseKV cluster not reachable")

        tokens = list(range(9000, 9032))  # 2 blocks of 16
        hashes = derive_vllm_block_hashes(tokens, block_size=16)

        test_data = b"X" * 1024  # 1 KB layer data
        ok = self.connector.save_kv_layer(layer_id=10, block_hashes=hashes, kv_data=test_data)
        self.assertTrue(ok)

        loaded = self.connector.load_kv_layer(layer_id=10, block_hashes=hashes)
        self.assertEqual(loaded, test_data)


if __name__ == "__main__":
    unittest.main()
