"""Unit and integration tests for PulseKVHiCacheStorage adapter (Step 7.1)."""

import os
import unittest
from typing import Dict, List, Optional, Tuple

from pulsekv_adapters.client import PulseKVClient
from pulsekv_adapters.key import derive_prefix_keys
from pulsekv_adapters.sglang import PulseKVHiCacheStorage


class MockClient:
    """In-memory mock PulseKV client for fast offline unit testing of storage interface."""

    def __init__(self):
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


class TestSGLangAdapterUnit(unittest.TestCase):
    def setUp(self):
        self.mock_client = MockClient()
        self.storage = PulseKVHiCacheStorage(client=self.mock_client)

    def test_three_method_interface(self):
        key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef.kv"
        val = b"tensor_kv_page_data_12345"

        # 1. exist before set
        self.assertFalse(self.storage.exist(key))
        self.assertFalse(self.storage.exists(key))
        self.assertIsNone(self.storage.get(key))

        # 2. set
        ok = self.storage.set(key, val)
        self.assertTrue(ok)

        # 3. exist after set
        self.assertTrue(self.storage.exist(key))
        self.assertTrue(self.storage.exists(key))

        # 4. get
        got = self.storage.get(key)
        self.assertEqual(got, val)

    def test_batch_exists_v2_longest_prefix(self):
        # Generate 4 prefix keys
        token_ids = list(range(100, 164))  # 64 tokens, 4 blocks of 16
        keys = derive_prefix_keys(token_ids, page_size=16, pool_name="kv")
        self.assertEqual(len(keys), 4)

        # Initially 0 hit pages
        res0 = self.storage.batch_exists_v2(keys)
        self.assertEqual(res0.kv_hit_pages, 0)

        # Store block 0 and block 1
        self.storage.set(keys[0], b"block0")
        self.storage.set(keys[1], b"block1")

        res2 = self.storage.batch_exists_v2(keys)
        self.assertEqual(res2.kv_hit_pages, 2)

        # Store block 3 (gap at block 2) -> still hit pages = 2 (prefix rule)
        self.storage.set(keys[3], b"block3")
        res2_gap = self.storage.batch_exists_v2(keys)
        self.assertEqual(res2_gap.kv_hit_pages, 2)

        # Store block 2 -> now full hit = 4
        self.storage.set(keys[2], b"block2")
        res4 = self.storage.batch_exists_v2(keys)
        self.assertEqual(res4.kv_hit_pages, 4)

    def test_batch_get_and_batch_set(self):
        kvs = {
            "key_a.kv": b"data_a",
            "key_b.kv": b"data_b",
            "key_c.kv": b"data_c",
        }
        res_set = self.storage.batch_set(kvs)
        self.assertEqual(res_set, [True, True, True])

        exists = self.storage.batch_exists(["key_a.kv", "key_b.kv", "key_c.kv", "key_missing.kv"])
        self.assertEqual(exists, [True, True, True, False])

        vals = self.storage.batch_get(["key_a.kv", "key_b.kv", "key_c.kv", "key_missing.kv"])
        self.assertEqual(vals, [b"data_a", b"data_b", b"data_c", None])


class TestSGLangAdapterLiveCluster(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cp_addr = os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000")
        try:
            cls.storage = PulseKVHiCacheStorage(control_plane_address=cp_addr)
            # Test a quick ping to see if cluster is alive
            cls.storage.exist("probe_key")
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

    def test_live_storage_operations(self):
        token_ids = list(range(2000, 2048))  # 48 tokens -> 3 pages of 16
        keys = derive_prefix_keys(token_ids, page_size=16, pool_name="kv")
        
        # Write 3 KV pages
        for i, k in enumerate(keys):
            page_data = f"live_cluster_kv_page_{i}_{os.urandom(16).hex()}".encode("utf-8")
            ok = self.storage.set(k, page_data)
            self.assertTrue(ok)
            self.assertTrue(self.storage.exist(k))
            self.assertEqual(self.storage.get(k), page_data)

        # Batch exists check
        exists = self.storage.batch_exists(keys)
        self.assertEqual(exists, [True, True, True])

        # Batch v2 prefix hit
        v2_res = self.storage.batch_exists_v2(keys)
        self.assertEqual(v2_res.kv_hit_pages, 3)


if __name__ == "__main__":
    unittest.main()
