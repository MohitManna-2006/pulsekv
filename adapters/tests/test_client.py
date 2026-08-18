"""Unit and integration tests for PulseKVClient (Step 7.1)."""

import os
import unittest
from typing import Dict, List, Tuple

from pulsekv_adapters.client import (
    PulseKVClient,
    PulseKVClientError,
    TopologySnapshot,
    compute_topology_fingerprint,
    fnv1a_64,
    mix64,
    shard_for_key,
)


class TestClientUnit(unittest.TestCase):
    def test_fnv1a_64_known_vectors(self):
        # Empty string FNV-1a offset basis
        self.assertEqual(fnv1a_64(b""), 0xCBF29CE484222325)
        # Check non-zero deterministic hash
        self.assertIsInstance(fnv1a_64(b"hello"), int)
        self.assertEqual(fnv1a_64(b"hello"), fnv1a_64(b"hello"))
        self.assertNotEqual(fnv1a_64(b"hello"), fnv1a_64(b"world"))

    def test_mix64_avalanche(self):
        # Check splitmix64 properties
        m0 = mix64(0)
        m1 = mix64(1)
        self.assertNotEqual(m0, m1)
        self.assertEqual(mix64(42), mix64(42))

    def test_shard_for_key(self):
        self.assertEqual(shard_for_key(b"key1", 256), fnv1a_64(b"key1") % 256)
        self.assertTrue(0 <= shard_for_key(b"any_key", 256) < 256)

    def test_topology_fingerprint_matches(self):
        nodes = {"node-0": "127.0.0.1:7100", "node-1": "127.0.0.1:7200"}
        shard_map = {0: "node-0", 1: "node-1", 2: "node-0", 3: "node-1"}
        owners = {
            0: ("node-0", ["node-1"]),
            1: ("node-1", ["node-0"]),
            2: ("node-0", ["node-1"]),
            3: ("node-1", ["node-0"]),
        }
        fp1 = compute_topology_fingerprint(
            shard_count=4,
            replication_factor=1,
            nodes=nodes,
            shard_map=shard_map,
            owners=owners,
        )
        self.assertEqual(len(fp1), 32)
        fp2 = compute_topology_fingerprint(
            shard_count=4,
            replication_factor=1,
            nodes=nodes,
            shard_map=shard_map,
            owners=owners,
        )
        self.assertEqual(fp1, fp2)

        # Mutating a node changes the fingerprint
        nodes_mut = dict(nodes)
        nodes_mut["node-0"] = "127.0.0.1:7999"
        fp3 = compute_topology_fingerprint(
            shard_count=4,
            replication_factor=1,
            nodes=nodes_mut,
            shard_map=shard_map,
            owners=owners,
        )
        self.assertNotEqual(fp1, fp3)


class TestClientLiveCluster(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cp_addr = os.getenv("PULSEKV_CONTROL_PLANE_ADDRESS", "127.0.0.1:7000")
        try:
            cls.client = PulseKVClient(control_plane_addresses=cp_addr, refresh_interval=0)
            cls.has_cluster = True
        except Exception:
            cls.has_cluster = False

    @classmethod
    def tearDownClass(cls):
        if getattr(cls, "has_cluster", False) and cls.client:
            cls.client.close()

    def setUp(self):
        if not self.has_cluster:
            self.skipTest("No live PulseKV cluster available at PULSEKV_CONTROL_PLANE_ADDRESS")

    def test_unary_round_trip(self):
        key = b"test:client:unary:1"
        value = b"Hello from PulseKV Python Client SDK!" * 100
        ok = self.client.set(key, value)
        self.assertTrue(ok)

        self.assertTrue(self.client.exist(key))
        self.assertTrue(self.client.exists(key))

        got, found = self.client.get(key)
        self.assertTrue(found)
        self.assertEqual(got, value)

    def test_miss_returns_none(self):
        key = b"test:client:nonexistent:99999"
        self.assertFalse(self.client.exist(key))
        got, found = self.client.get(key)
        self.assertFalse(found)
        self.assertIsNone(got)

    def test_chunked_round_trip_large_blob(self):
        # 5 MiB payload > 4 MiB unary limit -> tests PutChunked and GetChunked
        key = b"test:client:chunked:5mb"
        large_val = os.urandom(5 * 1024 * 1024)

        ok = self.client.set(key, large_val)
        self.assertTrue(ok)

        got, found = self.client.get(key)
        self.assertTrue(found)
        self.assertIsNotNone(got)
        self.assertEqual(len(got), len(large_val))
        self.assertEqual(got, large_val)

    def test_prefix_match(self):
        prefix = b"test:client:prefix_scan:"
        k1 = prefix + b"item_1"
        k2 = prefix + b"item_2"
        v1 = b"val_1"
        v2 = b"val_2"

        self.client.set(k1, v1)
        self.client.set(k2, v2)

        matches = self.client.prefix_match(prefix)
        self.assertIn(k1, matches)
        self.assertIn(k2, matches)
        self.assertEqual(matches[k1], v1)
        self.assertEqual(matches[k2], v2)


if __name__ == "__main__":
    unittest.main()
