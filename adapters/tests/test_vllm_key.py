"""Unit tests for vLLM block-hash key derivation and layer key formatting.
"""

import hashlib
import unittest
from typing import Optional, Sequence

from pulsekv_adapters.vllm_key import (
    derive_vllm_block_hashes,
    derive_vllm_layer_keys,
    format_vllm_kv_key,
    get_block_hash_str,
    get_token_bytes,
    parse_vllm_kv_key,
)


def reference_block_hash(
    tokens: Sequence[int], prior_hash: Optional[str] = None
) -> str:
    """Reference implementation of chained SHA-256 block hash."""
    hasher = hashlib.sha256()
    if prior_hash:
        hasher.update(prior_hash.encode("utf-8"))
    for t in tokens:
        hasher.update(int(t).to_bytes(4, byteorder="big", signed=True))
    return hasher.hexdigest()


class TestVLLMKey(unittest.TestCase):
    def test_token_bytes(self):
        self.assertEqual(get_token_bytes(0), b"\x00\x00\x00\x00")
        self.assertEqual(get_token_bytes(1), b"\x00\x00\x00\x01")
        self.assertEqual(get_token_bytes(255), b"\x00\x00\x00\xff")
        self.assertEqual(get_token_bytes(65536), b"\x00\x01\x00\x00")
        self.assertEqual(get_token_bytes(-1), b"\xff\xff\xff\xff")
        self.assertEqual(get_token_bytes((1, 2)), b"\x00\x00\x00\x01\x00\x00\x00\x02")

    def test_block_hash_matches_reference(self):
        samples = [
            [101, 2054, 2003, 1037, 7099, 102],
            list(range(16)),
            list(range(32)),
            [128000, 128001, 15, 8421],
        ]
        for tokens in samples:
            h = get_block_hash_str(tokens)
            ref = reference_block_hash(tokens)
            self.assertEqual(h, ref)
            self.assertEqual(len(h), 64)

            # Test with prior hash
            prior = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
            h_chained = get_block_hash_str(tokens, prior_hash=prior)
            ref_chained = reference_block_hash(tokens, prior_hash=prior)
            self.assertEqual(h_chained, ref_chained)
            self.assertNotEqual(h, h_chained)

    def test_derive_block_hashes_chaining(self):
        # 48 tokens, block_size=16 -> 3 blocks
        tokens = list(range(100, 148))
        hashes = derive_vllm_block_hashes(tokens, block_size=16)
        self.assertEqual(len(hashes), 3)

        # Block 0
        exp0 = reference_block_hash(tokens[0:16], prior_hash=None)
        self.assertEqual(hashes[0], exp0)

        # Block 1
        exp1 = reference_block_hash(tokens[16:32], prior_hash=exp0)
        self.assertEqual(hashes[1], exp1)

        # Block 2
        exp2 = reference_block_hash(tokens[32:48], prior_hash=exp1)
        self.assertEqual(hashes[2], exp2)

    def test_format_and_parse_vllm_kv_key(self):
        h = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

        # Default: vllm:layer_X:hash
        k1 = format_vllm_kv_key(h, layer_id=0)
        self.assertEqual(k1, f"vllm:layer_0:{h}")
        p1 = parse_vllm_kv_key(k1)
        self.assertIsNotNone(p1)
        self.assertIsNone(p1["model_name"])
        self.assertEqual(p1["layer_id"], 0)
        self.assertEqual(p1["block_hash"], h)
        self.assertIsNone(p1["tag"])

        # With model_name: vllm:llama3:layer_15:hash
        k2 = format_vllm_kv_key(h, layer_id=15, model_name="llama-3-8b")
        self.assertEqual(k2, f"vllm:llama-3-8b:layer_15:{h}")
        p2 = parse_vllm_kv_key(k2)
        self.assertIsNotNone(p2)
        self.assertEqual(p2["model_name"], "llama-3-8b")
        self.assertEqual(p2["layer_id"], 15)
        self.assertEqual(p2["block_hash"], h)

        # With tag: vllm:llama3:layer_3:hash:k
        k3 = format_vllm_kv_key(h, layer_id=3, model_name="llama-3", tag="k")
        self.assertEqual(k3, f"vllm:llama-3:layer_3:{h}:k")
        p3 = parse_vllm_kv_key(k3)
        self.assertIsNotNone(p3)
        self.assertEqual(p3["tag"], "k")

    def test_derive_vllm_layer_keys(self):
        tokens = list(range(32))  # 2 blocks of 16
        keys_l0 = derive_vllm_layer_keys(tokens, layer_id=0, block_size=16)
        keys_l1 = derive_vllm_layer_keys(tokens, layer_id=1, block_size=16)

        self.assertEqual(len(keys_l0), 2)
        self.assertEqual(len(keys_l1), 2)
        self.assertTrue(keys_l0[0].startswith("vllm:layer_0:"))
        self.assertTrue(keys_l1[0].startswith("vllm:layer_1:"))

        # Prefix sharing: identical block hashes for identical prefixes
        p1 = list(range(500, 532)) + [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        p2 = list(range(500, 532)) + [99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 87, 86, 85, 84]

        k_p1 = derive_vllm_layer_keys(p1, layer_id=5, block_size=16)
        k_p2 = derive_vllm_layer_keys(p2, layer_id=5, block_size=16)

        self.assertEqual(len(k_p1), 3)
        self.assertEqual(len(k_p2), 3)
        self.assertEqual(k_p1[0], k_p2[0])
        self.assertEqual(k_p1[1], k_p2[1])
        self.assertNotEqual(k_p1[2], k_p2[2])


if __name__ == "__main__":
    unittest.main()
