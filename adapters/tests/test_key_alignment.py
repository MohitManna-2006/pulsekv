"""Unit test for SGLang block-hash key scheme alignment (Step 7.2).

Verifies that PulseKV's key derivation produces bit-exact SHA-256 hash chains
matching SGLang's RadixCache and HiCache storage backend expectations.
"""

import hashlib
import unittest
from typing import Optional, Sequence

from pulsekv_adapters.key import (
    derive_block_hashes,
    derive_prefix_keys,
    format_cache_key,
    get_hash_str,
    get_token_bytes,
)


def reference_sglang_get_hash_str(
    tokens: Sequence[int], prior_hash: Optional[str] = None
) -> str:
    """Reference implementation of SGLang's RadixKey.hash_page / get_hash_str."""
    hasher = hashlib.sha256()
    if prior_hash:
        hasher.update(prior_hash.encode("utf-8"))
    for t in tokens:
        hasher.update(int(t).to_bytes(4, byteorder="big", signed=True))
    return hasher.hexdigest()


class TestKeyAlignment(unittest.TestCase):
    def test_single_token_bytes(self):
        # 0 -> 0x00000000
        self.assertEqual(get_token_bytes(0), b"\x00\x00\x00\x00")
        # 1 -> 0x00000001
        self.assertEqual(get_token_bytes(1), b"\x00\x00\x00\x01")
        # 256 -> 0x00000100
        self.assertEqual(get_token_bytes(256), b"\x00\x00\x01\x00")
        # 128000 -> 0x0001f400
        self.assertEqual(get_token_bytes(128000), b"\x00\x01\xf4\x00")
        # -1 -> 0xffffffff
        self.assertEqual(get_token_bytes(-1), b"\xff\xff\xff\xff")

    def test_hash_str_matches_reference(self):
        token_samples = [
            [101, 2054, 2003, 1037, 7099, 102],
            [128000, 128001, 15, 8421, 99999],
            list(range(64)),
            [42],
        ]

        for tokens in token_samples:
            # Without prior_hash
            h1 = get_hash_str(tokens)
            ref1 = reference_sglang_get_hash_str(tokens)
            self.assertEqual(h1, ref1)
            self.assertEqual(len(h1), 64)

            # With prior_hash
            prior = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            h2 = get_hash_str(tokens, prior_hash=prior)
            ref2 = reference_sglang_get_hash_str(tokens, prior_hash=prior)
            self.assertEqual(h2, ref2)
            self.assertNotEqual(h1, h2)

    def test_chained_block_hashes(self):
        # 48 tokens, page_size=16 -> 3 blocks
        tokens = list(range(1000, 1048))
        hashes = derive_block_hashes(tokens, page_size=16)
        self.assertEqual(len(hashes), 3)

        # Block 0: hash(tokens[0..16], prior=None)
        expected_h0 = reference_sglang_get_hash_str(tokens[0:16], prior_hash=None)
        self.assertEqual(hashes[0], expected_h0)

        # Block 1: hash(tokens[16..32], prior=h0)
        expected_h1 = reference_sglang_get_hash_str(tokens[16:32], prior_hash=expected_h0)
        self.assertEqual(hashes[1], expected_h1)

        # Block 2: hash(tokens[32..48], prior=h1)
        expected_h2 = reference_sglang_get_hash_str(tokens[32:48], prior_hash=expected_h1)
        self.assertEqual(hashes[2], expected_h2)

    def test_prefix_sharing_produces_identical_keys(self):
        # Two requests sharing a 32-token prefix but diverging afterwards
        prefix = list(range(5000, 5032))
        prompt_a = prefix + [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]
        prompt_b = prefix + [99, 88, 77, 66, 55, 44, 33, 22, 11, 0, 1, 2, 3, 4, 5, 6]

        keys_a = derive_prefix_keys(prompt_a, page_size=16, pool_name="kv")
        keys_b = derive_prefix_keys(prompt_b, page_size=16, pool_name="kv")

        self.assertEqual(len(keys_a), 3)
        self.assertEqual(len(keys_b), 3)

        # Prefix blocks 0 and 1 MUST be identical across requests
        self.assertEqual(keys_a[0], keys_b[0])
        self.assertEqual(keys_a[1], keys_b[1])

        # Block 2 diverges
        self.assertNotEqual(keys_a[2], keys_b[2])

    def test_format_cache_key(self):
        h = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
        self.assertEqual(format_cache_key(h, pool_name="kv"), f"{h}.kv")
        self.assertEqual(format_cache_key(h, pool_name="mamba"), f"{h}.mamba")
        self.assertEqual(
            format_cache_key(h, pool_name="kv", model_name="meta-llama/Llama-3-8B"),
            f"sglang:meta-llama/Llama-3-8B:{h}.kv",
        )


if __name__ == "__main__":
    unittest.main()
