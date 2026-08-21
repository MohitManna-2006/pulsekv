"""Focused contract tests against the real SGLang 0.5.15 installation."""
import unittest
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import torch
from sglang.srt.mem_cache.hicache_storage import (
    HiCacheStorage, HiCacheStorageConfig, PoolHitPolicy, PoolName, PoolTransfer,
    PoolTransferResult,
)
from sglang.srt.mem_cache.storage.backend_factory import StorageBackendFactory

from pulsekv_adapters.sglang import PulseKVHiCacheStorage


class MockClient:
    def __init__(self):
        self.store = {}
        self.keys = []
    def set(self, key, value):
        self.keys.append(("set", key)); self.store[key] = bytes(value); return True
    def get(self, key):
        self.keys.append(("get", key)); return (self.store[key], True) if key in self.store else (None, False)
    def exist(self, key):
        self.keys.append(("exist", key)); return key in self.store
    def close(self): pass


class FakePool:
    page_size = 1
    def __init__(self, pages=4, shape=(2, 3), dtype=torch.float16, device="cpu"):
        self.data = [torch.zeros(shape, dtype=dtype, device=device) for _ in range(pages)]
    def get_data_page(self, offset, flat=True): return self.data[offset].clone()
    def get_dummy_flat_data_page(self): return torch.empty_like(self.data[0])
    def set_from_flat_data_page(self, offset, page): self.data[offset].copy_(page)


def config(extra=None):
    return HiCacheStorageConfig(0, 1, 0, 1, 0, 1, False, False, True,
                                "Qwen/Qwen2.5-1.5B-Instruct", extra_config=extra)


class TestSGLang0515Contract(unittest.TestCase):
    def setUp(self):
        self.client = MockClient()
        self.storage = PulseKVHiCacheStorage(client=self.client)

    def test_real_inheritance_and_dynamic_factory(self):
        self.assertTrue(issubclass(PulseKVHiCacheStorage, HiCacheStorage))
        extra = {"backend_name": "pulsekv", "module_path": "pulsekv_adapters.sglang",
                 "class_name": "PulseKVHiCacheStorage",
                 "control_plane_address": "10.0.0.1:7000"}
        with patch("pulsekv_adapters.sglang.PulseKVClient", return_value=self.client):
            backend = StorageBackendFactory.create_backend("dynamic", config(extra), object())
        self.assertIsInstance(backend, PulseKVHiCacheStorage)
        self.assertEqual(backend.control_plane_address, "10.0.0.1:7000")

    def test_constructor_factory_kwargs_override(self):
        backend = PulseKVHiCacheStorage(config(), {"control_plane_address": "x:1", "client": self.client})
        self.assertEqual(backend.control_plane_address, "x:1")

    def test_legacy_batches_and_opaque_keys(self):
        keys = ["opaque/server/key:0", "opaque/server/key:1", "opaque/server/key:2"]
        self.assertTrue(self.storage.batch_set(keys[:2], [b"a", b"b"]))
        self.assertEqual(self.storage.batch_exists(keys), 2)
        self.assertEqual(self.storage.batch_get(keys), [b"a", b"b", None])
        touched = [key for _, key in self.client.keys]
        self.assertTrue(all(key in keys for key in touched))

    def test_cpu_tensor_round_trips_and_mismatch(self):
        for dtype in (torch.uint8, torch.float16, torch.bfloat16):
            src = torch.arange(12, dtype=torch.float32).reshape(3, 4).to(dtype)
            key = f"tensor-{dtype}"
            self.assertTrue(self.storage.set(key, src, target_sizes=src.numel() * src.element_size()))
            dst = torch.empty_like(src)
            self.assertIs(self.storage.get(key, dst, dst.numel() * dst.element_size()), dst)
            self.assertTrue(torch.equal(src, dst))
        with self.assertRaises(ValueError): self.storage.get(key, torch.empty(1, dtype=torch.uint8))
        with self.assertRaises(ValueError): self.storage.set("bad", src, target_sizes=1)

    @unittest.skipUnless(torch.cuda.is_available(), "CUDA unavailable")
    def test_cuda_tensor_round_trips(self):
        for dtype in (torch.float16, torch.bfloat16):
            src = torch.arange(12, device="cuda", dtype=torch.float32).reshape(3, 4).to(dtype)
            self.assertTrue(self.storage.set(f"cuda-{dtype}", src))
            dst = torch.empty_like(src)
            self.storage.get(f"cuda-{dtype}", dst)
            self.assertTrue(torch.equal(src, dst))

    def test_v1_io(self):
        pool = FakePool(); pool.data[1].fill_(7)
        self.storage.register_mem_pool_host(pool)
        self.assertEqual(self.storage.batch_set_v1(["v1"], torch.tensor([1])), [True])
        pool.data[2].zero_()
        self.assertEqual(self.storage.batch_get_v1(["v1"], torch.tensor([2])), [True])
        self.assertTrue(torch.equal(pool.data[1], pool.data[2]))

    def test_v2_io_and_result_update(self):
        pool = FakePool(); pool.data[0].fill_(9)
        self.storage.register_mem_host_pool_v2(pool, PoolName.KV)
        put = PoolTransfer(PoolName.KV, host_indices=torch.tensor([0]), keys=["v2"])
        self.assertEqual(self.storage.batch_set_v2([put]), {PoolName.KV: [True]})
        pool.data[1].zero_(); get = PoolTransfer(PoolName.KV, host_indices=torch.tensor([1]), keys=["v2"])
        self.assertEqual(self.storage.batch_get_v2([get]), {PoolName.KV: [True]})
        self.assertTrue(torch.equal(pool.data[0], pool.data[1]))
        result = PoolTransferResult.empty(); result.update_kv_hit_pages(2)
        result.update_extra_pool_hit_pages({PoolName.SWA: [True, False]})
        self.assertEqual((result.kv_hit_pages, result.extra_pool_hit_pages[PoolName.SWA]), (2, 1))

    def test_v2_hit_policies(self):
        keys = [f"k{i}" for i in range(4)]
        self.storage.batch_set(keys, [b"x"] * 4)
        for i in (0, 1, 3): self.storage.set(f"{keys[i]}.{PoolName.SWA}", b"s")
        all_pages = PoolTransfer(PoolName.SWA, keys=["marker"], hit_policy=PoolHitPolicy.ALL_PAGES)
        result = self.storage.batch_exists_v2(keys, [all_pages])
        self.assertEqual((result.kv_hit_pages, result.extra_pool_hit_pages[PoolName.SWA]), (2, 2))
        trailing = PoolTransfer(PoolName.SWA, keys=["marker"], hit_policy=PoolHitPolicy.TRAILING_PAGES)
        result = self.storage.batch_exists_v2(keys, [trailing])
        self.assertEqual((result.kv_hit_pages, result.extra_pool_hit_pages[PoolName.SWA]), (4, 4))

    def test_generated_protobuf_imports(self):
        from importlib.metadata import version
        from pulsekv_adapters.gen import metadata_pb2, node_pb2
        self.assertEqual(version("grpcio"), "1.83.0")
        self.assertEqual(version("protobuf"), "7.35.1")
        self.assertIsNotNone(metadata_pb2); self.assertIsNotNone(node_pb2)

    def test_environment_gated_jsonl_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "trace.jsonl")
            with patch.dict(os.environ, {"PULSEKV_SGLANG_TRACE_PATH": path,
                                         "PULSEKV_SGLANG_REPLICA": "test"}):
                self.assertTrue(self.storage.set("opaque-trace-key", b"value"))
                self.assertEqual(self.storage.batch_exists(["opaque-trace-key"]), 1)
                self.assertEqual(self.storage.get("opaque-trace-key"), b"value")
            with open(path, encoding="utf-8") as stream:
                records = [json.loads(line) for line in stream]
        self.assertEqual([r["operation"] for r in records],
                         ["set", "exists", "batch_exists", "get"])
        self.assertTrue(all(r["replica"] == "test" for r in records))
        self.assertTrue(all(r["keys"] == ["opaque-trace-key"] for r in records))

    def test_trace_write_failure_logs_once_and_does_not_break_storage(self):
        import pulsekv_adapters.sglang as adapter

        path = "/unwritable/phase10-6-trace.jsonl"
        adapter._TRACE_FAILED_PATHS.discard(path)
        with (
            patch.dict(os.environ, {"PULSEKV_SGLANG_TRACE_PATH": path}),
            patch("builtins.open", side_effect=OSError("read-only filesystem")),
            patch.object(adapter.logger, "error") as error,
        ):
            self.assertTrue(self.storage.set("trace-failure-1", b"one"))
            self.assertTrue(self.storage.set("trace-failure-2", b"two"))
        error.assert_called_once()

    def test_import_and_v2_fallback_without_sglang(self):
        code = r'''
import importlib.abc
import sys

class BlockSGLang(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "sglang" or fullname.startswith("sglang."):
            raise ImportError("blocked for fallback test")
        return None

sys.meta_path.insert(0, BlockSGLang())

from pulsekv_adapters.sglang import (
    HAS_SGLANG,
    PoolHitPolicy,
    PoolName,
    PoolTransfer,
    PoolTransferResult,
    PulseKVHiCacheStorage,
)

class Client:
    def __init__(self): self.values = {}
    def set(self, key, value): self.values[key] = bytes(value); return True
    def get(self, key): return (self.values[key], True) if key in self.values else (None, False)
    def exist(self, key): return key in self.values
    def close(self): pass

assert HAS_SGLANG is False
storage = PulseKVHiCacheStorage(client=Client())
assert storage.set("opaque", b"value")
assert storage.get("opaque") == b"value"
assert storage.batch_exists(["opaque", "missing"]) == 1
transfer = PoolTransfer(PoolName.SWA, keys=["marker"], hit_policy=PoolHitPolicy.ALL_PAGES)
result = storage.batch_exists_v2(["opaque"], [transfer])
assert isinstance(result, PoolTransferResult)
assert result.kv_hit_pages == 0
assert PoolTransferResult.empty().kv_hit_pages == 0
'''
        completed = subprocess.run(
            [sys.executable, "-c", code],
            check=False,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
            },
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__": unittest.main()
