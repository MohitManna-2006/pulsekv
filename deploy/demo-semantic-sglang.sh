#!/usr/bin/env bash
# Phase 10.6B: real semantic Gateway A -> SGLang A -> PulseKV -> SGLang B proof.
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${PULSEKV_PHASE10_6_LOG_DIR:-/workspace/phase10_6_logs}"
SGLANG_VENV="${PULSEKV_SGLANG_VENV:-/workspace/venvs/pulsekv-sglang-0.5.15}"
GATEWAY_VENV="${PULSEKV_GATEWAY_VENV:-/workspace/venvs/pulsekv-gateway-phase10_6}"
MODEL="Qwen/Qwen2.5-1.5B-Instruct"
REVISION="989aa7980e4cf806f80c7fef2b1adb7bc71aa306"
PIDS=()

cleanup() {
    rc=$?
    echo "===== CLEANUP ====="
    for ((i=${#PIDS[@]}-1; i>=0; i--)); do
        pid="${PIDS[$i]}"
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" || true
            wait "$pid" || true
        fi
    done
    "$REPO_ROOT/deploy/stop-local-cluster.sh" || true
    echo "CLEANUP_COMPLETE=1"
    exit "$rc"
}
trap cleanup EXIT

for executable in "$SGLANG_VENV/bin/python" "$GATEWAY_VENV/bin/python" curl nvidia-smi; do
    command -v "$executable" >/dev/null 2>&1 || { echo "missing prerequisite: $executable"; exit 1; }
done
mkdir -p "$LOG_DIR"
export PATH="$SGLANG_VENV/bin:$GATEWAY_VENV/bin:/usr/local/go/bin:$PATH" GOTOOLCHAIN=local
export PYTHONPATH="$REPO_ROOT/adapters"
export HF_HOME=/workspace/.cache/huggingface XDG_CACHE_HOME=/workspace/.cache
export CUDA_VISIBLE_DEVICES=0

command -v ninja >/dev/null 2>&1 || {
    echo "missing prerequisite: ninja from the selected SGLang environment"
    exit 1
}
EXPECTED_MODEL="$MODEL" EXPECTED_REVISION="$REVISION" "$SGLANG_VENV/bin/python" - <<'PY'
import os
from importlib.metadata import version

import torch

if version("sglang") != "0.5.15":
    raise SystemExit(f"expected sglang 0.5.15, found {version('sglang')}")
if not torch.cuda.is_available() or torch.cuda.device_count() < 1:
    raise SystemExit("the selected SGLang environment has no visible CUDA GPU")
if os.environ["EXPECTED_MODEL"] != "Qwen/Qwen2.5-1.5B-Instruct":
    raise SystemExit("unexpected Phase 10.6 model identifier")
revision = os.environ["EXPECTED_REVISION"]
if revision != "989aa7980e4cf806f80c7fef2b1adb7bc71aa306" or len(revision) != 40:
    raise SystemExit("unexpected or mutable Phase 10.6 model revision")
print(
    "environment ok:",
    f"sglang={version('sglang')}",
    f"gpu={torch.cuda.get_device_name(0)}",
    f"model={os.environ['EXPECTED_MODEL']}",
    f"revision={revision}",
)
PY

TRACE_A="$LOG_DIR/semantic-adapter-a.jsonl"
TRACE_B="$LOG_DIR/semantic-adapter-b.jsonl"
: >"$TRACE_A"; : >"$TRACE_B"

"$REPO_ROOT/deploy/run-local-cluster.sh"

EXTRA='{"backend_name":"pulsekv","module_path":"pulsekv_adapters.sglang","class_name":"PulseKVHiCacheStorage","control_plane_address":"127.0.0.1:7000","interface_v1":1,"prefetch_threshold":1}'
SGLANG_COMMON=(
    -m sglang.launch_server --model-path "$MODEL" --revision "$REVISION"
    --host 127.0.0.1 --mem-fraction-static 0.30 --context-length 2048
    --max-running-requests 2 --cuda-graph-backend-decode disabled
    --cuda-graph-backend-prefill disabled --enable-hierarchical-cache
    --hicache-size 1 --hicache-write-policy write_through
    --hicache-storage-backend dynamic --hicache-storage-prefetch-policy wait_complete
    --hicache-storage-backend-extra-config "$EXTRA"
)

wait_ready() {
    label=$1; pid=$2; url=$3; log=$4
    for _ in $(seq 1 120); do
        if ! kill -0 "$pid" 2>/dev/null; then tail -120 "$log"; return 1; fi
        if curl -fsS "$url" >/dev/null 2>&1; then echo "$label ready"; return 0; fi
        sleep 5
    done
    return 1
}

PULSEKV_SGLANG_REPLICA=A PULSEKV_SGLANG_TRACE_PATH="$TRACE_A" \
    "$SGLANG_VENV/bin/python" "${SGLANG_COMMON[@]}" --port 30000 \
    >"$LOG_DIR/semantic-sglang-a.log" 2>&1 &
SGLANG_A_PID=$!; PIDS+=("$SGLANG_A_PID")
wait_ready "SGLang A" "$SGLANG_A_PID" http://127.0.0.1:30000/health "$LOG_DIR/semantic-sglang-a.log"

PULSEKV_SGLANG_REPLICA=B PULSEKV_SGLANG_TRACE_PATH="$TRACE_B" \
    "$SGLANG_VENV/bin/python" "${SGLANG_COMMON[@]}" --port 30001 \
    >"$LOG_DIR/semantic-sglang-b.log" 2>&1 &
SGLANG_B_PID=$!; PIDS+=("$SGLANG_B_PID")
wait_ready "SGLang B" "$SGLANG_B_PID" http://127.0.0.1:30001/health "$LOG_DIR/semantic-sglang-b.log"

DEMO_DIR="$LOG_DIR/semantic-demo"
mkdir -p "$DEMO_DIR"
DEMO_DIR="$DEMO_DIR" "$GATEWAY_VENV/bin/python" - <<'PY'
import json, os
from datetime import datetime, timezone
from pulsekv_gateway.models import BlockType, CanonicalContextRecord
from pulsekv_gateway.normalizer import hash_normalized
from pulsekv_gateway.registry import Registry

d=os.environ['DEMO_DIR']
canonical=("Phase ten six canonical policy requires preserving every production record and validating each operation before execution. "*60)
variant=("For phase 10.6, keep all production data intact and verify every action prior to carrying it out. "*60)
registry = os.path.join(d, f"registry-{os.getpid()}.db")
r=Registry.from_dsn(registry, hash_text=hash_normalized)
r.register(CanonicalContextRecord(context_id='semantic-policy',version=1,namespace='phase10-6',canonical_text=canonical,content_hash=hash_normalized(canonical),block_type=BlockType.SYSTEM_PROMPT,aliases=(variant,),created_at=datetime.now(timezone.utc),created_by='phase-10.6-demo'))
r.close()
for label,port,upstream in [('a',8088,30000),('b',8089,30001)]:
 open(os.path.join(d,f'gateway-{label}.yaml'),'w').write(f'''enabled: true
listen_host: 127.0.0.1
listen_port: {port}
upstream_url: http://127.0.0.1:{upstream}
registry_dsn: {registry}
namespace_source: static
static_namespace: phase10-6
bypass_min_eligible_tokens: 0
request_timeout_ms: 120000
''')
for label,text in [('a',canonical),('b',variant)]:
 json.dump({'model':'Qwen/Qwen2.5-1.5B-Instruct','messages':[{'role':'system','content':text},{'role':'user','content':'Reply with a brief confirmation.'}],'temperature':0,'max_tokens':8},open(os.path.join(d,f'request-{label}.json'),'w'))
open(os.path.join(d,'canonical.txt'),'w').write(canonical)
open(os.path.join(d,'variant.txt'),'w').write(variant)
print('RAW_VARIANTS_EQUAL=',canonical==variant)
PY

cat >"$DEMO_DIR/gateway_runner.py" <<'PY'
import argparse
import asyncio

import httpx
import uvicorn

from pulsekv_gateway.config import load
from pulsekv_gateway.server import create_app


class CapturingTransport(httpx.AsyncBaseTransport):
    def __init__(self, capture_path: str) -> None:
        self.capture_path = capture_path
        self.inner = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        with open(self.capture_path, "wb") as stream:
            stream.write(body)
        forwarded = httpx.Request(
            request.method,
            request.url,
            headers=request.headers,
            content=body,
        )
        return await self.inner.handle_async_request(forwarded)

    async def aclose(self) -> None:
        await self.inner.aclose()


parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True)
parser.add_argument("--capture", required=True)
args = parser.parse_args()
config = load(args.config)
client = httpx.AsyncClient(
    transport=CapturingTransport(args.capture),
    timeout=httpx.Timeout(config.request_timeout_ms / 1000),
)
app = create_app(config, http_client=client)
uvicorn.run(app, host=config.listen_host, port=config.listen_port, workers=1)
PY

"$GATEWAY_VENV/bin/python" "$DEMO_DIR/gateway_runner.py" \
    --config "$DEMO_DIR/gateway-a.yaml" --capture "$DEMO_DIR/outbound-a.json" \
    >"$LOG_DIR/gateway-a.log" 2>&1 &
GATEWAY_A_PID=$!; PIDS+=("$GATEWAY_A_PID")
wait_ready "Gateway A" "$GATEWAY_A_PID" http://127.0.0.1:8088/readyz "$LOG_DIR/gateway-a.log"
"$GATEWAY_VENV/bin/python" "$DEMO_DIR/gateway_runner.py" \
    --config "$DEMO_DIR/gateway-b.yaml" --capture "$DEMO_DIR/outbound-b.json" \
    >"$LOG_DIR/gateway-b.log" 2>&1 &
GATEWAY_B_PID=$!; PIDS+=("$GATEWAY_B_PID")
wait_ready "Gateway B" "$GATEWAY_B_PID" http://127.0.0.1:8089/readyz "$LOG_DIR/gateway-b.log"

RESP_A=$(curl -fsS -H 'Content-Type: application/json' --data-binary @"$DEMO_DIR/request-a.json" http://127.0.0.1:8088/v1/chat/completions)
echo "GATEWAY_A_RESPONSE=$RESP_A"
sleep 6
RESP_B=$(curl -fsS -H 'Content-Type: application/json' --data-binary @"$DEMO_DIR/request-b.json" http://127.0.0.1:8089/v1/chat/completions)
echo "GATEWAY_B_RESPONSE=$RESP_B"
sleep 3

DEMO_DIR="$DEMO_DIR" TRACE_A="$TRACE_A" TRACE_B="$TRACE_B" SGLANG_VENV="$SGLANG_VENV" "$SGLANG_VENV/bin/python" - <<'PY'
import hashlib
import json
import os

from transformers import AutoTokenizer

demo_dir = os.environ["DEMO_DIR"]
raw_a = open(os.path.join(demo_dir, "canonical.txt"), encoding="utf-8").read()
raw_b = open(os.path.join(demo_dir, "variant.txt"), encoding="utf-8").read()
outbound_a = json.load(open(os.path.join(demo_dir, "outbound-a.json"), encoding="utf-8"))
outbound_b = json.load(open(os.path.join(demo_dir, "outbound-b.json"), encoding="utf-8"))
canonical_a = outbound_a["messages"][0]["content"]
canonical_b = outbound_b["messages"][0]["content"]

tokenizer = AutoTokenizer.from_pretrained(
    "Qwen/Qwen2.5-1.5B-Instruct",
    revision="989aa7980e4cf806f80c7fef2b1adb7bc71aa306",
)
tokens_a = tokenizer.encode(canonical_a)
tokens_b = tokenizer.encode(canonical_b)
sha_a = hashlib.sha256(json.dumps(tokens_a).encode()).hexdigest()
sha_b = hashlib.sha256(json.dumps(tokens_b).encode()).hexdigest()

def records(path):
    with open(path, encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]

def successful_read_keys(entries):
    successful = set()
    success_count = 0
    failure_count = 0
    successful_operations = 0
    batch_operations = {"batch_get", "batch_get_v1", "batch_get_v2"}
    batch_entries = [
        entry for entry in entries if entry.get("operation") in batch_operations
    ]
    read_entries = batch_entries or [
        entry for entry in entries if entry.get("operation") == "get"
    ]
    for entry in read_entries:
        operation = entry["operation"]
        if operation == "get":
            ok = entry.get("result") == "hit"
            booleans = [ok]
        elif operation in ("batch_get", "batch_get_v1"):
            booleans = entry.get("result")
            if not isinstance(booleans, list) or len(booleans) != len(entry["keys"]):
                raise ValueError(f"malformed {operation} evidence")
            if any(type(value) is not bool for value in booleans):
                raise ValueError(f"non-boolean {operation} evidence")
        elif operation == "batch_get_v2":
            result = entry.get("result")
            if not isinstance(result, dict):
                raise ValueError("malformed batch_get_v2 evidence")
            booleans = [value for values in result.values() for value in values]
            if len(booleans) != len(entry["keys"]) or any(
                type(value) is not bool for value in booleans
            ):
                raise ValueError("malformed batch_get_v2 key/result lengths")
        else:
            continue
        successes = sum(booleans)
        success_count += successes
        failure_count += len(booleans) - successes
        if successes:
            successful_operations += 1
        successful.update(
            key for key, succeeded in zip(entry["keys"], booleans) if succeeded
        )
    return successful, success_count, failure_count, successful_operations

a = records(os.environ["TRACE_A"])
b = records(os.environ["TRACE_B"])
a_written = {
    entry["keys"][0]
    for entry in a
    if entry["operation"] == "set"
    and entry.get("result") is True
    and len(entry["keys"]) == 1
}
b_queried = {
    key
    for entry in b
    if entry["operation"] in ("exists", "batch_exists", "batch_exists_v2")
    for key in entry["keys"]
}
b_read, read_successes, read_failures, successful_read_operations = (
    successful_read_keys(b)
)
intersection = a_written & b_read

raw_differ = raw_a != raw_b
canonical_equal = canonical_a == canonical_b
tokens_equal = tokens_a == tokens_b
hashes_equal = sha_a == sha_b
print("RAW_VARIANTS_DIFFER=", raw_differ)
print("CANONICAL_EQUAL=", canonical_equal)
print("CANONICAL_TOKEN_IDS_EQUAL=", tokens_equal)
print("CANONICAL_TOKEN_SHA_A=", sha_a)
print("CANONICAL_TOKEN_SHA_B=", sha_b)
print("TOKEN_HASHES_EQUAL=", hashes_equal)
print("A_WRITTEN_KEYS=", len(a_written))
print("B_QUERIED_KEYS=", len(b_queried))
print("B_READ_KEYS=", len(b_read))
print("B_READ_SUCCESS_COUNT=", read_successes)
print("B_READ_FAILURE_COUNT=", read_failures)
print("KEY_INTERSECTION_COUNT=", len(intersection))
print("EXAMPLE_SHARED_KEY=", sorted(intersection)[0] if intersection else "")

if not all(
    (
        raw_differ,
        canonical_equal,
        tokens_equal,
        hashes_equal,
        successful_read_operations > 0,
        read_successes > 0,
        len(b_read) > 0,
        len(intersection) > 0,
    )
):
    raise SystemExit(1)
PY

echo "SGLANG_A_PID=$SGLANG_A_PID SGLANG_B_PID=$SGLANG_B_PID GATEWAY_A_PID=$GATEWAY_A_PID GATEWAY_B_PID=$GATEWAY_B_PID"
nvidia-smi --query-compute-apps=pid,used_memory --format=csv,noheader
nvidia-smi --query-gpu=memory.used,memory.free --format=csv,noheader
echo "SEMANTIC_CROSS_REPLICA_PROOF=PASS"
