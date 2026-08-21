"""Phase 10.6 Gateway-to-SGLang request-shape integration coverage."""
from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
from starlette.testclient import TestClient

from pulsekv_gateway.config import GatewayConfig, NamespaceSource
from pulsekv_gateway.models import BlockType, CanonicalContextRecord
from pulsekv_gateway.normalizer import hash_normalized
from pulsekv_gateway.server import GatewayService, create_app


def successful_read_keys(keys: list[str], result: object) -> set[str]:
    if not isinstance(result, list) or len(result) != len(keys):
        raise ValueError("malformed batch read evidence")
    if any(type(value) is not bool for value in result):
        raise ValueError("non-boolean batch read evidence")
    return {key for key, succeeded in zip(keys, result) if succeeded}


def semantic_proof_passes(
    raw_a: str,
    raw_b: str,
    canonical_a: str,
    canonical_b: str,
    tokens_a: list[int],
    tokens_b: list[int],
    sha_a: str,
    sha_b: str,
    read_keys: set[str],
    written_keys: set[str],
) -> bool:
    return all(
        (
            raw_a != raw_b,
            canonical_a == canonical_b,
            tokens_a == tokens_b,
            sha_a == sha_b,
            bool(read_keys),
            bool(read_keys & written_keys),
        )
    )


def test_batch_read_evidence_counts_only_successful_keys():
    assert successful_read_keys(
        ["k1", "k2", "k3"], [False, True, False]
    ) == {"k2"}


def test_mismatched_captured_canonicals_cannot_pass_semantic_proof():
    assert not semantic_proof_passes(
        raw_a="raw a",
        raw_b="raw b",
        canonical_a="canonical a",
        canonical_b="canonical b",
        tokens_a=[1],
        tokens_b=[1],
        sha_a="same",
        sha_b="same",
        read_keys={"shared"},
        written_keys={"shared"},
    )


def test_semantic_variants_forward_identical_sglang_prefix_and_generation(tmp_path):
    canonical = "Canonical safety rule: preserve production data. " * 80
    variant = "Production information must be kept; this is the safety policy. " * 80
    captured = []

    async def upstream(request: httpx.Request) -> httpx.Response:
        body = json.loads(await request.aread())
        captured.append(body)
        return httpx.Response(200, json={"id": "sglang-real-shape", "choices": []})

    config = GatewayConfig(
        enabled=True,
        namespace_source=NamespaceSource.STATIC,
        static_namespace="phase10-6",
        registry_dsn=str(tmp_path / "registry.db"),
        upstream_url="http://127.0.0.1:30000",
        bypass_min_eligible_tokens=0,
    )
    service = GatewayService(config)
    service.registry.register(
        CanonicalContextRecord(
            context_id="phase10-6-policy",
            version=1,
            namespace="phase10-6",
            canonical_text=canonical,
            content_hash=hash_normalized(canonical),
            block_type=BlockType.SYSTEM_PROMPT,
            aliases=(variant,),
            created_at=datetime.now(timezone.utc),
            created_by="phase-10.6-test",
        )
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(upstream))
    app = create_app(config, service=service, http_client=client)
    try:
        with TestClient(app) as gateway:
            for system_text in (canonical, variant):
                response = gateway.post(
                    "/v1/chat/completions",
                    json={
                        "model": "Qwen/Qwen2.5-1.5B-Instruct",
                        "messages": [
                            {"role": "system", "content": system_text},
                            {"role": "user", "content": "Reply briefly."},
                        ],
                        "temperature": 0,
                        "max_tokens": 8,
                    },
                )
                assert response.status_code == 200
    finally:
        service.close()

    assert captured[0] == captured[1]
    assert captured[0]["messages"][0]["content"] == canonical
    assert captured[0]["temperature"] == 0
    assert captured[0]["max_tokens"] == 8
    assert captured[0]["model"] == "Qwen/Qwen2.5-1.5B-Instruct"
