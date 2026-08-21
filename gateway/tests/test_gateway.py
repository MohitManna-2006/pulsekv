"""Phase 10.5 tests for assembly, config, proxying, and fail-open behavior."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import httpx
import pytest
from starlette.testclient import TestClient

from pulsekv_gateway.assembler import AssemblyError, assemble, assemble_request
from pulsekv_gateway.auditlog import InMemoryAuditLog
from pulsekv_gateway.config import GatewayConfig, NamespaceSource, load
from pulsekv_gateway.decomposer import decompose
from pulsekv_gateway.models import (
    BlockType,
    CanonicalContextRecord,
    ContextBlock,
    GatewayComponent,
    MatchResult,
    RejectionReason,
)
from pulsekv_gateway.normalizer import hash_normalized
from pulsekv_gateway.server import GatewayService, create_app


NOW = datetime(2026, 8, 21, 12, 0, 0, tzinfo=timezone.utc)


def gateway_config(tmp_path, **overrides) -> GatewayConfig:
    fields = dict(
        namespace_source=NamespaceSource.HEADER,
        namespace_header="x-pulsekv-namespace",
        registry_dsn=str(tmp_path / "registry.db"),
        upstream_url="http://inference.test",
        bypass_min_eligible_tokens=0,
    )
    fields.update(overrides)
    return GatewayConfig(**fields)


def record(
    canonical_text: str = "Canonical organization policy.",
    *,
    aliases=("legacy organization policy",),
    block_type=BlockType.SYSTEM_PROMPT,
) -> CanonicalContextRecord:
    return CanonicalContextRecord(
        context_id="organization-policy",
        version=1,
        namespace="acme",
        canonical_text=canonical_text,
        content_hash=hash_normalized(canonical_text),
        block_type=block_type,
        aliases=aliases,
        created_at=NOW,
        created_by="phase-10.5-test",
    )


class TestAssembly:
    def test_preserves_order_and_only_replaces_named_indices(self):
        blocks = (
            ContextBlock(index=0, block_type=BlockType.SYSTEM_PROMPT, text="a"),
            ContextBlock(index=1, block_type=BlockType.USER_QUERY, text="b"),
            ContextBlock(index=2, block_type=BlockType.TOOL_SCHEMA, text='{"z": 1}'),
        )
        assert assemble(blocks, {0: "A", 2: '{"z":2}'}) == ("A", "b", '{"z":2}')

    def test_no_substitution_returns_original_request_object(self):
        request = {
            "messages": [
                {"role": "system", "content": "  retain whitespace  "},
                {"role": "user", "content": "question"},
            ],
            "temperature": 0.2,
        }
        blocks = decompose(request)
        assert assemble_request(request, blocks, {}) is request

    def test_message_and_tool_positions_are_unchanged(self):
        request = {
            "messages": [
                {"role": "system", "content": "old"},
                {"role": "user", "content": "question"},
            ],
            "tools": [
                {"type": "function", "function": {"name": "old_tool"}},
                {"type": "function", "function": {"name": "untouched"}},
            ],
        }
        blocks = decompose(request)
        output = assemble_request(
            request,
            blocks,
            {
                0: "canonical",
                2: '{"type":"function","function":{"name":"canonical_tool"}}',
            },
        )
        assert [message["role"] for message in output["messages"]] == [
            "system",
            "user",
        ]
        assert output["messages"][0]["content"] == "canonical"
        assert output["messages"][1]["content"] == "question"
        assert [tool["function"]["name"] for tool in output["tools"]] == [
            "canonical_tool",
            "untouched",
        ]
        assert request["messages"][0]["content"] == "old"  # deep copy

    def test_invalid_tool_canonical_text_is_refused(self):
        request = {"messages": [], "tools": [{"type": "function"}]}
        with pytest.raises(AssemblyError, match="not valid JSON"):
            assemble_request(request, decompose(request), {0: "not-json"})

    def test_unknown_or_duplicate_indices_are_refused(self):
        block = ContextBlock(index=0, block_type=BlockType.SYSTEM_PROMPT, text="x")
        with pytest.raises(AssemblyError, match="unknown blocks"):
            assemble((block,), {1: "y"})
        with pytest.raises(AssemblyError, match="duplicate"):
            assemble((block, block), {})


class TestConfiguration:
    def test_yaml_loads_defaults_and_rejects_unknown_keys(self, tmp_path):
        config_file = tmp_path / "gateway.yaml"
        config_file.write_text(
            "\n".join(
                [
                    "namespace_source: static",
                    "static_namespace: acme",
                    f"registry_dsn: {tmp_path / 'registry.db'}",
                    "upstream_url: http://127.0.0.1:30000",
                ]
            )
        )
        config = load(str(config_file))
        assert config.listen_port == 8088
        assert config.static_namespace == "acme"

        config_file.write_text(config_file.read_text() + "\nupsteam_url: typo\n")
        with pytest.raises(ValueError, match="upsteam_url"):
            load(str(config_file))

    def test_reports_multiple_cross_field_problems(self, tmp_path):
        config = GatewayConfig(
            namespace_source=NamespaceSource.ROUTE,
            registry_dsn="postgresql://wrong/backend",
            upstream_url="not-a-url",
            route_namespaces={"relative": "Bad Namespace"},
            top_k=1,
            guard_top_n=2,
        )
        problems = config.validate_config()
        assert any("route_namespaces key" in problem for problem in problems)
        assert any("invalid namespace" in problem for problem in problems)
        assert any("SQLite" in problem for problem in problems)
        assert any("absolute http" in problem for problem in problems)
        assert any("guard_top_n" in problem for problem in problems)

    @pytest.mark.parametrize(
        "source,extra",
        [
            (NamespaceSource.STATIC, {"static_namespace": "acme"}),
            (
                NamespaceSource.HEADER,
                {"namespace_header": "x-pulsekv-namespace"},
            ),
            (
                NamespaceSource.API_KEY,
                {"api_key_namespaces": {"secret": "acme"}},
            ),
            (
                NamespaceSource.ROUTE,
                {"route_namespaces": {"/v1/chat/completions": "acme"}},
            ),
        ],
    )
    def test_every_namespace_source_has_a_valid_shape(self, tmp_path, source, extra):
        config = gateway_config(tmp_path, namespace_source=source, **extra)
        assert config.validate_config() == []


class TestProxyFlow:
    def test_match_substitutes_in_place_and_preserves_routing_headers(self, tmp_path):
        captured = []

        async def downstream(request: httpx.Request) -> httpx.Response:
            captured.append((request, await request.aread()))
            return httpx.Response(
                200,
                json={"id": "chatcmpl-test"},
                headers={"x-upstream": "stub"},
            )

        config = gateway_config(tmp_path)
        service = GatewayService(config)
        service.registry.register(record())
        client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
        app = create_app(config, service=service, http_client=client)
        try:
            with TestClient(app) as test_client:
                response = test_client.post(
                    "/v1/chat/completions?trace=1",
                    headers={
                        "x-pulsekv-namespace": "acme",
                        "authorization": "Bearer upstream-key",
                        "x-custom": "kept",
                        "x-request-id": "req-10-5",
                    },
                    json={
                        "model": "stub-model",
                        "messages": [
                            {
                                "role": "system",
                                "content": "legacy organization policy",
                            },
                            {"role": "user", "content": "keep me unchanged"},
                        ],
                    },
                )
            assert response.status_code == 200
            assert response.headers["x-upstream"] == "stub"
            assert response.headers["x-request-id"] == "req-10-5"
            request, body = captured[0]
            forwarded = json.loads(body)
            assert request.url == "http://inference.test/v1/chat/completions?trace=1"
            assert request.headers["authorization"] == "Bearer upstream-key"
            assert request.headers["x-custom"] == "kept"
            assert "x-pulsekv-namespace" not in request.headers
            assert forwarded["messages"][0]["content"] == (
                "Canonical organization policy."
            )
            assert forwarded["messages"][1]["content"] == "keep me unchanged"
            decisions = service.audit.for_request("req-10-5")
            assert [decision.decision_label for decision in decisions] == [
                "alias",
                "bypassed",
            ]
        finally:
            service.close()
            asyncio.run(client.aclose())

    def test_no_match_forwards_the_exact_original_bytes(self, tmp_path):
        captured = []

        async def downstream(request: httpx.Request) -> httpx.Response:
            captured.append(await request.aread())
            return httpx.Response(200, content=b"{}")

        config = gateway_config(tmp_path)
        service = GatewayService(config)
        client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
        app = create_app(config, service=service, http_client=client)
        raw = b'{ "model" : "stub", "messages" : [{"role":"system","content":"unregistered"}] }'
        try:
            with TestClient(app) as test_client:
                response = test_client.post(
                    "/v1/chat/completions",
                    headers={"x-pulsekv-namespace": "acme"},
                    content=raw,
                )
            assert response.status_code == 200
            assert captured == [raw]
        finally:
            service.close()
            asyncio.run(client.aclose())

    def test_disabled_gateway_needs_no_namespace_and_is_byte_exact(self, tmp_path):
        captured = []

        async def downstream(request: httpx.Request) -> httpx.Response:
            captured.append(await request.aread())
            return httpx.Response(200, content=b"ok")

        config = gateway_config(tmp_path, enabled=False)
        service = GatewayService(config)
        client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
        app = create_app(config, service=service, http_client=client)
        raw = b"not even json"
        try:
            with TestClient(app) as test_client:
                response = test_client.post("/v1/chat/completions", content=raw)
            assert response.content == b"ok"
            assert captured == [raw]
        finally:
            service.close()
            asyncio.run(client.aclose())

    def test_namespace_is_mandatory_and_health_is_non_sensitive(self, tmp_path):
        config = gateway_config(tmp_path)
        service = GatewayService(config)
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: None))
        app = create_app(config, service=service, http_client=client)
        try:
            with TestClient(app) as test_client:
                missing = test_client.post("/v1/chat/completions", content=b"{}")
                health = test_client.get("/healthz")
                ready = test_client.get("/readyz")
            assert missing.status_code == 400
            assert health.json()["status"] == "ok"
            assert "registry" not in health.text
            assert ready.status_code == 200
        finally:
            service.close()
            asyncio.run(client.aclose())

    def test_streaming_response_is_relayed_without_buffer_reformatting(self, tmp_path):
        class Chunks(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield b"data: one\n\n"
                yield b"data: [DONE]\n\n"

        async def downstream(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                stream=Chunks(),
                headers={"content-type": "text/event-stream"},
            )

        config = gateway_config(tmp_path, enabled=False)
        service = GatewayService(config)
        client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
        app = create_app(config, service=service, http_client=client)
        try:
            with TestClient(app) as test_client:
                response = test_client.post(
                    "/v1/chat/completions", content=b'{"stream":true}'
                )
            assert response.headers["content-type"] == "text/event-stream"
            assert response.content == b"data: one\n\ndata: [DONE]\n\n"
        finally:
            service.close()
            asyncio.run(client.aclose())

    def test_upstream_transport_failure_is_a_502_not_a_semantic_error(self, tmp_path):
        async def downstream(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("stub is down", request=request)

        config = gateway_config(tmp_path, enabled=False)
        service = GatewayService(config)
        client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
        app = create_app(config, service=service, http_client=client)
        try:
            with TestClient(app) as test_client:
                response = test_client.post(
                    "/v1/chat/completions",
                    headers={"x-request-id": "req-upstream-down"},
                    content=b"{}",
                )
            assert response.status_code == 502
            assert response.headers["x-request-id"] == "req-upstream-down"
            assert response.json()["error"]["message"] == "upstream request failed"
        finally:
            service.close()
            asyncio.run(client.aclose())

    def test_request_limit_stops_reading_before_upstream_forward(self, tmp_path):
        calls = 0

        async def downstream(_request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, content=b"unexpected")

        config = gateway_config(tmp_path, enabled=False, max_request_bytes=4)
        service = GatewayService(config)
        client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
        app = create_app(config, service=service, http_client=client)
        try:
            with TestClient(app) as test_client:
                response = test_client.post(
                    "/v1/chat/completions", content=b"12345"
                )
            assert response.status_code == 413
            assert calls == 0
        finally:
            service.close()
            asyncio.run(client.aclose())


class _FixedMatcher:
    semantic_enabled = False

    def __init__(self, result: MatchResult):
        self.result = result

    def resolve(self, _block, _namespace):
        return self.result


@pytest.mark.parametrize(
    "failure_result,expected",
    [
        (MatchResult.errored(GatewayComponent.ENCODER), "encoder"),
        (
            MatchResult.rejected(
                reason=RejectionReason.GUARD_ERROR,
                context_id="candidate",
                version=1,
                confidence=0.99,
            ),
            "guard_error",
        ),
    ],
)
def test_encoder_and_guard_failures_forward_original_bytes(
    tmp_path, failure_result, expected
):
    captured = []

    async def downstream(request: httpx.Request) -> httpx.Response:
        captured.append(await request.aread())
        return httpx.Response(200, content=b"{}")

    config = gateway_config(tmp_path)
    audit = InMemoryAuditLog()
    service = GatewayService(config, audit=audit)
    service.matcher = _FixedMatcher(failure_result)
    client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
    app = create_app(config, service=service, http_client=client)
    raw = b'{"model":"stub","messages":[{"role":"system","content":"policy"}]}'
    try:
        with TestClient(app) as test_client:
            response = test_client.post(
                "/v1/chat/completions",
                headers={
                    "x-pulsekv-namespace": "acme",
                    "x-request-id": f"req-{expected}",
                },
                content=raw,
            )
        assert response.status_code == 200
        assert captured == [raw]
        decision = audit.for_request(f"req-{expected}")[0]
        if expected == "encoder":
            assert decision.error_component is GatewayComponent.ENCODER
        else:
            assert decision.rejection_reason is RejectionReason.GUARD_ERROR
    finally:
        service.close()
        asyncio.run(client.aclose())


def test_registry_loss_during_canonical_fetch_forwards_original_bytes(tmp_path):
    captured = []

    async def downstream(request: httpx.Request) -> httpx.Response:
        captured.append(await request.aread())
        return httpx.Response(200, content=b"{}")

    config = gateway_config(tmp_path)
    service = GatewayService(config)
    service.registry.register(record(aliases=("policy",)))
    original_get = service.registry.get

    def fail_during_fetch(*args, **kwargs):
        # Tier 0 resolves the alias first; the registry then disappears before
        # the accepted version is fetched for assembly.
        service.registry.close()
        return original_get(*args, **kwargs)

    service.registry.get = fail_during_fetch
    client = httpx.AsyncClient(transport=httpx.MockTransport(downstream))
    app = create_app(config, service=service, http_client=client)
    raw = b'{"model":"stub","messages":[{"role":"system","content":"policy"}]}'
    try:
        with TestClient(app) as test_client:
            response = test_client.post(
                "/v1/chat/completions",
                headers={
                    "x-pulsekv-namespace": "acme",
                    "x-request-id": "req-registry-loss",
                },
                content=raw,
            )
        assert response.status_code == 200
        assert captured == [raw]
        decision = service.audit.for_request("req-registry-loss")[0]
        assert decision.error_component is GatewayComponent.REGISTRY
    finally:
        service.close()
        asyncio.run(client.aclose())
