"""OpenAI-compatible, fail-open semantic context reverse proxy (Phase 10.5)."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Mapping, Optional, Sequence, Tuple

import httpx
import uvicorn
from pydantic import TypeAdapter, ValidationError
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from .assembler import assemble_request
from .auditlog import AuditLog, InMemoryAuditLog, JsonlAuditLog
from .config import GatewayConfig, NamespaceSource, load
from .decomposer import decompose
from .encoder import EncoderError, OnnxEncoder
from .guardrail import Guardrail
from .index import VectorIndex
from .matcher import Matcher
from .models import (
    BypassReason,
    ContextBlock,
    DecisionLogRecord,
    GatewayComponent,
    GatewayError,
    MatchResult,
    Namespace,
)
from .normalizer import hash_normalized
from .registry import Registry, RegistryError

__all__ = ["GatewayService", "create_app", "main"]

_NAMESPACE_ADAPTER = TypeAdapter(Namespace)
_LOGGER = logging.getLogger("pulsekv_gateway")
_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


class NamespaceResolutionError(ValueError):
    """The deployment input did not resolve to an allowed namespace."""


class RequestTooLargeError(ValueError):
    """The request stream crossed the configured memory/body bound."""


class GatewayService:
    """Owns the synchronous matcher resources used by request worker threads."""

    def __init__(
        self,
        config: GatewayConfig,
        *,
        registry: Optional[Registry] = None,
        matcher: Optional[Matcher] = None,
        audit: Optional[AuditLog] = None,
    ) -> None:
        problems = config.validate_config()
        if problems:
            raise ValueError("invalid gateway config: " + "; ".join(problems))
        self.config = config
        self.registry = registry or Registry.from_dsn(
            config.registry_dsn,
            busy_timeout_ms=config.registry_timeout_ms,
            hash_text=hash_normalized,
        )
        self.audit = audit or (
            JsonlAuditLog(config.audit_log_path)
            if config.audit_log_path
            else InMemoryAuditLog()
        )
        self._encoder: Optional[OnnxEncoder] = None
        self._index: Optional[VectorIndex] = None
        self._indexed_namespaces: set[str] = set()
        self._index_lock = threading.Lock()
        self.semantic_startup_error: Optional[str] = None

        if matcher is not None:
            self.matcher = matcher
        else:
            encoder = None
            index = None
            if config.enabled and config.model_dir:
                try:
                    encoder = OnnxEncoder(
                        config.model_dir, timeout_ms=config.encoder_timeout_ms
                    )
                    index = VectorIndex(encoder)
                    self._encoder = encoder
                    self._index = index
                except EncoderError as exc:
                    # Design doc §17: encoder loss disables Tier 2/3, never the
                    # deterministic tiers and never the request path.
                    self.semantic_startup_error = "encoder_unavailable"
                    _LOGGER.error("semantic encoder unavailable at startup: %s", exc)
            self.matcher = Matcher(
                self.registry,
                encoder=encoder,
                index=index,
                top_k=config.top_k,
                guardrail=Guardrail(timeout_ms=config.guard_timeout_ms),
                similarity_threshold=config.similarity_threshold,
                guard_top_n=config.guard_top_n,
            )

    @property
    def semantic_enabled(self) -> bool:
        return self.matcher.semantic_enabled

    def close(self) -> None:
        if self._encoder is not None:
            self._encoder.close()
        self.audit.close()
        self.registry.close()

    def resolve_namespace(self, request: Request) -> str:
        """Resolve and validate namespace exclusively from deployment input."""

        source = self.config.namespace_source
        value: Optional[str]
        if source is NamespaceSource.STATIC:
            value = self.config.static_namespace
        elif source is NamespaceSource.HEADER:
            value = request.headers.get(self.config.namespace_header or "")
        elif source is NamespaceSource.API_KEY:
            credential = request.headers.get(self.config.api_key_header, "")
            if credential.lower().startswith("bearer "):
                credential = credential[7:].strip()
            value = self.config.api_key_namespaces.get(credential)
        else:
            value = self.config.route_namespaces.get(request.url.path)
        if value is None:
            raise NamespaceResolutionError(
                f"request did not resolve through namespace_source={source.value}"
            )
        try:
            return _NAMESPACE_ADAPTER.validate_python(value)
        except ValidationError as exc:
            raise NamespaceResolutionError("resolved namespace is invalid") from exc

    def transform(
        self,
        payload: Mapping[str, Any],
        raw_body: bytes,
        *,
        namespace: str,
        request_id: str,
    ) -> bytes:
        """Apply accepted substitutions, returning original bytes on any failure."""

        if not self.config.enabled:
            return raw_body

        try:
            blocks = tuple(_with_token_estimate(block) for block in decompose(payload))
            model = payload.get("model")
            model_name = model if isinstance(model, str) and model else "unknown"
            timestamp = datetime.now(timezone.utc)

            eligible_tokens = sum(
                block.token_estimate or 0
                for block in blocks
                if block.is_mvp_eligible
            )
            if eligible_tokens < self.config.bypass_min_eligible_tokens:
                results = tuple(
                    MatchResult.bypassed(
                        BypassReason.BELOW_MIN_TOKENS
                        if block.is_mvp_eligible
                        else BypassReason.INELIGIBLE_BLOCK_TYPE
                    )
                    for block in blocks
                )
                self._record(
                    blocks,
                    results,
                    namespace=namespace,
                    request_id=request_id,
                    timestamp=timestamp,
                    model=model_name,
                )
                return raw_body

            self._ensure_namespace_indexed(namespace)
            results, substitutions = self._resolve(
                blocks,
                namespace=namespace,
                request_id=request_id,
                timestamp=timestamp,
                model=model_name,
            )
            if not substitutions:
                return raw_body
            assembled = assemble_request(payload, blocks, substitutions)
            return json.dumps(
                assembled, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        except (GatewayError, RegistryError, ValueError, TypeError, KeyError) as exc:
            # Fail-open is byte-for-byte: malformed annotations, decomposition
            # drift, registry loss during canonical fetch, and unsafe canonical
            # tool JSON all send precisely the bytes the application sent.
            _LOGGER.warning(
                "semantic transform failed open request_id=%s error=%s",
                request_id,
                type(exc).__name__,
            )
            return raw_body

    def _resolve(
        self,
        blocks: Sequence[ContextBlock],
        *,
        namespace: str,
        request_id: str,
        timestamp: datetime,
        model: str,
    ) -> Tuple[Tuple[MatchResult, ...], Mapping[int, str]]:
        results = []
        substitutions: dict[int, str] = {}
        for block in blocks:
            result = self.matcher.resolve(block, namespace)
            if result.substitutes:
                try:
                    record = self.registry.get(
                        result.context_id or "",
                        namespace,
                        version=result.version,
                    )
                    substitutions[block.index] = record.canonical_text
                except RegistryError:
                    # A match is not useful unless its exact version can still
                    # be read. Record the final sent outcome, not the stale hit.
                    result = MatchResult.errored(GatewayComponent.REGISTRY)
            results.append(result)
        final = tuple(results)
        self._record(
            blocks,
            final,
            namespace=namespace,
            request_id=request_id,
            timestamp=timestamp,
            model=model,
        )
        return final, substitutions

    def _record(
        self,
        blocks: Sequence[ContextBlock],
        results: Sequence[MatchResult],
        *,
        namespace: str,
        request_id: str,
        timestamp: datetime,
        model: str,
    ) -> None:
        self.audit.record_many(
            DecisionLogRecord.from_match_result(
                result,
                request_id=request_id,
                timestamp=timestamp,
                namespace=namespace,
                model=model,
                block=block,
                block_content_hash=Matcher.block_hash(block),
            )
            for block, result in zip(blocks, results)
        )

    def _ensure_namespace_indexed(self, namespace: str) -> None:
        if self._index is None or namespace in self._indexed_namespaces:
            return
        with self._index_lock:
            if namespace in self._indexed_namespaces:
                return
            report = self._index.build_from_registry(
                self.registry, namespaces=(namespace,)
            )
            # A registry error is retried on the next request. Model-mismatched
            # or missing embeddings are safe skips and do not make startup fail.
            if report.registry_errors == 0:
                self._indexed_namespaces.add(namespace)


def create_app(
    config: GatewayConfig,
    *,
    service: Optional[GatewayService] = None,
    http_client: Optional[httpx.AsyncClient] = None,
) -> Starlette:
    """Build the validated ASGI reverse proxy.

    The deployed shape is design option E: an inline reverse proxy. Gateway
    process loss cannot be repaired by code inside that process, so the direct
    ``upstream_url`` remains an independent operator/LB fallback route and is
    documented as part of deployment rather than hidden behind this app.
    """

    gateway = service or GatewayService(config)
    owns_gateway = service is None
    client = http_client or httpx.AsyncClient(
        timeout=httpx.Timeout(config.request_timeout_ms / 1000.0)
    )
    owns_client = http_client is None

    @asynccontextmanager
    async def lifespan(_app: Starlette):
        yield
        if owns_client:
            await client.aclose()
        if owns_gateway:
            gateway.close()

    async def health(_request: Request) -> Response:
        return JSONResponse(
            {
                "status": "ok",
                "semantic_enabled": gateway.semantic_enabled,
                "semantic_startup_error": gateway.semantic_startup_error,
                "audit_dropped": gateway.audit.dropped,
            }
        )

    async def ready(_request: Request) -> Response:
        try:
            await run_in_threadpool(gateway.registry.applied_migrations)
        except RegistryError:
            return JSONResponse({"status": "not_ready"}, status_code=503)
        return JSONResponse({"status": "ready"})

    async def chat_completions(request: Request) -> Response:
        try:
            raw_body = await _read_limited_body(request, config.max_request_bytes)
        except RequestTooLargeError:
            return JSONResponse(
                {"error": {"message": "request body exceeds gateway limit"}},
                status_code=413,
            )

        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        namespace: Optional[str] = None
        if config.enabled:
            try:
                namespace = gateway.resolve_namespace(request)
            except NamespaceResolutionError as exc:
                return JSONResponse(
                    {"error": {"message": str(exc)}},
                    status_code=(
                        401
                        if config.namespace_source is NamespaceSource.API_KEY
                        else 400
                    ),
                )

        outbound_body = raw_body
        if config.enabled and namespace is not None:
            try:
                payload = json.loads(raw_body)
                if isinstance(payload, Mapping):
                    outbound_body = await run_in_threadpool(
                        gateway.transform,
                        payload,
                        raw_body,
                        namespace=namespace,
                        request_id=request_id,
                    )
            except (json.JSONDecodeError, UnicodeDecodeError):
                # The upstream remains the authority for OpenAI request
                # validation; the gateway forwards malformed JSON unchanged.
                pass

        url = config.upstream_url.rstrip("/") + request.url.path
        if request.url.query:
            url += "?" + request.url.query
        headers = _request_headers(request, config, request_id)
        upstream_request = client.build_request(
            "POST", url, headers=headers, content=outbound_body
        )
        try:
            upstream = await client.send(upstream_request, stream=True)
        except httpx.RequestError as exc:
            _LOGGER.error(
                "upstream request failed request_id=%s error=%s: %s",
                request_id,
                type(exc).__name__,
                exc,
            )
            return JSONResponse(
                {"error": {"message": "upstream request failed"}},
                status_code=502,
                headers={"x-request-id": request_id},
            )
        return StreamingResponse(
            _response_body(upstream),
            status_code=upstream.status_code,
            headers=_response_headers(upstream, request_id),
        )

    return Starlette(
        debug=False,
        routes=[
            Route("/healthz", health, methods=["GET"]),
            Route("/readyz", ready, methods=["GET"]),
            Route("/v1/chat/completions", chat_completions, methods=["POST"]),
        ],
        lifespan=lifespan,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Load a config and run one uvicorn worker."""

    parser = argparse.ArgumentParser(description="PulseKV semantic context gateway")
    parser.add_argument("--config", required=True, help="gateway YAML/JSON path")
    args = parser.parse_args(argv)
    try:
        config = load(args.config)
        for warning in config.warnings():
            print(f"warning: {warning}", file=sys.stderr)
        app = create_app(config)
    except (ValueError, RegistryError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    uvicorn.run(app, host=config.listen_host, port=config.listen_port, workers=1)
    return 0


def _with_token_estimate(block: ContextBlock) -> ContextBlock:
    # This is only the performance bypass estimate, never an identity or safety
    # input. Four Unicode characters per token is intentionally conservative and
    # Phase 10.8 owns replacing it with measured data without adding inference-
    # engine tokenization to this gateway.
    estimate = max(1, (len(block.text) + 3) // 4)
    return block.model_copy(update={"token_estimate": estimate})


def _request_headers(
    request: Request, config: GatewayConfig, request_id: str
) -> list[tuple[str, str]]:
    stripped = _HOP_BY_HOP_HEADERS | {"host", "content-length"}
    if config.namespace_source is NamespaceSource.HEADER and config.namespace_header:
        stripped.add(config.namespace_header.lower())
    headers = [
        (name, value)
        for name, value in request.headers.items()
        if name.lower() not in stripped and name.lower() != "x-request-id"
    ]
    headers.append(("x-request-id", request_id))
    return headers


def _response_headers(response: httpx.Response, request_id: str) -> dict[str, str]:
    headers = {
        name: value
        for name, value in response.headers.items()
        if name.lower() not in _HOP_BY_HOP_HEADERS
        and name.lower() not in {"content-length", "x-request-id"}
    }
    headers["x-request-id"] = request_id
    return headers


async def _response_body(response: httpx.Response) -> AsyncIterator[bytes]:
    """Yield an upstream body whether its transport buffered or streamed it.

    Real network transports leave ``stream=True`` responses unread. httpx's
    in-process ``MockTransport`` buffers them, and supporting both keeps the
    integration test faithful without changing production streaming behavior.
    """

    try:
        if response.is_stream_consumed:
            yield response.content
            return
        async for chunk in response.aiter_raw():
            yield chunk
    finally:
        await response.aclose()


async def _read_limited_body(request: Request, limit: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise RequestTooLargeError
        chunks.append(chunk)
    return b"".join(chunks)


if __name__ == "__main__":  # pragma: no cover - exercised through console script
    raise SystemExit(main())
