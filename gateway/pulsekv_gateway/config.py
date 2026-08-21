"""Configuration for the Phase 10.5 OpenAI-compatible gateway process.

Loading is deliberately strict: YAML/JSON is parsed once, unknown fields are
rejected by Pydantic, and all cross-field problems are reported together. A
bad deployment must refuse to start rather than quietly select a tenant,
registry, or upstream that the operator did not name.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, ValidationError

from .guardrail import SIMILARITY_THRESHOLD
from .matcher import DEFAULT_GUARD_TOP_N, DEFAULT_TOP_K
from .models import Namespace

__all__ = [
    "PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS",
    "PLACEHOLDER_ENCODER_TIMEOUT_MS",
    "PLACEHOLDER_GUARD_TIMEOUT_MS",
    "PLACEHOLDER_REGISTRY_TIMEOUT_MS",
    "GatewayConfig",
    "NamespaceSource",
    "load",
]


# Phase 10.8 replaces these unmeasured defaults. They are configuration
# defaults, not performance conclusions.
PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS = 512
PLACEHOLDER_REGISTRY_TIMEOUT_MS = 50
PLACEHOLDER_ENCODER_TIMEOUT_MS = 100
PLACEHOLDER_GUARD_TIMEOUT_MS = 50

_NAMESPACE_ADAPTER = TypeAdapter(Namespace)


class NamespaceSource(str, Enum):
    """Deployment-layer source of the already-authenticated namespace."""

    API_KEY = "api_key"
    HEADER = "header"
    ROUTE = "route"
    STATIC = "static"


class GatewayConfig(BaseModel):
    """Validated process, proxy, matching, and namespace configuration."""

    model_config = ConfigDict(frozen=True, extra="forbid", validate_default=True)

    enabled: bool = True
    namespace_source: NamespaceSource
    namespace_header: Optional[str] = None
    static_namespace: Optional[str] = None

    # API-key mode maps an opaque inbound credential to a namespace. Keys are
    # never written to the decision log. A leading ``Bearer `` is stripped
    # before lookup, so the map contains the credential itself.
    api_key_header: str = "authorization"
    api_key_namespaces: Dict[str, str] = Field(default_factory=dict, repr=False)

    # Route mode uses an exact inbound path -> namespace deployment rule. It
    # keeps namespace choice outside prompt content without inventing a global
    # namespace. The normal OpenAI path can therefore be mapped directly.
    route_namespaces: Dict[str, str] = Field(default_factory=dict)

    registry_dsn: str
    upstream_url: str
    model_dir: Optional[str] = None
    audit_log_path: Optional[str] = None

    listen_host: str = "127.0.0.1"
    listen_port: int = Field(default=8088, ge=1, le=65535)
    request_timeout_ms: int = Field(default=120_000, gt=0)
    max_request_bytes: int = Field(default=16 * 1024 * 1024, gt=0)

    bypass_min_eligible_tokens: int = Field(
        default=PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS, ge=0
    )
    registry_timeout_ms: int = Field(
        default=PLACEHOLDER_REGISTRY_TIMEOUT_MS, gt=0
    )
    encoder_timeout_ms: int = Field(default=PLACEHOLDER_ENCODER_TIMEOUT_MS, gt=0)
    guard_timeout_ms: int = Field(default=PLACEHOLDER_GUARD_TIMEOUT_MS, gt=0)
    similarity_threshold: float = Field(
        default=SIMILARITY_THRESHOLD, ge=0.0, le=1.0
    )
    top_k: int = Field(default=DEFAULT_TOP_K, gt=0)
    guard_top_n: int = Field(default=DEFAULT_GUARD_TOP_N, gt=0)

    def validate_config(self) -> List[str]:
        """Return every semantic configuration problem, in stable order."""

        problems: List[str] = []

        if self.namespace_source is NamespaceSource.HEADER:
            if not _nonempty(self.namespace_header):
                problems.append(
                    "namespace_header is required when namespace_source=header"
                )
        elif self.namespace_source is NamespaceSource.STATIC:
            if not _nonempty(self.static_namespace):
                problems.append(
                    "static_namespace is required when namespace_source=static"
                )
            elif not _valid_namespace(self.static_namespace):
                problems.append("static_namespace is not a valid PulseKV namespace")
        elif self.namespace_source is NamespaceSource.API_KEY:
            if not _nonempty(self.api_key_header):
                problems.append(
                    "api_key_header is required when namespace_source=api_key"
                )
            if not self.api_key_namespaces:
                problems.append(
                    "api_key_namespaces must not be empty when namespace_source=api_key"
                )
        elif self.namespace_source is NamespaceSource.ROUTE:
            if not self.route_namespaces:
                problems.append(
                    "route_namespaces must not be empty when namespace_source=route"
                )

        for credential, namespace in self.api_key_namespaces.items():
            if not credential:
                problems.append("api_key_namespaces contains an empty credential")
            if not _valid_namespace(namespace):
                problems.append(
                    f"api_key_namespaces contains invalid namespace {namespace!r}"
                )
        for route, namespace in self.route_namespaces.items():
            if not route.startswith("/"):
                problems.append(f"route_namespaces key {route!r} must start with '/'")
            if not _valid_namespace(namespace):
                problems.append(
                    f"route_namespaces contains invalid namespace {namespace!r}"
                )

        if not _nonempty(self.registry_dsn):
            problems.append("registry_dsn must not be empty")
        elif "://" in self.registry_dsn and not self.registry_dsn.startswith(
            "sqlite://"
        ):
            problems.append("registry_dsn must be a SQLite DSN or filesystem path")

        if not _nonempty(self.upstream_url):
            problems.append("upstream_url must not be empty")
        else:
            parsed = urlparse(self.upstream_url)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                problems.append("upstream_url must be an absolute http(s) URL")
            if parsed.query or parsed.fragment:
                problems.append("upstream_url must not contain a query or fragment")

        if not _nonempty(self.listen_host):
            problems.append("listen_host must not be empty")
        if self.guard_top_n > self.top_k:
            problems.append("guard_top_n must be less than or equal to top_k")

        return list(dict.fromkeys(problems))

    def warnings(self) -> List[str]:
        """Return legal but operationally surprising settings."""

        warnings: List[str] = []
        if not self.enabled:
            warnings.append("semantic matching is disabled; gateway is pass-through")
        if self.bypass_min_eligible_tokens == 0:
            warnings.append(
                "bypass_min_eligible_tokens=0 enables matching for every eligible block"
            )
        if self.enabled and not self.model_dir:
            warnings.append(
                "model_dir is unset; deterministic Tier 0/1 matching works but "
                "semantic Tier 2 is disabled"
            )
        if not self.audit_log_path:
            warnings.append(
                "audit_log_path is unset; decisions are kept only in process memory"
            )
        return warnings


def load(path: str) -> GatewayConfig:
    """Parse, default, and validate a YAML or JSON file."""

    source = Path(path).expanduser()
    try:
        raw = yaml.safe_load(source.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"{source}: cannot read config: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"{source}: invalid YAML: {exc}") from exc
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError(f"{source}: config root must be a mapping")
    try:
        config = GatewayConfig.model_validate(raw)
    except ValidationError as exc:
        rendered = "; ".join(
            f"{'.'.join(str(part) for part in error['loc'])}: {error['msg']}"
            for error in exc.errors()
        )
        raise ValueError(f"{source}: invalid config: {rendered}") from exc
    problems = config.validate_config()
    if problems:
        raise ValueError(f"{source}: invalid config: {'; '.join(problems)}")
    return config


def _nonempty(value: Optional[str]) -> bool:
    return value is not None and bool(value.strip())


def _valid_namespace(value: str) -> bool:
    try:
        _NAMESPACE_ADAPTER.validate_python(value)
    except ValidationError:
        return False
    return True
