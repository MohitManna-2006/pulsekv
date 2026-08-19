"""OpenAI-compatible ingress -- STUB. Implemented by Phase 10.5.

Design doc §8 (the gateway is a process the application points at instead of
the inference server, not a patch to SGLang, vLLM or anything in ``adapters/``),
§17 (failure model); plan §9.

No web framework is chosen or imported here. Plan §1 expects an ASGI framework
in Phase 10.5 and plan §9 owns the deployment-shape decision between design doc
§8's option E (inline proxy) and option F (a service applications call
directly); both are packaging decisions made there, against a real request
path, rather than implied by a dependency added now.

Two requirements Phase 10.5 inherits and must not quietly drop:

* **Applications keep a working path to SGLang/vLLM that does not depend on
  this process being up** (design doc §17's last row, risk register row 15).
  Fail-open covers a component failing inside a running gateway; it does not
  cover the gateway being unreachable, and only a deployment/routing decision
  does.
* **Nothing already using ``PulseKVClient``, ``NodeService`` or
  ``ClusterMetadataService`` notices this process exists** (design doc §7.4).
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from .config import GatewayConfig


def create_app(config: GatewayConfig) -> Any:
    """Build the ASGI application for a validated config."""
    raise NotImplementedError("Phase 10.5")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point: load config, validate, refuse to start if invalid.

    Design doc-adjacent but concrete: config problems are reported all at once
    and the process does not start, matching ``control/internal/config``'s
    posture rather than half-starting and failing later (see ``config``).
    """
    raise NotImplementedError("Phase 10.5")
