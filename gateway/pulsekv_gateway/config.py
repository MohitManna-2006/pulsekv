"""Gateway configuration -- SHAPE ONLY. Validation implemented by Phase 10.5.

Phase 10.0 freezes the *shape* and the *validation posture*; it implements
neither loading nor validation. The point of doing the shape now is that Phase
10.5 should not have to re-decide questions the design doc has already
answered -- which fields exist, which have no safe default, and what happens to
a config that does not validate.

**Validation posture, committed here so 10.5 inherits rather than invents it.**
Follow ``control/internal/config/config.go`` exactly:

* ``Load`` parses, applies defaults, then validates, and returns an error
  instead of a partly-usable config. That file's own comment states the rule --
  "a dev cluster that half-starts because two nodes share a port is worse than
  one that refuses to start" -- and it applies unchanged to a gateway that
  would otherwise start with, say, a registry pointed at the wrong environment
  (risk register row 13).
* Unknown keys are rejected, the way ``dec.KnownFields(true)`` does there. A
  typo'd key that silently defaults is the failure that costs an hour later.
* ``validate()`` collects **every** problem and reports them together, the way
  that file's ``Validate()`` accumulates into ``problems`` and returns
  ``errors.Join(problems...)``. ``docs/pulsekv-v2-phase0-summary.md`` §6's
  negative-path table records this as tested behavior, not an aspiration:
  "Duplicate ``node_id`` and port in config -> Rejected before anything starts,
  both problems reported at once."
* ``warnings()`` is separate from ``validate()``: legal configurations that
  will start but probably are not what the operator meant (a bypass threshold
  of 0, a registry and an upstream on the same host in production) are
  surfaced, not refused. Same split as that file's ``Warnings()``.

**The checklist Phase 10.5 must implement in ``validate()``** -- each item is
already decided by a document, so none of it is 10.5's call to make:

1. ``namespace_source`` resolves: ``HEADER`` requires ``namespace_header``,
   ``STATIC`` requires ``static_namespace``, and a ``static_namespace`` must
   itself satisfy ``models.Namespace``'s pattern. Design doc §15 takes
   namespace as an input from the deployment layer; a gateway that cannot
   determine one must refuse to start rather than invent a default, because
   plan §5 states no global namespace exists.
2. ``registry_dsn`` and ``upstream_url`` are non-empty and parse as URLs.
3. ``bypass_min_eligible_tokens`` is >= 0, with a warning at 0 (risk register
   row 13 names that exact misconfiguration).
4. Every timeout is > 0. Design doc §17's budgets and risk register row 14 both
   require real enforced timeouts rather than aspirational ones; a 0 or absent
   timeout is how "fail-open" quietly becomes "hang".
5. Nothing here silently disables the feature: ``enabled=False`` is legal and
   observable (``pulsekv_semantic_bypass_total{reason="disabled"}``), never a
   fallback for an invalid config.

**What this class deliberately does not carry yet:** listen address, TLS, auth,
decision-log sink, and the deployment shape (design doc §8's E vs F). Those are
Phase 10.5's own decisions, made against a real request path. Only the fields
whose semantics the design doc has already fixed are frozen here.
"""

from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS",
    "PLACEHOLDER_ENCODER_TIMEOUT_MS",
    "PLACEHOLDER_GUARD_TIMEOUT_MS",
    "PLACEHOLDER_REGISTRY_TIMEOUT_MS",
    "GatewayConfig",
    "NamespaceSource",
    "load",
]


# Every PLACEHOLDER_* value below is a shape-filler, not a measurement, and is
# named so that no reader can mistake one for a design conclusion.
#
# Design doc §19 is explicit that the bypass threshold "is explicitly not
# hardcoded from the prior report's unsupported 512-token figure -- it ships as
# a configuration default pending Phase 10.8 data, not as a design conclusion."
# 512 appears here only because the field needs *a* value and that is the
# number the superseded report used; it has no benchmark behind it. Phase 10.8
# produces the real one, and until it does, the honest reading of this default
# is "unknown".
PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS = 512

# Per-tier latency budgets (design doc §17's "past a budget", risk register row
# 14's "enforced with real timeouts, not aspirational"). No tier has been
# measured; these are round numbers that keep the fail-open path bounded, to be
# replaced with Phase 10.8's measurements.
PLACEHOLDER_REGISTRY_TIMEOUT_MS = 50
PLACEHOLDER_ENCODER_TIMEOUT_MS = 100
PLACEHOLDER_GUARD_TIMEOUT_MS = 50


class NamespaceSource(str, Enum):
    """Where the gateway learns a request's namespace.

    Design doc §15: namespace resolution is "an application/deployment-layer
    concern the gateway takes as an input (e.g., an API key, a routing rule) --
    not something this design invents". These are the shapes that input can
    take; the gateway reuses whatever tenant identity the application already
    established, and never derives one from the prompt.
    """

    API_KEY = "api_key"
    HEADER = "header"
    ROUTE = "route"
    STATIC = "static"


class GatewayConfig(BaseModel):
    """The gateway's configuration shape.

    Field types are checked on construction (that is pydantic's doing, not
    domain validation); the semantic rules in this module's docstring are
    Phase 10.5's ``validate()``.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", validate_default=True)

    enabled: bool = Field(
        default=True,
        description=(
            "Master switch. False makes the gateway a pass-through proxy and "
            "is counted as pulsekv_semantic_bypass_total{reason='disabled'} "
            "(design doc §18), never silently. Phase 10.5 may revisit the "
            "default when it settles the deployment shape."
        ),
    )
    namespace_source: NamespaceSource = Field(
        description="Required, no default -- see this module's docstring, item 1."
    )
    namespace_header: Optional[str] = Field(
        default=None,
        description="Header carrying the namespace when namespace_source=HEADER.",
    )
    static_namespace: Optional[str] = Field(
        default=None,
        description=(
            "The single namespace served when namespace_source=STATIC. A "
            "single-tenant deployment, not a global namespace: it still scopes "
            "every retrieval (design doc §15)."
        ),
    )
    registry_dsn: str = Field(
        description=(
            "Connection target for the Canonical Context Registry. An opaque "
            "string here on purpose -- the store is Phase 10.1's choice, and "
            "design doc §10 rules out only one option (PulseKV itself, whose "
            "NVMe tier is loss-tolerant by design and so is wrong for records "
            "that must not come back different)."
        )
    )
    upstream_url: str = Field(
        description="The unmodified SGLang/vLLM endpoint requests are forwarded to."
    )
    bypass_min_eligible_tokens: int = Field(
        default=PLACEHOLDER_BYPASS_MIN_ELIGIBLE_TOKENS,
        ge=0,
        description=(
            "Skip semantic processing when a request's eligible-block token "
            "count is below this (design doc §19). Compared against "
            "ContextBlock.token_estimate, which is an estimate by design -- the "
            "gateway does not tokenize. See the placeholder constant's comment: "
            "this default is unmeasured."
        ),
    )
    registry_timeout_ms: int = Field(
        default=PLACEHOLDER_REGISTRY_TIMEOUT_MS, gt=0
    )
    encoder_timeout_ms: int = Field(default=PLACEHOLDER_ENCODER_TIMEOUT_MS, gt=0)
    guard_timeout_ms: int = Field(default=PLACEHOLDER_GUARD_TIMEOUT_MS, gt=0)

    def validate_config(self) -> List[str]:
        """Return every problem with this config, empty if it is valid.

        A list rather than a raised exception so the caller decides where the
        problems go -- the same reason ``control/internal/config``'s
        ``Warnings()`` returns a slice instead of logging. ``load`` is what
        turns a non-empty list into a refusal to start.

        Named ``validate_config`` rather than ``validate`` only because
        ``BaseModel.validate`` is pydantic's own deprecated classmethod;
        shadowing it would make the override's behavior depend on how it was
        called.
        """
        raise NotImplementedError("Phase 10.5")

    def warnings(self) -> List[str]:
        """Legal settings that will start but probably are not intended."""
        raise NotImplementedError("Phase 10.5")


def load(path: str) -> GatewayConfig:
    """Parse, default, and validate a config file, or fail without starting."""
    raise NotImplementedError("Phase 10.5")
