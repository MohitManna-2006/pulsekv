"""Health-check gRPC client — the Phase 0 proof that Python can talk to the cluster.

Phase 0's exit criteria ask for an adapter client that calls
``AdapterService.HealthCheck``. Nothing implements ``AdapterService`` yet: its
server side arrives in Phase 7, when the adapters actually have something to
call. So :func:`check_adapter_service` exists and is correct, but the concrete
proof that the Python side can speak gRPC to the Go side is
:func:`check_cluster_metadata`, which calls
``ClusterMetadataService.HealthCheck`` on the running control plane.

That substitution is deliberate and is called out in
``docs/pulsekv-v2-phase0-summary.md``. :func:`check_adapter_service` against a
Phase 0 cluster returns ``ok=False`` with ``UNIMPLEMENTED``, which is the
honest answer rather than a skipped check.

Command line::

    python -m pulsekv_adapters.health_client --address 127.0.0.1:7000
    python -m pulsekv_adapters.health_client --address 127.0.0.1:7100 --service node
    pulsekv-health --address 127.0.0.1:7000 --service adapter

Exits 0 when the checked service reports healthy, 1 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass

import grpc

try:
    from .gen import adapter_pb2, adapter_pb2_grpc
    from .gen import metadata_pb2, metadata_pb2_grpc
    from .gen import node_pb2, node_pb2_grpc
except ImportError as exc:  # pragma: no cover - only hit on a broken install
    raise ImportError(
        "pulsekv_adapters generated stubs are missing. "
        "Regenerate them with deploy/gen-proto.sh (see proto/README.md)."
    ) from exc

DEFAULT_TIMEOUT_SECONDS = 2.0

#: Every service in the v2 contract, for the --service flag.
SERVICES = ("metadata", "node", "adapter")


class HealthCheckError(RuntimeError):
    """Raised for failures that are not a normal unhealthy response.

    A refused connection or an ``UNIMPLEMENTED`` status is a *result*, not an
    error — it gets reported as ``ok=False``. This is reserved for misuse, such
    as an unknown service name.
    """


@dataclass(frozen=True)
class HealthResult:
    """Outcome of one health check."""

    service: str
    address: str
    ok: bool
    detail: str = ""
    #: gRPC status code name when the RPC failed, else ``None``.
    status_code: str | None = None

    def __str__(self) -> str:
        mark = "ok  " if self.ok else "FAIL"
        suffix = f" [{self.status_code}]" if self.status_code else ""
        return f"{mark} {self.service:<9} {self.address:<22} {self.detail}{suffix}"


def _channel(address: str) -> grpc.Channel:
    # Insecure on purpose: Phase 0's cluster is a set of loopback processes on
    # one machine. TLS/mTLS between components is a hardening item for Phase 9,
    # not something to half-do here.
    return grpc.insecure_channel(address)


def _failed(service: str, address: str, exc: grpc.RpcError) -> HealthResult:
    """Turn an RpcError into a reportable result rather than a traceback."""
    code = exc.code() if isinstance(exc, grpc.Call) else None
    detail = exc.details() if isinstance(exc, grpc.Call) else str(exc)
    return HealthResult(
        service=service,
        address=address,
        ok=False,
        detail=detail or "rpc failed",
        status_code=code.name if code is not None else None,
    )


def check_cluster_metadata(
    address: str, timeout: float = DEFAULT_TIMEOUT_SECONDS
) -> HealthResult:
    """Call ``ClusterMetadataService.HealthCheck`` on the Go control plane.

    This is the Phase 0 cross-language proof: Python client, generated stubs,
    Go server, real data on the wire.
    """
    with _channel(address) as channel:
        stub = metadata_pb2_grpc.ClusterMetadataServiceStub(channel)
        try:
            resp = stub.HealthCheck(
                metadata_pb2.HealthCheckRequest(), timeout=timeout
            )
        except grpc.RpcError as exc:
            return _failed("metadata", address, exc)

    return HealthResult(
        service="metadata",
        address=address,
        ok=resp.ok,
        detail=f"ok={resp.ok} uptime={resp.uptime_seconds}s",
    )


def check_node(address: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> HealthResult:
    """Call ``NodeService.HealthCheck`` on a C++ shim data-plane node.

    Not required by Phase 0, but it costs nothing and proves the second
    language boundary — Python to C++ — as well as the first.
    """
    with _channel(address) as channel:
        stub = node_pb2_grpc.NodeServiceStub(channel)
        try:
            resp = stub.HealthCheck(node_pb2.HealthCheckRequest(), timeout=timeout)
        except grpc.RpcError as exc:
            return _failed("node", address, exc)

    return HealthResult(
        service="node",
        address=address,
        ok=resp.ok,
        detail=f"ok={resp.ok} node_id={resp.node_id} uptime={resp.uptime_seconds}s",
    )


def check_adapter_service(
    address: str, timeout: float = DEFAULT_TIMEOUT_SECONDS
) -> HealthResult:
    """Call ``AdapterService.HealthCheck``.

    Nothing serves ``AdapterService`` in Phase 0 — the server side arrives in
    Phase 7 — so against a Phase 0 cluster this returns ``ok=False`` with
    ``UNIMPLEMENTED``. That is the correct answer, and it exercises the
    generated adapter stubs end to end in the meantime.
    """
    with _channel(address) as channel:
        stub = adapter_pb2_grpc.AdapterServiceStub(channel)
        try:
            resp = stub.HealthCheck(adapter_pb2.HealthCheckRequest(), timeout=timeout)
        except grpc.RpcError as exc:
            return _failed("adapter", address, exc)

    return HealthResult(
        service="adapter",
        address=address,
        ok=resp.ok,
        detail=f"ok={resp.ok}",
    )


_CHECKS = {
    "metadata": check_cluster_metadata,
    "node": check_node,
    "adapter": check_adapter_service,
}


def check(
    service: str, address: str, timeout: float = DEFAULT_TIMEOUT_SECONDS
) -> HealthResult:
    """Dispatch to the checker for ``service``."""
    try:
        fn = _CHECKS[service]
    except KeyError:
        raise HealthCheckError(
            f"unknown service {service!r}; expected one of {', '.join(SERVICES)}"
        ) from None
    return fn(address, timeout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pulsekv-health",
        description="Health-check a PulseKV v2 process over gRPC.",
    )
    parser.add_argument(
        "--address",
        default="127.0.0.1:7000",
        help="host:port to check (default: %(default)s, the control plane)",
    )
    parser.add_argument(
        "--service",
        default="metadata",
        choices=SERVICES,
        help="which service to call (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="RPC deadline in seconds (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    result = check(args.service, args.address, args.timeout)
    print(result, file=sys.stdout if result.ok else sys.stderr)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
