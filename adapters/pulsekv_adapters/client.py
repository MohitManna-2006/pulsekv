"""PulseKV v2 Python Client SDK.

Provides topology discovery from ClusterMetadataService (Go control plane),
rendezvous hash shard routing, and automatic unary / chunked / bulk transport
to data nodes (C++ data plane).
"""

from __future__ import annotations

import contextlib
import hashlib
import logging
import mmap
import os
import socket
import struct
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union

import grpc

try:
    from .gen import metadata_pb2, metadata_pb2_grpc
    from .gen import node_pb2, node_pb2_grpc
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "pulsekv_adapters generated stubs are missing. "
        "Regenerate them with deploy/gen-proto.sh (see proto/README.md)."
    ) from exc

logger = logging.getLogger(__name__)

# Wire limits and defaults matching control/pkg/client and node/grpc_shim
DEFAULT_UNARY_LIMIT_BYTES = 4 * 1024 * 1024  # 4 MiB
DEFAULT_CHUNK_SIZE = 1024 * 1024  # 1 MiB
DEFAULT_MAX_MESSAGE_BYTES = 8 * 1024 * 1024  # 8 MiB
DEFAULT_TIMEOUT_SECONDS = 5.0
DEFAULT_BULK_PORT_OFFSET = 1000
DEFAULT_BULK_SOCKET_DIR = "/tmp"

# Bulk binary protocol constants matching node/grpc_shim/bulk.h
BULK_MAGIC = 0x504B4231  # "PKB1"
BULK_HEADER_BYTES = 32
BULK_OP_PING = 1
BULK_OP_GET = 2
BULK_OP_PUT = 3

BULK_STATUS_OK = 0
BULK_STATUS_NOT_FOUND = 1
BULK_STATUS_ERROR = 2
BULK_STATUS_OK_MEMFD = 3

BULK_FLAG_ACCEPT_MEMFD = 1 << 0

# 64-bit FNV-1a constants matching control/internal/router
FNV1A_64_OFFSET_BASIS = 0xCBF29CE484222325
FNV1A_64_PRIME = 0x100000001B3
MASK64 = 0xFFFFFFFFFFFFFFFF


def fnv1a_64(data: bytes) -> int:
    """Compute 64-bit FNV-1a hash over raw bytes."""
    h = FNV1A_64_OFFSET_BASIS
    for b in data:
        h ^= b
        h = (h * FNV1A_64_PRIME) & MASK64
    return h


def mix64(x: int) -> int:
    """SplitMix64 finalizer matching control/internal/router/router.go."""
    x &= MASK64
    x ^= (x >> 30)
    x = (x * 0xBF58476D1CE4E5B9) & MASK64
    x ^= (x >> 27)
    x = (x * 0x94D049BB133111EB) & MASK64
    x ^= (x >> 31)
    return x & MASK64


def shard_for_key(key: bytes, shard_count: int) -> int:
    """Map a key to a cluster shard index (0 <= shard < shard_count)."""
    if shard_count <= 0:
        return 0
    return fnv1a_64(key) % shard_count


def _write_uint32(buf: bytearray, val: int) -> None:
    buf.extend(struct.pack(">I", val & 0xFFFFFFFF))


def _write_bytes(buf: bytearray, data: bytes) -> None:
    _write_uint32(buf, len(data))
    buf.extend(data)


def compute_topology_fingerprint(
    shard_count: int,
    replication_factor: int,
    nodes: Dict[str, str],
    shard_map: Dict[int, str],
    owners: Dict[int, Tuple[str, List[str]]],
) -> bytes:
    """Compute canonical SHA-256 topology fingerprint matching control/internal/topology."""
    buf = bytearray()
    buf.extend(b"pulsekv-topology-v2\x00")
    _write_uint32(buf, shard_count)
    _write_uint32(buf, replication_factor)

    sorted_node_ids = sorted(nodes.keys())
    _write_uint32(buf, len(sorted_node_ids))
    for node_id in sorted_node_ids:
        _write_bytes(buf, node_id.encode("utf-8"))
        _write_bytes(buf, nodes[node_id].encode("utf-8"))

    _write_uint32(buf, len(shard_map))
    for s in range(shard_count):
        if s in shard_map:
            _write_uint32(buf, s)
            _write_bytes(buf, shard_map[s].encode("utf-8"))

    _write_uint32(buf, len(owners))
    for s in range(shard_count):
        if s in owners:
            primary, replicas = owners[s]
            _write_uint32(buf, s)
            _write_bytes(buf, primary.encode("utf-8"))
            _write_uint32(buf, len(replicas))
            for r in replicas:
                _write_bytes(buf, r.encode("utf-8"))

    return hashlib.sha256(buf).digest()


def encode_bulk_header(code: int, flags: int, aux_len: int, value_len: int) -> bytes:
    """Encode 32-byte binary bulk transport header matching node/grpc_shim/bulk.cc."""
    # Wire layout:
    # 0..3: magic (uint32)
    # 4: code (uint8)
    # 5: flags (uint8)
    # 6..7: reserved (uint16)
    # 8..11: aux_len (uint32)
    # 12..19: value_len (uint64)
    # 20..31: reserved (12 bytes zeros)
    return struct.pack(">IBBHIQ12x", BULK_MAGIC, code, flags, 0, aux_len, value_len)


def decode_bulk_header(data: bytes) -> Optional[Tuple[int, int, int, int]]:
    """Decode 32-byte binary bulk transport header. Returns (code, flags, aux_len, value_len)."""
    if len(data) < BULK_HEADER_BYTES:
        return None
    magic, code, flags, _, aux_len, value_len = struct.unpack(
        ">IBBHIQ12x", data[:BULK_HEADER_BYTES]
    )
    if magic != BULK_MAGIC:
        return None
    return code, flags, aux_len, value_len


def _recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    """Read exactly n bytes from socket."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


@dataclass(frozen=True)
class TopologySnapshot:
    """Validated cluster routing snapshot."""

    generation: int
    fingerprint: bytes
    shard_count: int
    shard_map: Dict[int, str]  # shard_idx -> node_id
    nodes: Dict[str, str]  # node_id -> address (host:port)
    owners: Dict[int, Tuple[str, List[str]]]  # shard_idx -> (primary, replicas)
    replication_factor: int

    def owner_addresses(self) -> List[str]:
        """Return unique sorted node addresses currently owning at least one shard."""
        unique_addrs = {
            self.nodes[node_id] for node_id in self.shard_map.values() if node_id in self.nodes
        }
        return sorted(unique_addrs)

    def owner_for_key(self, key: bytes) -> Optional[Tuple[str, str]]:
        """Return (node_id, node_address) owning the given key."""
        if not self.shard_map or self.shard_count <= 0:
            return None
        shard = shard_for_key(key, self.shard_count)
        node_id = self.shard_map.get(shard)
        if not node_id:
            return None
        address = self.nodes.get(node_id)
        if not address:
            return None
        return node_id, address


class PulseKVClientError(RuntimeError):
    """Raised for fatal PulseKV client errors."""


class PulseKVClient:
    """Generic concurrency-safe client for PulseKV v2 cluster."""

    def __init__(
        self,
        control_plane_addresses: Union[str, Sequence[str]],
        refresh_interval: float = 5.0,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        enable_bulk: bool = True,
        bulk_socket_dir: str = DEFAULT_BULK_SOCKET_DIR,
        bulk_port_offset: int = DEFAULT_BULK_PORT_OFFSET,
    ):
        if isinstance(control_plane_addresses, str):
            self._cp_addrs = [
                a.strip() for a in control_plane_addresses.split(",") if a.strip()
            ]
        else:
            self._cp_addrs = list(control_plane_addresses)
        if not self._cp_addrs:
            raise ValueError("control_plane_addresses must not be empty")

        self._refresh_interval = refresh_interval
        self._timeout = timeout
        self._enable_bulk = enable_bulk
        self._bulk_socket_dir = bulk_socket_dir
        self._bulk_port_offset = bulk_port_offset

        self._lock = threading.RLock()
        self._closed = False
        self._preferred_cp = 0

        self._topology: Optional[TopologySnapshot] = None
        self._node_channels: Dict[str, grpc.Channel] = {}
        self._node_stubs: Dict[str, node_pb2_grpc.NodeServiceStub] = {}

        # Initial eager metadata refresh
        self._refresh_topology()

        # Background refresh thread
        self._stop_event = threading.Event()
        self._refresh_thread: Optional[threading.Thread] = None
        if self._refresh_interval > 0:
            self._refresh_thread = threading.Thread(
                target=self._background_refresh_loop, daemon=True, name="pulsekv-refresh"
            )
            self._refresh_thread.start()

    def _create_grpc_channel(self, address: str) -> grpc.Channel:
        options = [
            ("grpc.max_send_message_length", DEFAULT_MAX_MESSAGE_BYTES),
            ("grpc.max_receive_message_length", DEFAULT_MAX_MESSAGE_BYTES),
        ]
        return grpc.insecure_channel(address, options=options)

    def _refresh_topology(self) -> None:
        """Fetch and validate coherent topology snapshot from control plane."""
        last_err = None
        with self._lock:
            start_idx = self._preferred_cp
            num_cps = len(self._cp_addrs)

            for attempt in range(num_cps):
                idx = (start_idx + attempt) % num_cps
                addr = self._cp_addrs[idx]
                try:
                    channel = self._create_grpc_channel(addr)
                    stub = metadata_pb2_grpc.ClusterMetadataServiceStub(channel)

                    # Fetch NodeList and ShardMap
                    nodes_resp = stub.GetNodeList(
                        metadata_pb2.GetNodeListRequest(), timeout=self._timeout
                    )
                    shards_resp = stub.GetShardMap(
                        metadata_pb2.GetShardMapRequest(), timeout=self._timeout
                    )
                    channel.close()

                    # Validate fingerprint coherence
                    node_fp = nodes_resp.topology_fingerprint
                    shard_fp = shards_resp.topology_fingerprint
                    if node_fp and shard_fp and node_fp != shard_fp:
                        continue  # publisher churned between calls, retry

                    nodes_dict = {n.node_id: n.address for n in nodes_resp.nodes if n.alive}
                    shard_map = dict(shards_resp.shard_to_node_id)
                    shard_count = shards_resp.shard_count or len(shard_map)

                    owners_dict: Dict[int, Tuple[str, List[str]]] = {}
                    for s, owners in shards_resp.shard_to_owners.items():
                        owners_dict[s] = (owners.primary, list(owners.replicas))

                    snapshot = TopologySnapshot(
                        generation=shards_resp.topology_generation,
                        fingerprint=shard_fp,
                        shard_count=shard_count,
                        shard_map=shard_map,
                        nodes=nodes_dict,
                        owners=owners_dict,
                        replication_factor=shards_resp.replication_factor,
                    )

                    self._topology = snapshot
                    self._preferred_cp = idx
                    return
                except Exception as e:
                    last_err = e
                    logger.debug(f"Failed to fetch topology from {addr}: {e}")

        if last_err and not self._topology:
            raise PulseKVClientError(
                f"Failed to load PulseKV topology from any control plane: {last_err}"
            )

    def _background_refresh_loop(self) -> None:
        while not self._stop_event.wait(self._refresh_interval):
            try:
                self._refresh_topology()
            except Exception as e:
                logger.debug(f"Background topology refresh failed: {e}")

    def _get_node_stub(self, address: str) -> node_pb2_grpc.NodeServiceStub:
        with self._lock:
            if self._closed:
                raise PulseKVClientError("Client is closed")
            if address not in self._node_stubs:
                ch = self._create_grpc_channel(address)
                self._node_channels[address] = ch
                self._node_stubs[address] = node_pb2_grpc.NodeServiceStub(ch)
            return self._node_stubs[address]

    def _to_bytes_key(self, key: Union[str, bytes]) -> bytes:
        if isinstance(key, str):
            return key.encode("utf-8")
        return bytes(key)

    # -------------------------------------------------------------------------
    # Bulk transport fast-path
    # -------------------------------------------------------------------------

    def _try_bulk_get(self, node_address: str, key: bytes) -> Optional[Tuple[Optional[bytes], bool]]:
        """Attempt to read value over bulk Unix/TCP socket. Returns (val, found) or None on fallback."""
        if not self._enable_bulk:
            return None

        host, port_str = node_address.split(":")
        port = int(port_str)
        unix_path = os.path.join(
            self._bulk_socket_dir, f"pulsekv-bulk-{host}-{port}.sock"
        )

        sock = None
        try:
            if os.path.exists(unix_path):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self._timeout)
                sock.connect(unix_path)
                use_unix = True
            else:
                # Try bulk TCP port
                bulk_port = port + self._bulk_port_offset
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self._timeout)
                sock.connect((host, bulk_port))
                use_unix = False

            flags = BULK_FLAG_ACCEPT_MEMFD if use_unix else 0
            req_hdr = encode_bulk_header(BULK_OP_GET, flags, len(key), 0)
            sock.sendall(req_hdr + key)

            # Receive 32-byte response header
            fds: List[int] = []
            if use_unix:
                msg_buf = bytearray(BULK_HEADER_BYTES)
                cmsg_buf_len = socket.CMSG_LEN(struct.calcsize("i"))
                bytes_recvd, ancdata, _, _ = sock.recvmsg([msg_buf], cmsg_buf_len)
                if bytes_recvd < BULK_HEADER_BYTES:
                    # try reading rest of header
                    rest = _recv_exact(sock, BULK_HEADER_BYTES - bytes_recvd)
                    if not rest:
                        return None
                    msg_buf[bytes_recvd:] = rest
                for cmsg_level, cmsg_type, cmsg_data in ancdata:
                    if (
                        cmsg_level == socket.SOL_SOCKET
                        and cmsg_type == socket.SCM_RIGHTS
                    ):
                        fd = struct.unpack("i", cmsg_data[: struct.calcsize("i")])[0]
                        fds.append(fd)
                resp_hdr = bytes(msg_buf)
            else:
                raw = _recv_exact(sock, BULK_HEADER_BYTES)
                if not raw:
                    return None
                resp_hdr = raw

            decoded = decode_bulk_header(resp_hdr)
            if not decoded:
                for fd in fds:
                    os.close(fd)
                return None

            status, rflags, aux_len, val_len = decoded

            if status == BULK_STATUS_NOT_FOUND:
                for fd in fds:
                    os.close(fd)
                return None, False

            elif status == BULK_STATUS_OK_MEMFD:
                if not fds:
                    return None
                memfd = fds[0]
                try:
                    if val_len == 0:
                        return b"", True
                    mm = mmap.mmap(memfd, val_len, access=mmap.ACCESS_READ)
                    val = mm.read(val_len)
                    mm.close()
                    return val, True
                finally:
                    os.close(memfd)

            elif status == BULK_STATUS_OK:
                for fd in fds:
                    os.close(fd)
                if val_len == 0:
                    return b"", True
                val_data = _recv_exact(sock, val_len)
                if val_data is None:
                    return None
                return val_data, True

            else:
                for fd in fds:
                    os.close(fd)
                if aux_len > 0:
                    _recv_exact(sock, aux_len)
                return None
        except Exception:
            return None
        finally:
            if sock:
                with contextlib.suppress(Exception):
                    sock.close()

    def _try_bulk_put(self, node_address: str, key: bytes, value: bytes) -> Optional[bool]:
        """Attempt to write value over bulk socket. Returns True on success, None on fallback."""
        if not self._enable_bulk:
            return None

        host, port_str = node_address.split(":")
        port = int(port_str)
        unix_path = os.path.join(
            self._bulk_socket_dir, f"pulsekv-bulk-{host}-{port}.sock"
        )

        sock = None
        try:
            if os.path.exists(unix_path):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(self._timeout)
                sock.connect(unix_path)
            else:
                bulk_port = port + self._bulk_port_offset
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self._timeout)
                sock.connect((host, bulk_port))

            req_hdr = encode_bulk_header(BULK_OP_PUT, 0, len(key), len(value))
            sock.sendall(req_hdr + key + value)

            resp_hdr = _recv_exact(sock, BULK_HEADER_BYTES)
            if not resp_hdr:
                return None
            decoded = decode_bulk_header(resp_hdr)
            if not decoded:
                return None
            status, rflags, aux_len, val_len = decoded
            if status == BULK_STATUS_OK:
                return True
            if aux_len > 0:
                _recv_exact(sock, aux_len)
            return None
        except Exception:
            return None
        finally:
            if sock:
                with contextlib.suppress(Exception):
                    sock.close()

    # -------------------------------------------------------------------------
    # Public Client SDK API
    # -------------------------------------------------------------------------

    def get(self, key: Union[str, bytes]) -> Tuple[Optional[bytes], bool]:
        """Get value for key. Returns (value, found). found is False on a miss."""
        key_bytes = self._to_bytes_key(key)
        with self._lock:
            if not self._topology:
                self._refresh_topology()
            mapping = self._topology.owner_for_key(key_bytes) if self._topology else None

        if not mapping:
            raise PulseKVClientError("No live data nodes available for routing")

        node_id, address = mapping

        # Try bulk transport fast-path if enabled
        if self._enable_bulk:
            bulk_res = self._try_bulk_get(address, key_bytes)
            if bulk_res is not None:
                val, found = bulk_res
                return (val, True) if found else (None, False)

        # gRPC path
        stub = self._get_node_stub(address)
        try:
            resp = stub.Get(node_pb2.GetRequest(key=key_bytes), timeout=self._timeout)
            if not resp.found:
                return None, False
            return resp.value, True
        except grpc.RpcError as e:
            # Check for FAILED_PRECONDITION (value > 4 MiB)
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                # Fallback to GetChunked
                chunks = []
                for chunk in stub.GetChunked(
                    node_pb2.GetRequest(key=key_bytes), timeout=self._timeout
                ):
                    chunks.append(chunk.data)
                if not chunks:
                    return None, False
                return b"".join(chunks), True
            raise PulseKVClientError(
                f"Get RPC failed on node {address}: {e.details()}"
            ) from e

    def exist(self, key: Union[str, bytes]) -> bool:
        """Check if key exists in the cluster."""
        val, found = self.get(key)
        return found

    def exists(self, key: Union[str, bytes]) -> bool:
        """Alias for exist(key)."""
        return self.exist(key)

    def set(
        self,
        key: Union[str, bytes],
        value: Union[bytes, bytearray, memoryview],
        require_replica_acks: int = 0,
    ) -> bool:
        """Store key-value pair in the cluster."""
        key_bytes = self._to_bytes_key(key)
        val_bytes = bytes(value)

        with self._lock:
            if not self._topology:
                self._refresh_topology()
            mapping = self._topology.owner_for_key(key_bytes) if self._topology else None

        if not mapping:
            raise PulseKVClientError("No live data nodes available for routing")

        node_id, address = mapping

        # Try bulk transport if unary and no sync replica acks required
        if self._enable_bulk and require_replica_acks == 0:
            bulk_res = self._try_bulk_put(address, key_bytes, val_bytes)
            if bulk_res is True:
                return True

        # gRPC path
        stub = self._get_node_stub(address)
        try:
            if len(val_bytes) <= DEFAULT_UNARY_LIMIT_BYTES:
                resp = stub.Put(
                    node_pb2.PutRequest(
                        key=key_bytes,
                        value=val_bytes,
                        require_replica_acks=require_replica_acks,
                    ),
                    timeout=self._timeout,
                )
                return resp.ok
            else:
                # Streaming PutChunked
                def chunk_generator() -> Iterator[node_pb2.PutChunk]:
                    total_chunks = (
                        len(val_bytes) + DEFAULT_CHUNK_SIZE - 1
                    ) // DEFAULT_CHUNK_SIZE
                    for i in range(total_chunks):
                        lo = i * DEFAULT_CHUNK_SIZE
                        hi = min(lo + DEFAULT_CHUNK_SIZE, len(val_bytes))
                        yield node_pb2.PutChunk(
                            key=key_bytes if i == 0 else b"",
                            chunk_index=i,
                            total_chunks=total_chunks,
                            total_length=len(val_bytes),
                            data=val_bytes[lo:hi],
                        )

                resp = stub.PutChunked(chunk_generator(), timeout=self._timeout)
                return resp.ok
        except grpc.RpcError as e:
            raise PulseKVClientError(
                f"Put RPC failed on node {address}: {e.details()}"
            ) from e

    def put(
        self, key: Union[str, bytes], value: Union[bytes, bytearray, memoryview]
    ) -> bool:
        """Alias for set(key, value)."""
        return self.set(key, value)

    def prefix_match(self, prefix: Union[str, bytes]) -> Dict[bytes, bytes]:
        """Return all matching key-value pairs across the cluster."""
        p_bytes = self._to_bytes_key(prefix)
        with self._lock:
            if not self._topology:
                self._refresh_topology()
            addresses = self._topology.owner_addresses() if self._topology else []

        matched_keys: Dict[bytes, bytes] = {}
        for addr in addresses:
            stub = self._get_node_stub(addr)
            try:
                for match in stub.PrefixMatch(
                    node_pb2.PrefixMatchRequest(prefix=p_bytes),
                    timeout=self._timeout,
                ):
                    key = match.key
                    if match.value_omitted:
                        val, found = self.get(key)
                        if found and val is not None:
                            matched_keys[key] = val
                    else:
                        matched_keys[key] = match.value
            except grpc.RpcError as e:
                raise PulseKVClientError(
                    f"PrefixMatch scan failed on node {addr}: {e.details()}"
                ) from e

        return matched_keys

    def close(self) -> None:
        """Close all connections and stop background threads."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._stop_event.set()
            for ch in self._node_channels.values():
                ch.close()
            self._node_channels.clear()
            self._node_stubs.clear()

    def __enter__(self) -> "PulseKVClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
