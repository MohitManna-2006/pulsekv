from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HealthCheckRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("ok", "uptime_seconds")
    OK_FIELD_NUMBER: _ClassVar[int]
    UPTIME_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    uptime_seconds: int
    def __init__(self, ok: _Optional[bool] = ..., uptime_seconds: _Optional[int] = ...) -> None: ...

class NodeInfo(_message.Message):
    __slots__ = ("node_id", "address", "alive")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    node_id: str
    address: str
    alive: bool
    def __init__(self, node_id: _Optional[str] = ..., address: _Optional[str] = ..., alive: _Optional[bool] = ...) -> None: ...

class GetNodeListRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetNodeListResponse(_message.Message):
    __slots__ = ("nodes", "topology_generation", "topology_fingerprint")
    NODES_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[NodeInfo]
    topology_generation: int
    topology_fingerprint: bytes
    def __init__(self, nodes: _Optional[_Iterable[_Union[NodeInfo, _Mapping]]] = ..., topology_generation: _Optional[int] = ..., topology_fingerprint: _Optional[bytes] = ...) -> None: ...

class GetShardMapRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ShardOwners(_message.Message):
    __slots__ = ("primary", "replicas")
    PRIMARY_FIELD_NUMBER: _ClassVar[int]
    REPLICAS_FIELD_NUMBER: _ClassVar[int]
    primary: str
    replicas: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, primary: _Optional[str] = ..., replicas: _Optional[_Iterable[str]] = ...) -> None: ...

class GetShardMapResponse(_message.Message):
    __slots__ = ("shard_to_node_id", "topology_generation", "topology_fingerprint", "shard_count", "shard_to_owners", "replication_factor", "raft_leader_id", "raft_term")
    class ShardToNodeIdEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    class ShardToOwnersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: ShardOwners
        def __init__(self, key: _Optional[int] = ..., value: _Optional[_Union[ShardOwners, _Mapping]] = ...) -> None: ...
    SHARD_TO_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGY_GENERATION_FIELD_NUMBER: _ClassVar[int]
    TOPOLOGY_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    SHARD_COUNT_FIELD_NUMBER: _ClassVar[int]
    SHARD_TO_OWNERS_FIELD_NUMBER: _ClassVar[int]
    REPLICATION_FACTOR_FIELD_NUMBER: _ClassVar[int]
    RAFT_LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    RAFT_TERM_FIELD_NUMBER: _ClassVar[int]
    shard_to_node_id: _containers.ScalarMap[int, str]
    topology_generation: int
    topology_fingerprint: bytes
    shard_count: int
    shard_to_owners: _containers.MessageMap[int, ShardOwners]
    replication_factor: int
    raft_leader_id: str
    raft_term: int
    def __init__(self, shard_to_node_id: _Optional[_Mapping[int, str]] = ..., topology_generation: _Optional[int] = ..., topology_fingerprint: _Optional[bytes] = ..., shard_count: _Optional[int] = ..., shard_to_owners: _Optional[_Mapping[int, ShardOwners]] = ..., replication_factor: _Optional[int] = ..., raft_leader_id: _Optional[str] = ..., raft_term: _Optional[int] = ...) -> None: ...
