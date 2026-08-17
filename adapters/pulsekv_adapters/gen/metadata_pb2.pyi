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
    __slots__ = ("nodes",)
    NODES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[NodeInfo]
    def __init__(self, nodes: _Optional[_Iterable[_Union[NodeInfo, _Mapping]]] = ...) -> None: ...

class GetShardMapRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetShardMapResponse(_message.Message):
    __slots__ = ("shard_to_node_id",)
    class ShardToNodeIdEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: str
        def __init__(self, key: _Optional[int] = ..., value: _Optional[str] = ...) -> None: ...
    SHARD_TO_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    shard_to_node_id: _containers.ScalarMap[int, str]
    def __init__(self, shard_to_node_id: _Optional[_Mapping[int, str]] = ...) -> None: ...
