from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class UnaryLimit(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    UNARY_LIMIT_UNSPECIFIED: _ClassVar[UnaryLimit]
    UNARY_VALUE_LIMIT_BYTES: _ClassVar[UnaryLimit]
UNARY_LIMIT_UNSPECIFIED: UnaryLimit
UNARY_VALUE_LIMIT_BYTES: UnaryLimit

class HealthCheckRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class HealthCheckResponse(_message.Message):
    __slots__ = ("ok", "node_id", "uptime_seconds")
    OK_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    UPTIME_SECONDS_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    node_id: str
    uptime_seconds: int
    def __init__(self, ok: _Optional[bool] = ..., node_id: _Optional[str] = ..., uptime_seconds: _Optional[int] = ...) -> None: ...

class GetRequest(_message.Message):
    __slots__ = ("key",)
    KEY_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    def __init__(self, key: _Optional[bytes] = ...) -> None: ...

class GetResponse(_message.Message):
    __slots__ = ("found", "value")
    FOUND_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    found: bool
    value: bytes
    def __init__(self, found: _Optional[bool] = ..., value: _Optional[bytes] = ...) -> None: ...

class PutRequest(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ...) -> None: ...

class PutResponse(_message.Message):
    __slots__ = ("ok", "error")
    OK_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ok: bool
    error: str
    def __init__(self, ok: _Optional[bool] = ..., error: _Optional[str] = ...) -> None: ...

class PutChunk(_message.Message):
    __slots__ = ("key", "chunk_index", "total_chunks", "total_length", "data")
    KEY_FIELD_NUMBER: _ClassVar[int]
    CHUNK_INDEX_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_LENGTH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    chunk_index: int
    total_chunks: int
    total_length: int
    data: bytes
    def __init__(self, key: _Optional[bytes] = ..., chunk_index: _Optional[int] = ..., total_chunks: _Optional[int] = ..., total_length: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class GetChunk(_message.Message):
    __slots__ = ("chunk_index", "total_chunks", "total_length", "data")
    CHUNK_INDEX_FIELD_NUMBER: _ClassVar[int]
    TOTAL_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_LENGTH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    chunk_index: int
    total_chunks: int
    total_length: int
    data: bytes
    def __init__(self, chunk_index: _Optional[int] = ..., total_chunks: _Optional[int] = ..., total_length: _Optional[int] = ..., data: _Optional[bytes] = ...) -> None: ...

class PrefixMatchRequest(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: bytes
    def __init__(self, prefix: _Optional[bytes] = ...) -> None: ...

class PrefixMatchResponse(_message.Message):
    __slots__ = ("key", "value", "value_omitted")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VALUE_OMITTED_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    value_omitted: bool
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ..., value_omitted: _Optional[bool] = ...) -> None: ...

class CapacityRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class CapacityResponse(_message.Message):
    __slots__ = ("resident_keys", "bytes_in_ram_tier", "bytes_in_nvme_tier")
    RESIDENT_KEYS_FIELD_NUMBER: _ClassVar[int]
    BYTES_IN_RAM_TIER_FIELD_NUMBER: _ClassVar[int]
    BYTES_IN_NVME_TIER_FIELD_NUMBER: _ClassVar[int]
    resident_keys: int
    bytes_in_ram_tier: int
    bytes_in_nvme_tier: int
    def __init__(self, resident_keys: _Optional[int] = ..., bytes_in_ram_tier: _Optional[int] = ..., bytes_in_nvme_tier: _Optional[int] = ...) -> None: ...
