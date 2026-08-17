from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

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

class PrefixMatchRequest(_message.Message):
    __slots__ = ("prefix",)
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    prefix: bytes
    def __init__(self, prefix: _Optional[bytes] = ...) -> None: ...

class PrefixMatchResponse(_message.Message):
    __slots__ = ("key", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: bytes
    value: bytes
    def __init__(self, key: _Optional[bytes] = ..., value: _Optional[bytes] = ...) -> None: ...

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
