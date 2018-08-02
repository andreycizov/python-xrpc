import base64
import json
import struct
import zlib
from datetime import datetime
from enum import Enum
from typing import NamedTuple, Any
from uuid import UUID, uuid4

import bitstruct
import pytz
from dataclasses import dataclass, field

from xrpc.serde.types import ISO8601
from xrpc.util import time_now

RPC_TS_FORMAT_SPEC = [
    ('year', 12, 12, 1.5, 4096),
    ('month', 4, 16, 2, 16),
    ('day', 5, 21, 2.625, 32),
    ('hour', 5, 26, 3.25, 32),
    ('minute', 6, 32, 4, 64),
    ('second', 6, 38, 4.75, 64),
    ('microsecond', 20, 58, 7.25, 1048576),
    ('_reserved', 6, 64, 8, 64),
]

RPC_TS_FMT = ''.join(f'u{x}' for _, x, *_ in RPC_TS_FORMAT_SPEC)
RPC_TS_FMT_COMPILED = bitstruct.compile(RPC_TS_FMT)
RPC_TS_COMPILED_SIZE = RPC_TS_FMT_COMPILED.calcsize() // 8
RPC_UUID_SIZE = 16
RPC_KEY_SIZE = RPC_TS_COMPILED_SIZE + RPC_UUID_SIZE


def time_pack(dt: datetime):
    timestamp = RPC_TS_FMT_COMPILED.pack(
        *(getattr(dt, x) if not x.startswith('_') else 0 for x, *_ in RPC_TS_FORMAT_SPEC)
    )

    return timestamp


def time_unpack(body: bytes) -> datetime:
    to_unpack = zip(
        (x for x, *_ in RPC_TS_FORMAT_SPEC),
        RPC_TS_FMT_COMPILED.unpack(body[:RPC_TS_COMPILED_SIZE])
    )

    return datetime(**{k: v for k, v in to_unpack if not k.startswith('_')}, tzinfo=pytz.utc)


@dataclass(eq=True, frozen=True, order=True)
class RPCKey:
    timestamp: datetime = field(default_factory=time_now)
    uuid: UUID = field(default_factory=uuid4)

    @classmethod
    def null(cls, timestamp: datetime):
        return RPCKey(timestamp, UUID(bytes=b'\x00' * 16))

    @classmethod
    def new(cls):
        return RPCKey(time_now(), uuid4())

    def pack(self) -> bytes:
        assert self.timestamp.tzinfo == pytz.utc

        timestamp = time_pack(self.timestamp)

        uuid = self.uuid.bytes

        return timestamp + uuid

    def pack_str(self) -> str:
        return base64.b64encode(self.pack(), altchars=b'_-').decode()

    @classmethod
    def unpack(cls, body: bytes):
        timestamp = time_unpack(body[:RPC_TS_COMPILED_SIZE])
        uuid = UUID(bytes=body[RPC_TS_COMPILED_SIZE:])

        return RPCKey(timestamp=timestamp, uuid=uuid)

    @classmethod
    def unpack_str(cls, val: str) -> 'RPCKey':
        return cls.unpack(base64.b64decode(val, altchars=b'_-'))

    def __repr__(self):
        return f'{self.__class__.__name__}({self.timestamp:{ISO8601}} {self.uuid.hex})'


class UUKey(RPCKey):
    def __repr__(self):
        return f'{self.__class__.__name__}({self.pack_str()})'


class RPCReply(Enum):
    ok = 'ok'
    horizon = 'hor'
    fingerprint = 'fin'
    internal = 'int'


RPC_PACKET_PACKER = struct.Struct('!BHL')
RPC_PACKET_FIXED_SIZE = RPC_PACKET_PACKER.size
RPC_PACKET_HEADER_SIZE = RPC_KEY_SIZE + RPC_PACKET_FIXED_SIZE

assert RPC_PACKET_FIXED_SIZE == 7, RPC_PACKET_PACKER.size


class RPCPacketType(Enum):
    Req = 0
    Rep = 1

    def __repr__(self):
        return f'{self.__class__.__name__}.{self.name}'


def json_pack(payload):
    try:
        return json.dumps(payload, separators=(',', ':')).encode()
    except:
        raise ValueError(f'Could not encode {payload}')


def json_unpack(payload: bytes):
    return json.loads(payload)


class RPCPacket(NamedTuple):
    key: RPCKey
    type: RPCPacketType
    name: str
    payload: Any = None
    """This must be json-serializable"""

    def pack(self) -> bytes:
        name = self.name.encode()
        payload = json_pack(self.payload)

        fixed_fields = RPC_PACKET_PACKER.pack(self.type.value, len(name), len(payload))
        body = self.key.pack() + fixed_fields + name + payload
        body = zlib.compress(body, level=9)
        return body

    @classmethod
    def unpack(cls, body: bytes):
        body = zlib.decompress(body)

        key = RPCKey.unpack(body[:RPC_KEY_SIZE])

        type_val, name_size, payload_size = RPC_PACKET_PACKER.unpack(body[RPC_KEY_SIZE:RPC_PACKET_HEADER_SIZE])
        type = RPCPacketType(type_val)
        name = body[RPC_PACKET_HEADER_SIZE:RPC_PACKET_HEADER_SIZE + name_size].decode()
        payload = body[RPC_PACKET_HEADER_SIZE + name_size:RPC_PACKET_HEADER_SIZE + name_size + payload_size]

        payload = json_unpack(payload)

        return RPCPacket(
            key,
            type,
            name,
            payload
        )
