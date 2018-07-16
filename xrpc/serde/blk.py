import hashlib
import struct
from enum import Enum
from typing import NamedTuple, Tuple, List, Optional

from dataclasses import dataclass


class ChecksumType(Enum):
    sha256 = 0

    def __repr__(self):
        return self.__class__.__name__ + '.' + self.name


CS_PACKER = struct.Struct('!B')


@dataclass
class Checksum:
    type: ChecksumType
    payload: bytes

    @classmethod
    def unpack(cls, payload: bytes) -> Tuple['Checksum', bytes]:
        cti, = CS_PACKER.unpack(payload[:CS_PACKER.size])
        t = ChecksumType(cti)

        if t == ChecksumType.sha256:
            ln = 256 // 8
            return Checksum(t, payload[CS_PACKER.size:CS_PACKER.size + ln]), payload[CS_PACKER.size + ln:]
        else:  # pragma: no cover
            assert False, t

    def pack(self) -> bytes:
        return CS_PACKER.pack(self.type.value) + self.payload

    @classmethod
    def new(cls, type: ChecksumType, payload: bytes) -> 'Checksum':
        if type == ChecksumType.sha256:
            m = hashlib.sha256()
            m.update(payload)
            return Checksum(type, m.digest())
        else:  # pragma: no cover
            assert False, type


class BlockError(Exception):
    pass


class ChecksumError(BlockError):
    def __init__(self, curr: Optional[Checksum], reqd: Checksum):
        self.curr = curr
        self.reqd = reqd

        super().__init__()

    def __str__(self):
        return f'Curr={self.curr} Required={self.reqd}'


# todo: we must be able to chain blocks.

BLK_PACKER = struct.Struct('!L')


@dataclass
class Block:
    checksum: Checksum
    payload: bytes

    @classmethod
    def new(cls, payload: bytes, checksum_type: ChecksumType = ChecksumType.sha256):
        return Block(Checksum.new(checksum_type, payload), payload)

    @classmethod
    def unpack(cls, payload: bytes) -> 'Block':
        checksum_reqd, payload = Checksum.unpack(payload)

        payload_size, = BLK_PACKER.unpack(payload[:BLK_PACKER.size])

        if len(payload) < payload_size + BLK_PACKER.size:
            raise ChecksumError(None, checksum_reqd)

        payload_bytes = payload[BLK_PACKER.size:BLK_PACKER.size + payload_size]

        checksum_curr = Checksum.new(checksum_reqd.type, payload_bytes)

        if checksum_reqd != checksum_curr:
            raise ChecksumError(checksum_curr, checksum_reqd)

        return Block(checksum_reqd, payload_bytes)

    def pack(self) -> bytes:
        cs_bytes = self.checksum.pack()

        pl_size = BLK_PACKER.pack(len(self.payload))

        return cs_bytes + pl_size + self.payload
