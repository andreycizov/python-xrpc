from dataclasses import dataclass
from typing import TypeVar, Generic, Type

from xrpc.dsl import rpc, regular
from xrpc.error import TerminationException
from xrpc.util import time_now

T = TypeVar('T')


@dataclass
class Data(Generic[T]):
    val: T


class GenericRPC(Generic[T]):
    def __init__(self, cls: Type[T], timeout=20.):
        self.cls = cls
        self.has_replied_a = False
        self.has_replied_b = False
        self.started = time_now()
        self.timeout = timeout

    @rpc()
    def process(self, val: T) -> Data[T]:
        self.has_replied_a = True
        return Data(val)

    @rpc()
    def process_blunt(self, val: T) -> T:
        self.has_replied_b = True
        return val

    @regular(tick=True)
    def tick(self) -> float:
        if (time_now() - self.started).total_seconds() > self.timeout:
            raise TimeoutError('Timeout')

        if all([self.has_replied_b, self.has_replied_a]):
            raise TerminationException()

        return 0.5
