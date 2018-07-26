import inspect
import unittest
from enum import Enum

import signal as signalz
from dataclasses import dataclass
from typing import Tuple, Type


class RPCType(Enum):
    """The calling convention of the RPC point"""

    Repliable = 1
    """Reply is expected from the receiver (OK-RECEIVED)
    Beware this does dead-lock services when they both try to send a repliable request at the same time to each other
    """
    Durable = 2
    """we only make sure the packet is received and do not wait for reply (UNDECIDED-RECEIVED)"""
    Signalling = 3
    """we don't care if the packet is received (UNDECIDED-UNDECIDED)"""

    def __repr__(self):
        return str(self.name)


class DSL:
    pass


@dataclass
class Wrapped:
    fn: None
    conf: DSL

    def __call__(self, *args, **kwargs):
        return self.fn(self.__self__, *args, **kwargs)

    def __get__(self, instance, owner):
        self.__self__ = instance
        return self


@dataclass
class Wrapper:
    type: Type[DSL]

    def __call__(self, _cls=None, **kwargs):
        print(_cls, kwargs)
        print(inspect.ismethod(_cls))
        if _cls is None:
            print('b')

            def wrapper(fn):
                return Wrapped(fn, self.type(**kwargs))

            return wrapper

        else:
            print('a')
            return Wrapped(_cls, self.type())




@dataclass
class RPC(DSL):
    type: RPCType = RPCType.Repliable


rpc = Wrapper(RPC)


@dataclass
class Regular(DSL):
    initial: float = 0
    """Initial wait time in seconds"""
    tick: bool = False
    """Run this function on every tick (this should affect the wait times)"""


@dataclass
class Signal(DSL):
    codes: Tuple[int] = (signalz.SIGTERM, signalz.SIGINT)
    """Connect to this signal number"""


class Startup(DSL):
    pass


class SocketIO(DSL):
    pass


class RPCFr:
    @rpc
    def method_a(self):
        pass

    @rpc(type=RPCType.Durable)
    def method_b(self):
        pass


class TestMethodDSL(unittest.TestCase):
    def test_wrapper(self):
        x = RPCFr()

        x.method_a()
        print(dir(x.method_a))
        print(inspect.ismethod(x.method_a))
