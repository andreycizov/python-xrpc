import signal as signalz
from enum import Enum
from typing import NamedTuple, Tuple, Optional


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


ATTR_RPC = 'rpc_conf'
ATTR_REGULAR = 'rpc_regular'
ATTR_SIGNAL = 'rpc_signal'
ATTR_STARTUP = 'rpc_startup'
ATTR_SOCKETIO = 'rpc_socketio'

DEFAULT_GROUP = 'default'


class rpc(NamedTuple):
    type: RPCType = RPCType.Repliable
    """Selected calling convention for the RPC call"""
    group: str = DEFAULT_GROUP
    exc: bool = False
    """If an exception is raised while processing the packet from the transport, run this one"""

    # add 'RPC GROUPS' to allow certain ports to be used differently

    # todo: rpcs returning iterables

    def __call__(self, fn):
        setattr(fn, ATTR_RPC, self)
        return fn


class regular(NamedTuple):
    initial: Optional[float] = 0.
    """Initial wait time in seconds"""
    tick: bool = False
    """Run this function on every tick (this should affect the wait times)"""

    def __call__(self, fn):
        setattr(fn, ATTR_REGULAR, self)
        return fn


class signal(NamedTuple):
    codes: Tuple[int] = (signalz.SIGTERM, signalz.SIGINT)
    """Connect to this signal number"""

    def __call__(self, fn):
        setattr(fn, ATTR_SIGNAL, self)
        return fn


class startup(NamedTuple):
    empty: int = 0

    def __call__(self, fn):
        setattr(fn, ATTR_STARTUP, self)
        return fn


# wait for a decision on file descriptor.
class socketio():
    empty: int = 0

    def __call__(self, fn):
        setattr(fn, ATTR_SOCKETIO, self)
        return fn

# todo unhandled_exception handler
# todo startup handler
# todo exit handler
