import logging

from xrpc.dsl import rpc, RPCType, signal
from xrpc.error import TerminationException

LIVELY = 'lively'


class Lively:
    @rpc(RPCType.Repliable, group=LIVELY)
    def is_alive(self) -> bool:
        return True


class Exceptional(Lively):
    """Handle exceptional socket conditions"""

    @rpc(exc=True)
    def ep(self, exc: Exception) -> bool:
        """Return False if the exception had not been handled gracefully"""
        if not isinstance(exc, ConnectionAbortedError):
            return False

        if len(exc.args) != 2:
            return False

        origin, reason = exc.args

        logging.getLogger(__name__).warning('Exited')

        return True

    @rpc(RPCType.Durable)
    def callme(self, x: int) -> int:
        return x + 1

    @signal()
    def exit(self):
        raise TerminationException()


class ExceptionalDropper(Exceptional):
    """Handle exceptional socket conditions"""

    @rpc(exc=True)
    def ep(self, exc: Exception) -> bool:
        pass


class ExceptionalClient(Lively):
    @rpc(exc=True)
    def ep(self, exc: ConnectionAbortedError) -> bool:
        return True

    @rpc(RPCType.Durable)
    def callme(self, x: int) -> int:
        return x + 1

    @signal()
    def exit(self):
        raise TerminationException()


class Client:
    @rpc(RPCType.Durable)
    def callme(self, x: int) -> int:
        return x + 1
