from typing import List

from xrpc.client import ClientConfig
from xrpc.dsl import rpc, signal
from xrpc.error import TerminationException
from xrpc.runtime import service, origin, sender


class Recursive:
    """Recursively call itself till exit"""

    @rpc()
    def ep(self, a: int) -> int:
        assert a > 0

        s = service(Recursive, origin(), ClientConfig(timeout_total=1., horz=False))

        if a == 1:
            return 0
        elif a > 1:
            return s.ep(a - 1)

    @signal()
    def exit(self):
        raise TerminationException()


class RecursiveA:
    """Call `RecursiveB` in order to return"""

    def __init__(self, url):
        self.url = url

    @rpc()
    def status(self) -> List[str]:
        return ['running', 'pending']

    @rpc()
    def poll(self, name: str = 'running') -> int:
        s = service(RecursiveB, self.url, ClientConfig(timeout_total=1., horz=False))

        return s.count_status(name)

    @signal()
    def exit(self):
        raise TerminationException()


class RecursiveC(RecursiveA):
    @rpc()
    def status(self) -> List[str]:
        raise ValueError()


class RecursiveB:
    """Make a call to the sender of the message"""

    @rpc()
    def count_status(self, name: str = 'running') -> int:
        s = service(RecursiveA, sender(), ClientConfig(timeout_total=1., horz=False))

        return len([x for x in s.status() if x == name])

    @signal()
    def exit(self):
        raise TerminationException()
