from contextlib import contextmanager
from typing import TypeVar, Type, ContextManager

from time import sleep

from xrpc.client import build_wrapper, ClientConfig
from xrpc.error import HorizonPassedError
from xrpc.examples.generic import GenericRPC, Data
from xrpc.service import ServiceDefn
from xrpc.transport import Transport, RPCTransportStack
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def run_times(addr_a):
    return GenericRPC[int], GenericRPC(int, timeout=5)


T = TypeVar('T')


@contextmanager
def build_ts(rpc: Type[T], addr='udp://127.0.0.1:7483', conf=ClientConfig(timeout_total=5)) -> ContextManager[T]:
    t = Transport.from_url('udp://127.0.0.1')

    with t:
        ts = RPCTransportStack([t])
        pt = ServiceDefn.from_obj(rpc)
        r: T = build_wrapper(pt, ts, addr, conf=conf)

        yield r


def run_times_2(addr_b, addr_a):
    rpc = GenericRPC[int]

    t = Transport.from_url(addr_b, )

    with t:
        ts = RPCTransportStack([t])
        pt = ServiceDefn.from_obj(rpc)
        r: GenericRPC[int] = build_wrapper(pt, ts, addr_a, conf=ClientConfig(timeout_total=5))

        while True:
            try:
                a = r.process(5)

                assert a == Data(5), a
                break
            except HorizonPassedError:
                sleep(0)

        while True:
            try:
                a = r.process_blunt(5)

                assert a == 5, a
                return
            except HorizonPassedError:
                sleep(0)


class TestGenericServer(ProcessHelperCase):

    def test_udp(self):
        addr_a = 'udp://127.0.0.1:7483'
        addr_b = 'udp://'
        a = self.ps.popen(server_main, run_times, addr_a)
        b = self.ps.popen(run_times_2, addr_b, addr_a)

        self.ps.wait([b, a])
