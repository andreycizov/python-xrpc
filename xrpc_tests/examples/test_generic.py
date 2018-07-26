from time import sleep

from xrpc.client import build_wrapper, ClientConfig
from xrpc.error import HorizonPassedError
from xrpc.examples.generic import GenericRPC, Data
from xrpc.loop import EventLoop
from xrpc.service import ServiceDefn
from xrpc.transport import Transport
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def generic_a_main(addr_a):
    return GenericRPC[int], GenericRPC(int, timeout=5)


def generic_b_main(addr_b, addr_a):
    rpc = GenericRPC[int]

    t = Transport.from_url(addr_b, )

    with t:
        ts = EventLoop()
        tref = ts.transport_add(t)
        pt = ServiceDefn.from_cls(rpc)
        r: GenericRPC[int] = build_wrapper(pt, tref, addr_a, conf=ClientConfig(timeout_total=5))

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
        a = self.ps.popen(server_main, generic_a_main, addr_a)
        b = self.ps.popen(generic_b_main, addr_b, addr_a)

        self.ps.wait([b, a])
