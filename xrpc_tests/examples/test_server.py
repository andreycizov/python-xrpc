from multiprocessing.pool import Pool
from time import sleep

from xrpc.client import build_wrapper, ClientConfig
from xrpc.error import HorizonPassedError
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.service import ServiceDefn
from xrpc.transport import RPCTransportStack, Transport
from xrpc.popen import cov
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def run_times(addr):
    return ExemplaryRPC, ExemplaryRPC()


def run_times_2(addr_b, addr_a):
    rpc = ExemplaryRPC

    t = Transport.from_url('udp://127.0.0.1')

    with t:
        ts = RPCTransportStack([t])
        pt = ServiceDefn.from_obj(rpc)
        r: ExemplaryRPC = build_wrapper(pt, ts, addr_a, conf=ClientConfig(timeout_total=5))

        while True:
            try:
                a = r.move_something(5, 6, 8, pop='asd')
                b = r.reply(5, 6, 8, pop='asd')
                c = r.exit()
                return
            except HorizonPassedError:
                    sleep(2)


class TestServer(ProcessHelperCase):
    def test_udp(self):
        addr_a = 'udp://127.0.0.1:7483'
        addr_b = 'udp://'
        a = self.ps.popen(server_main, run_times, addr_a)
        b = self.ps.popen(run_times_2, addr_b, addr_a)
        self.ps.wait([a, b])
