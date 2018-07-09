import unittest
from multiprocessing.pool import Pool
from time import sleep

from xrpc.client import build_wrapper, ClientConfig
from xrpc.error import HorizonPassedError
from xrpc.server import run_server
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.service import ServiceDefn
from xrpc.transport import RPCTransportStack, Transport
from xrpc_tests.examples.test_pong import wait_items, cov


def run_times():
    with cov():
        rpc = ExemplaryRPC()

        sleep(1)
        run_server(ExemplaryRPC, rpc, ['udp://127.0.0.1:7483'])


def run_times_2():
    with cov():
        rpc = ExemplaryRPC

        t = Transport.from_url('udp://127.0.0.1')

        with t:
            ts = RPCTransportStack([t])
            pt = ServiceDefn.from_obj(rpc)
            r: ExemplaryRPC = build_wrapper(pt, ts, 'udp://127.0.0.1:7483', conf=ClientConfig(timeout_total=5))

            while True:
                try:
                    a = r.move_something(5, 6, 8, pop='asd')
                    b = r.reply(5, 6, 8, pop='asd')
                    c = r.exit()
                    return
                except HorizonPassedError:
                    sleep(2)


class TestServer(unittest.TestCase):
    def test_udp(self):
        with Pool(2) as p:
            a = p.apply_async(run_times)
            b = p.apply_async(run_times_2)

            wait_items([a, b])

        p.join()
