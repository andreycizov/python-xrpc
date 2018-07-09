import unittest
from multiprocessing.pool import Pool

from xrpc.client import build_wrapper, ClientConfig
from xrpc.server import run_server
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.service import ServiceDefn
from xrpc.transport import RPCTransportStack, Transport
from xrpc_tests.test_pong import wait_items


def run_times():
    rpc = ExemplaryRPC()
    try:
        run_server(rpc, ['udp://127.0.0.1:7483'])
    finally:
        pass


def run_times_2():
    rpc = ExemplaryRPC

    t = Transport.from_url('udp://127.0.0.1')

    with t:
        ts = RPCTransportStack([t])
        pt = ServiceDefn.from_obj(rpc, override_method=True)
        r: ExemplaryRPC = build_wrapper(pt, ts, 'udp://127.0.0.1:7483', conf=ClientConfig(timeout_total=2.))

        a = r.move_something(5, 6, 8, pop='asd')
        b = r.reply(5, 6, 8, pop='asd')
        c = r.exit()


class TestTransform(unittest.TestCase):
    def test_udp(self):
        p = Pool(2)

        a = p.apply_async(run_times)
        b = p.apply_async(run_times_2)

        wait_items([a, b])

        p.close()
        p.join()
