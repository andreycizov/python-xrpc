import unittest
from multiprocessing.pool import Pool

from xrpc.server import run_server
from xrpc.examples import BroadcastClientRPC, BroadcastRPC


def run_server_a(b_addr):
    run_server(BroadcastClientRPC(b_addr), ['udp://127.0.0.1:7483'])


def run_server_b(c_addr):
    run_server(BroadcastRPC(), ['udp://127.0.0.1:7482'])


class TestTransform(unittest.TestCase):
    def test_udp(self):
        with Pool(2) as p:
            a = p.apply_async(run_server_a, args=(('127.0.0.1', 7482),))
            b = p.apply_async(run_server_b, args=(('127.0.0.1', 7482),))

            a.wait()
            b.wait()

