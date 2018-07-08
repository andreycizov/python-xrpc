import unittest

from xrpc.server import run_server
from xrpc.examples import ExemplaryRPC


class TestTransform(unittest.TestCase):
    def test_udp(self):
        rpc = ExemplaryRPC()
        run_server(rpc, ['udp://127.0.0.1:7483'])
