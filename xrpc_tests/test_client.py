import unittest

from xrpc import error
from xrpc.client import build_wrapper, ClientConfig
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.loop import EventLoop
from xrpc.service import ServiceDefn
from xrpc.transport import Transport


class TestClient(unittest.TestCase):
    def test_udp(self):
        rpc = ExemplaryRPC

        t = Transport.from_url('udp://127.0.0.1:8905')

        with t:
            ts = EventLoop()
            tref = ts.transport_add(t)
            pt = ServiceDefn.from_cls(rpc)
            with self.assertRaises(error.TimeoutError):
                r: ExemplaryRPC = build_wrapper(pt, tref, 'udp://127.0.0.1:7483', conf=ClientConfig(timeout_total=2.))

                a = r.move_something(5, 6, 8, pop='asd')
                b = r.reply(5, 6, 8, pop='asd')
