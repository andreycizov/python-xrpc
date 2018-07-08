import unittest

from xrpc.client import build_wrapper
from xrpc.service import ServiceDefn
from xrpc.examples import ExemplaryRPC
from xrpc.transport import Transport, RPCTransportStack


class TestClient(unittest.TestCase):
    def test_udp(self):
        rpc = ExemplaryRPC

        t = Transport.from_url('udp://127.0.0.1:8905')

        with t:
            ts = RPCTransportStack([t])
            pt = ServiceDefn.from_obj(rpc, override_method=True)
            r: ExemplaryRPC = build_wrapper(pt, ts, ('127.0.0.1', 7483))

            a = r.move_something(5, 6, 8, pop='asd')
            b = r.reply(5, 6, 8, pop='asd')

            print(a)
            print(b)
