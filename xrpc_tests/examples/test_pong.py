from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc_tests.mp.abstract import server_main, ProcessHelperCase


def pong_a_main(addr, other_addr):
    return BroadcastClientRPC, BroadcastClientRPC(other_addr)


def pong_b_main(addr):
    return BroadcastRPC, BroadcastRPC()


class TestPingPong(ProcessHelperCase):
    def test_ping_pong_0(self):
        url_a = 'udp://127.0.0.1:11134'
        url_b = 'udp://127.0.0.1:11146'

        a = self.ps.popen(server_main, pong_a_main, url_a, url_b)
        b = self.ps.popen(server_main, pong_b_main, url_b)
        self.ps.wait([a, b])
