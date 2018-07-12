from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc_tests.mp.abstract import server_main, ProcessHelperCase


def run_server_a(addr, other_addr):
    return BroadcastClientRPC, BroadcastClientRPC(other_addr)


def run_server_b(addr):
    return BroadcastRPC, BroadcastRPC()


class TestPingPong(ProcessHelperCase):
    def test_udp(self):
        url_a = 'udp://127.0.0.1:11134'
        url_b = 'udp://127.0.0.1:11146'

        a = self.ps.popen(server_main, run_server_a, url_a, url_b)
        b = self.ps.popen(server_main, run_server_b, url_b)
        self.ps.wait([a, b])
