from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def run_times(a, b):
    return BroadcastClientRPC, BroadcastClientRPC(b)


def run_times_2(a):
    return BroadcastRPC, BroadcastRPC()


class TestBroadcast(ProcessHelperCase):
    def test_udp(self):

        ua = 'udp://127.0.0.1:23443'
        ub = 'udp://127.0.0.1:23445'
        a = self.ps.popen(server_main, run_times, ua, ub)
        b = self.ps.popen(server_main, run_times_2, ub)

        self.ps.wait([b, a])
