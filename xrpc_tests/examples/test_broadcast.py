from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc.popen import wait_all
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def broadcast_client_main(a, b):
    return BroadcastClientRPC, BroadcastClientRPC(b)


def broadcast_main(a):
    return BroadcastRPC, BroadcastRPC()


class TestBroadcast(ProcessHelperCase):
    def test_bcast(self):

        ua = 'udp://127.0.0.1:23443'
        ub = 'udp://127.0.0.1:23445'
        a = self.ps.popen(server_main, broadcast_client_main, ua, ub)
        b = self.ps.popen(server_main, broadcast_main, ub)

        self.ps.wait([b, a])

        self.assertEqual(wait_all(b, a), [0, 0])
