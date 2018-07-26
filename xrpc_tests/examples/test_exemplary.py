from time import sleep

from xrpc.client import client_transport
from xrpc.error import HorizonPassedError
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.popen import wait_all
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def exemplary_main(addr):
    return ExemplaryRPC, ExemplaryRPC()


class TestExemplary(ProcessHelperCase):
    def test_exemplary(self):
        addr_a = 'udp://127.0.0.1:7483'
        addr_b = 'udp://'
        ax = self.ps.popen(server_main, exemplary_main, addr_a)

        with client_transport(ExemplaryRPC, dest=addr_a, origin=addr_b) as r:
            while True:
                try:
                    a = r.move_something(5, 6, 8, pop='asd')
                    b = r.reply(5, 6, 8, pop='asd')
                    c = r.exit()
                    break
                except HorizonPassedError:
                    sleep(0)

        self.assertEqual(wait_all(ax, max_wait=5), [0])
