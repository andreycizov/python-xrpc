from _signal import SIGTERM

from xrpc.client import ClientConfig
from xrpc.dsl import regular, rpc, signal
from xrpc_tests.examples.test_generic import build_ts
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


class BaseClass:
    @rpc()
    def recv_reply(self, i: int) -> int:
        raise NotImplementedError('')


class ObjA:
    @rpc()
    def message_2(self, i: int) -> int:
        return i + 1

    @signal()
    def exit(self):
        return True


def aliasing_main(url):
    return ObjA, ObjA()


class TestAliasing(ProcessHelperCase):
    def test_simple(self):
        url_b = 'udp://127.0.0.1:32456?recv_reply=message_2'
        a = self.ps.popen(server_main, aliasing_main, url_b)

        with build_ts(BaseClass, url_b, ClientConfig(ignore_horizon=True)) as b:
            self.assertEqual(b.recv_reply(5), 6)

        a.send_signal(SIGTERM)
