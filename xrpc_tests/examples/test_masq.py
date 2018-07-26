from _signal import SIGTERM

import xrpc
from xrpc.client import client_transport
from xrpc.dsl import rpc, signal
from xrpc.error import TerminationException
from xrpc.runtime import masquerade
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
        raise TerminationException()


class ObjB:
    @rpc()
    def message_3(self, i: int) -> int:
        return i + 1

    @signal()
    def exit(self):
        raise TerminationException()


def masq_main(url):
    return ObjA, ObjA()


class TestMasquerade(ProcessHelperCase):
    def test_masq_st_0(self):
        url_b = 'udp://127.0.0.1:32456'

        req_url = masquerade(url_b, ObjA, BaseClass, recv_reply='message_2')
        print(req_url)

    def test_masq_st_1(self):
        req_url = 'udp://127.0.0.1:32456'

        req_url = masquerade(req_url, ObjA, BaseClass, recv_reply='message_2')
        # this is a req_url that calls ObjA as BaseClass

        # (ObjA masq BaseClass)

        self.assertEqual('udp://127.0.0.1:32456?recv_reply=message_2', req_url)

        # now we know ObjA by it's req_url (we know it's a and we know it's masqueraded)
        req_url = masquerade(req_url, BaseClass, ObjB, message_3='recv_reply')

        # (ObjA masq BaseClass) masq BaseClass
        self.assertEqual('udp://127.0.0.1:32456?message_3=message_2', req_url)

    def test_simple(self):
        # we may need to report our origin in the stack somewhere
        url_b = 'udp://127.0.0.1:32456'

        req_url = masquerade(url_b, ObjA, BaseClass, recv_reply='message_2')

        a = self.ps.popen(server_main, masq_main, url_b)

        with client_transport(BaseClass, req_url, xrpc.client.ClientConfig(ignore_horizon=True)) as b:
            self.assertEqual(b.recv_reply(5), 6)

        a.send_signal(SIGTERM)
