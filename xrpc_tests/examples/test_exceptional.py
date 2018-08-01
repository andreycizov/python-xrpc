from _signal import SIGTERM
from typing import List, Type

from xrpc.client import client_transport, ClientConfig
from xrpc.dsl import DEFAULT_GROUP
from xrpc.error import TimeoutError
from xrpc.examples.exceptional import ExceptionalDropper, ExceptionalClient, Exceptional, Lively, LIVELY
from xrpc.popen import wait_all
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main_new


class TestExc(ProcessHelperCase):
    def _test(self, cls: Type[Exceptional], mask: List[int]):
        url_b = f'unix://{self.dtemp}/hugely.sock'
        url_b_l = f'udp://127.0.0.1:5678'
        url_a_l = f'udp://127.0.0.1:5679'

        a = self.ps.popen(
            server_main_new,
            lambda: (cls, cls()),
            {
                DEFAULT_GROUP: url_b + '#bind',
                LIVELY: url_b_l,
            }
        )

        with client_transport(Lively, url_b_l, conf=ClientConfig(horz=False, timeout_total=None)) as br:
            br.is_alive()

        b = self.ps.popen(
            server_main_new,
            lambda: (ExceptionalClient, ExceptionalClient()),
            {
                DEFAULT_GROUP: url_b,
                LIVELY: url_a_l,
            }
        )

        with client_transport(Lively, url_a_l, conf=ClientConfig(horz=False, timeout_total=None)) as br:
            br.is_alive()

        b.send_signal(SIGTERM)

        with client_transport(Lively, url_a_l, conf=ClientConfig(horz=False, timeout_total=1.)) as br:
            while True:
                try:
                    br.is_alive()
                except TimeoutError:
                    break

        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, max_wait=1), mask)

    def test_dropper(self):
        self._test(ExceptionalDropper, [1, 0])

    def test_catcher(self):
        self._test(Exceptional, [0, 0])
