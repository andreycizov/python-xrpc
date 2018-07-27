from _signal import SIGTERM

from time import sleep

from xrpc.examples.exceptional import ExceptionalDropper, ExceptionalClient, Exceptional
from xrpc.popen import wait_all
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


class TestExc(ProcessHelperCase):
    def test_dropper(self):
        url_b = f'unix://{self.dtemp}/hugely.sock'
        a = self.ps.popen(server_main, lambda _: (ExceptionalDropper, ExceptionalDropper()), url_b + '#bind')

        sleep(0.2)
        b = self.ps.popen(server_main, lambda _: (ExceptionalClient, ExceptionalClient()), url_b)
        sleep(0.2)
        b.send_signal(SIGTERM)
        sleep(0.1)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, max_wait=1), [1, 0])

    def test_catcher(self):
        url_b = f'unix://{self.dtemp}/hugely.sock'
        a = self.ps.popen(server_main, lambda _: (Exceptional, Exceptional()), url_b + '#bind')

        sleep(0.2)
        b = self.ps.popen(server_main, lambda _: (ExceptionalClient, ExceptionalClient()), url_b)
        sleep(0.2)
        b.send_signal(SIGTERM)
        sleep(0.1)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, max_wait=1), [0, 0])
