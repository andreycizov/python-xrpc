from _signal import SIGTERM

from xrpc.client import ClientConfig, ClientTransportCircuitBreaker, client_transport
from xrpc.examples.recursive import Recursive, RecursiveA, RecursiveB
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main


def recursive_main(_):
    return Recursive, Recursive()


def recursive_a_main(_, url):
    return RecursiveA, RecursiveA(url)


def recursive_b_main(_):
    return RecursiveB, RecursiveB()


class TestRecursive(ProcessHelperCase):
    def test_self(self):
        url_b = 'udp://127.0.0.1:3456'
        a = self.ps.popen(server_main, recursive_main, url_b)

        with client_transport(Recursive, url_b, ClientConfig(ignore_horizon=True)) as b:
            self.assertEqual(0, b.ep(10))

        a.send_signal(SIGTERM)

    def test_double(self):
        url_a = 'udp://127.0.0.1:32456'
        url_b = 'udp://127.0.0.1:32457'
        a = self.ps.popen(server_main, recursive_a_main, url_a, url_b)
        b = self.ps.popen(server_main, recursive_b_main, url_b)

        with client_transport(RecursiveA, url_a, ClientConfig(ignore_horizon=True)) as acli:
            self.assertEqual(1, acli.poll())

        a.send_signal(SIGTERM)
        b.send_signal(SIGTERM)

    def test_callback_service_failure(self):
        url_b = 'udp://127.0.0.1:32457'
        b = self.ps.popen(server_main, recursive_b_main, url_b)

        with client_transport(RecursiveB, url_b, ClientConfig(ignore_horizon=True)) as acli:
            with self.assertRaises(ClientTransportCircuitBreaker):
                acli.count_status()

        b.send_signal(SIGTERM)
