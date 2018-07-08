import unittest

from xrpc.transport import Transport, Packet


class TestUDPTransport(unittest.TestCase):
    def test_empty_port_local(self):
        with Transport.from_url('udp://127.0.0.1') as t:
            pass

    def test_empty_port_any(self):
        with Transport.from_url('udp://0.0.0.0') as t:
            pass

    def test_cool(self):
        with Transport.from_url('udp://0.0.0.0:23454') as t:
            pass

    def test_sendto(self):
        with Transport.from_url('udp://0.0.0.0:23454') as t:
            t.send(Packet(('127.0.0.1', 12), b''))

    def test_sendto_url(self):
        with Transport.from_url('udp://0.0.0.0:23454') as t:
            t.send(Packet('udp://127.0.0.1:23453', b''))