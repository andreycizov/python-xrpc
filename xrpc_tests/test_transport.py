from tempfile import mkdtemp

import shutil

from xrpc.transport import Transport, Packet, select_helper, _insert_ordered
from xrpc_tests.mp.abstract import ProcessHelperCase


class TestUDPTransport(ProcessHelperCase):
    def test_empty_port_local(self):
        with Transport.from_url('udp://127.0.0.1') as t:
            pass

    def test_unknown_hostname(self):
        with Transport.from_url('udp://0.0.0.0') as t:
            t.send(Packet('udp://unknown_hostname:234', b'123'))

    def test_empty_port_any(self):
        with Transport.from_url('udp://0.0.0.0') as t:
            pass

    def test_empty_addr_any(self):
        with Transport.from_url('udp://') as t:
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

    def test_max_sendto(self):
        S_ADDR = 'udp://0.0.0.0:23454'
        ATTEMPTS = 5
        with Transport.from_url(S_ADDR) as ts, Transport.from_url('udp://') as tc:
            for i in [1, 100, 1000, 10000, 50000] + [2**16-1000]:
                for y in range(ATTEMPTS):
                    pkt = Packet(S_ADDR, b'\x00' * i)
                    tc.send(pkt)

                    select_helper([ts.fd], max_wait=1.)

                    for pkt2 in ts.read():
                        pass


class TestUnixTransport(ProcessHelperCase):
    def test_unix(self):
        item = mkdtemp()

        try:
            url = f'unix://{item}/socket.sock'
            with Transport.from_url(url + '#bind') as s, Transport.from_url(url) as c:
                c.send(Packet(url, b'123'))

                for x in s.read():
                    break
                else:
                    raise ValueError('asd')

                s.send(Packet(x.addr, x.data + b'__rep'))

                for x in c.read():
                    break
                else:
                    raise ValueError('asd')

                self.assertEqual(b'123__rep', x.data)
        finally:
            shutil.rmtree(item)

    def test_unix_abort(self):
        item = mkdtemp()

        try:
            url = f'unix://{item}/socket.sock'
            with Transport.from_url(url + '#bind') as s, Transport.from_url(url) as c:
                c.send(Packet(url, b'123'))

                c.close()

                for x in s.read():
                    break
                else:
                    raise ValueError('asd')

                try:
                    for _ in s.read():
                        pass
                except ConnectionAbortedError:
                    pass

                self.assertEqual(0, len(s._fd_clients))

                s.send(Packet(x.addr, x.data + b'__rep'))
        finally:
            shutil.rmtree(item)

    def test_unix_abort_2(self):
        item = mkdtemp()

        try:
            url = f'unix://{item}/socket.sock'
            with Transport.from_url(url + '#bind') as s, Transport.from_url(url) as c:
                for _ in s.read():
                    raise ValueError('asd')
                else:
                    pass

                c.close()

                try:
                    for x in s.read():
                        raise ValueError('asd')
                except ConnectionAbortedError:
                    pass

                self.assertEqual(0, len(s._fd_clients))
        finally:
            shutil.rmtree(item)

    def test_unix_abort_select(self):
        item = mkdtemp()

        try:
            url = f'unix://{item}/socket.sock'
            with Transport.from_url(url + '#bind') as s, Transport.from_url(url) as c:
                for _ in s.read():
                    pass
                c.close()

                try:
                    for _ in s.read():
                        pass
                except ConnectionAbortedError:
                    pass

                self.assertEqual(0, len(s._fd_clients))
        finally:
            shutil.rmtree(item)

    def test_insert_ordered_0(self):
        x = _insert_ordered([True, True], False, 'b', ['a', 'b', 'c'])

        self.assertEqual([True, False, True], x)
