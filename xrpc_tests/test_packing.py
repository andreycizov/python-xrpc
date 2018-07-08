import unittest

from xrpc.net import RPCKey, RPCPacket


class TestPacking(unittest.TestCase):
    def test_key(self):
        a = RPCKey.new()
        ap = a.pack()
        b = RPCKey.unpack(ap)

        self.assertEqual(a, b)

    def test_packet(self):
        k = RPCKey.new()
        a = RPCPacket(
            k,
            'абра',
            b'382490as'
        )

        x = a.pack()

        b = RPCPacket.unpack(x)

        self.assertEqual(a, b)
