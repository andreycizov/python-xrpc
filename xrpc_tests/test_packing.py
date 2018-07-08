import unittest

from xrpc.net import RPCKey, RPCPacket, RPCPacketType


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
            RPCPacketType.Req,
            'абра',
            [{}, None]
        )

        x = a.pack()

        b = RPCPacket.unpack(x)

        self.assertEqual(a, b)
