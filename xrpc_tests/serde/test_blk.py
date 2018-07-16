import unittest

from xrpc.serde.blk import Block, ChecksumError


class TestBlock(unittest.TestCase):
    def test_blk_0(self):
        b = Block.new(b'123')
        c = b.unpack(b.pack())

        self.assertEqual(b, c)

    def test_blk_fail_0(self):
        b = Block.new(b'123')

        packed_b = bytearray(b.pack())

        packed_b[1] = (packed_b[1] + 1) % 256

        with self.assertRaises(ChecksumError):
            c = b.unpack(packed_b)

