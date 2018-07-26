import unittest

from xrpc.popen import popen


class TestPOpen(unittest.TestCase):
    def test_ok(self):
        self.assertEqual(popen(lambda: 1).wait(1), 0)

    def test_fail(self):
        def check():
            raise ValueError()

        self.assertEqual(popen(check).wait(1), 1)
