import unittest

from time import sleep

from xrpc.popen import popen, wait_all


class TestPOpen(unittest.TestCase):
    def test_ok(self):
        self.assertEqual(popen(lambda: 1).wait(1), 0)

    def test_fail(self):
        def check():
            raise ValueError()

        self.assertEqual(popen(check).wait(1), 1)

    def test_wait_all_0(self):
        def check():
            raise ValueError()

        self.assertEqual(wait_all(popen(check), popen(lambda: 1)), [1, 0])

    def test_wait_all_1(self):
        def check():
            raise ValueError()

        self.assertEqual(wait_all(popen(lambda: 1), popen(check)), [0, 1])

    def test_wait_all_2(self):
        def check():
            sleep(0.05)
            raise ValueError()

        self.assertEqual(wait_all(popen(lambda: 1), popen(check)), [0, 1])

    def test_wait_all_3(self):
        def check():
            sleep(0.05)
            raise ValueError()

        self.assertEqual(wait_all(popen(check), popen(lambda: 1)), [1, 0])
