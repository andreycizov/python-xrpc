import unittest

from xrpc.trace import trc


class TestTrace(unittest.TestCase):
    def test_trace(self):
        x = trc()

        self.assertEqual(f'{self.__module__}.test_trace', x.name)

    def test_trace_postfix(self):
        x = trc('pf')
        self.assertEqual(f'{self.__module__}.test_trace_postfix.pf', x.name)
