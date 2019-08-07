import unittest

from xrpc.trace import trc


class Traced:
    def a(self, pf=None):
        return trc(pf)


def _trace(pf=None):
    return trc(pf)


class TestTrace(unittest.TestCase):
    def test_trace(self):
        x = _trace()

        self.assertEqual(f'{self.__module__}._trace:12', x.name)

    def test_trace_postfix(self):
        x = _trace('pf')
        self.assertEqual(f'{self.__module__}._trace:12.pf', x.name)

    def test_trace_obj(self):
        x = Traced().a()

        self.assertEqual(f'{self.__module__}.{Traced.__name__}.a:8', x.name)

    def test_trace_obj_postfix(self):
        x = Traced().a('pf')

        self.assertEqual(f'{self.__module__}.{Traced.__name__}.a:8.pf', x.name)
