import unittest
from inspect import getfullargspec
from pprint import pprint

from xrpc.serde.types import pair_spec, build_types


def mutably(a, b, c: int, d, *df, arg, kwarg=None, **kwargs):
    pass


class TestPairSpec(unittest.TestCase):
    def test_pair_spec(self):
        spec = getfullargspec(mutably)
        tps = build_types(spec, False, allow_missing=True)

        print(tps)

        for x, y in pair_spec(spec, False, 'a', 'b', c='c', d=''):
            print(x, y)

            if x == 'c':
                self.assertEqual(tps['c'], int)

    def test_pair_spec2(self):
        spec = getfullargspec(mutably)
        tps = build_types(spec, False, allow_missing=True)

        print(tps)

        for x, y in pair_spec(spec, False, 'a', 'b', 1, 2, 3, 4, 5):
            print(x, y)

            if x == 'c':
                self.assertEqual(tps['c'], int)


