import unittest
from inspect import getfullargspec
from pprint import pprint

from xrpc.serde.types import pair_spec, build_types, ArgumentsException


def mutably(a, b, c: int, d, *df, arg, kwarg=None, **kwargs):
    pass


def mutably_defs(a, b=5, c: int = 6, d=9, *df, arg=3, arg3=4, kwarg=None, **kwargs):
    pass


def mutably_defs_unmatched_kwonlyarg(*, d, **kwargs: int):
    pass


class TestPairSpec(unittest.TestCase):
    def test_pair_spec(self):
        spec = getfullargspec(mutably)
        tps = build_types(spec, False, allow_missing=True)

        print(tps)

        for x in pair_spec(spec, False, 'a', 'b', c='c', d='', arg=0):
            print(x)
            if x.name == 'c':
                self.assertEqual(tps['c'], int)

    def test_pair_spec2(self):
        spec = getfullargspec(mutably)
        tps = build_types(spec, False, allow_missing=True)

        print(tps)

        for x in pair_spec(spec, False, 'a', 'b', 1, 2, 3, 4, 5, arg=0):
            print(x)

            if x == 'c':
                self.assertEqual(tps['c'], int)

    def test_pair_spec_defaults(self):
        spec = getfullargspec(mutably_defs)
        tps = build_types(spec, False, allow_missing=True)

        print(tps)

        for x in pair_spec(spec, False, 'a', 'b', 1, 2, 3, 4, 5):
            print(x)

            if x.name == 'c':
                self.assertEqual(tps['c'], int)

    def test_pair_spec_unmatched_kwonlyarg(self):
        spec = getfullargspec(mutably_defs_unmatched_kwonlyarg)
        tps = build_types(spec, False, allow_missing=True)

        for x in pair_spec(spec, False, d=9):
            print(x)

            if x.name == 'c':
                self.assertEqual(tps['c'], int)

        try:
            for x in pair_spec(spec, False):
                print(x)

                if x == 'c':
                    self.assertEqual(tps['c'], int)
        except ArgumentsException as e:
            self.assertEqual(['d'], e.argument_required)
