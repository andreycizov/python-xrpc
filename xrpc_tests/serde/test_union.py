import sys
import unittest
from pprint import pprint
from typing import Optional, Union

from dataclasses import dataclass

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet
from xrpc.serde.error import SerdeException
from xrpc_tests.impl.test_broker import build_logging


@dataclass
class A:
    a: int


_A = A


@dataclass
class A:
    a: str


@dataclass
class B:
    x: bytes


@dataclass
class C:
    a: bytes


class TestWalk(unittest.TestCase):


    def setUp(self):
        self.x = build_logging()
        self.x.__enter__()

    def test_union(self):
        tp = Optional[Union[A, B]]

        with self.subTest('init'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, tp).struct(SERVER_SERDE_INST)

            with self.subTest('ser'):
                i = A('6')
                x = serde.serialize(tp, i)

                with self.subTest('des'):
                    y = serde.deserialize(tp, x)

                    self.assertEqual(i, y)

    def test_union_optional(self):
        tp = Optional[Union[A, B]]
        with self.subTest('init'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, tp).struct(SERVER_SERDE_INST)

            i = B(b'6')

            with self.subTest('ser'):
                x = serde.serialize(tp, i)

                with self.subTest('des'):
                    y = serde.deserialize(tp, x)

                    self.assertEqual(i, y)

    def test_union_optional_none(self):
        tp = Optional[Union[A, B]]
        with self.subTest('init'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, tp).struct(SERVER_SERDE_INST)

            i = None

            with self.subTest('ser'):
                x = serde.serialize(tp, i)

                with self.subTest('des'):
                    y = serde.deserialize(tp, x)

                    self.assertEqual(i, y)

    def test_union_versioning(self):
        self._generic(Union[A, B], Union[A, B, C], B(b'asd'))

    def test_union_versioning_back(self):
        self._generic(Union[A, B, C], Union[A, B], B(b'asd'))

    def test_union_versioning_fail(self):
        tp = Union[A, B, C]
        tp_2 = Union[A, C]
        with self.subTest('init'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, tp).struct(SERVER_SERDE_INST)
            serde_2 = SerdeSet.walk(SERVER_SERDE_INST, tp_2).struct(SERVER_SERDE_INST)

            i = B(b'asd')

            with self.subTest('ser'):
                x = serde.serialize(tp, i)

                with self.subTest('des'):
                    try:
                        y = serde_2.deserialize(tp_2, x)

                        self.assertEqual(i, y)
                    except SerdeException as e:
                        self.assertEqual('u_idx', e.resolve().code)

    def test_union_degraded(self):
        self._generic(Union[A], Union[A], A('asd'))

    def _generic(self, tp, tp_2, val):
        with self.subTest('init'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, tp).struct(SERVER_SERDE_INST)
            serde_2 = SerdeSet.walk(SERVER_SERDE_INST, tp_2).struct(SERVER_SERDE_INST)

            pprint(serde.serializers)
            pprint(serde_2.deserializers)

            i = val

            with self.subTest('ser'):
                x = serde.serialize(tp, i)

                with self.subTest('des'):
                    y = serde_2.deserialize(tp_2, x)

                    self.assertEqual(i, y)

    def test_union_degraded_versioning(self):
        # todo python automatically normalizes Union[A] to A
        # self._generic(Union[A], Union[A, B], A('asd'))
        pass

    def test_union_degraded_versioning_back(self):
        # todo python automatically normalizes Union[A] to A
        #self._generic(Union[A, B], Union[A], A('asd'))
        pass


    def test_union_same_name(self):
        with self.subTest('init'):
            with self.assertRaises(AssertionError):
                serde = SerdeSet.walk(SERVER_SERDE_INST, Union[_A, A, B]).struct(SERVER_SERDE_INST)

    def tearDown(self):
        self.x.__exit__(*sys.exc_info())

