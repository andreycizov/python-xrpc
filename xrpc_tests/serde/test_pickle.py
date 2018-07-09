import base64
import pickle
import sys
import unittest
from typing import NamedTuple, Optional, TypeVar, Generic

from dataclasses import dataclass

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet
from xrpc_tests.serde import test_pickle2


class Obj(NamedTuple):
    i: int = 0


class ObjR1(NamedTuple):
    i: Optional['ObjR2']


class ObjR2(NamedTuple):
    i: ObjR1


class ObjR4(NamedTuple):
    y: 'test_pickle2.ObjR3'


T = TypeVar('T')


@dataclass
class Gen(Generic[T]):
    i: T


GEN_TYPES = [int, float, bytes, Obj]
GEN_VALS = [5, 5.3, base64.b64encode(b'asd').decode(), {'i': 5}]


class TestPickle(unittest.TestCase):
    def test_pickle_simple(self):
        with self.subTest('is_creatable'):
            WorkerSerde = SerdeSet.walk(SERVER_SERDE_INST, Obj).struct(SERVER_SERDE_INST)

        with self.subTest('is_picklable'):
            import pickle
            pickle.dumps(WorkerSerde)
        xo = {'i': 5}
        with self.subTest('des'):
            x = WorkerSerde.deserialize(Obj, xo)
        with self.subTest('ser'):
            y = WorkerSerde.serialize(Obj, x)
        with self.subTest('res'):
            self.assertEqual(y, xo)

    def test_generic(self):
        for ty, tv in zip(GEN_TYPES, GEN_VALS):
            n = ty.__class__.__name__
            gt = Gen[ty]

            tv = {'i': tv}

            with self.subTest(f'{n}.is_creatable'):
                WorkerSerde = SerdeSet.walk(SERVER_SERDE_INST, gt).struct(SERVER_SERDE_INST)

            if sys.version_info >= (3, 7):
                with self.subTest(f'{n}.is_picklable'):
                    pickle.dumps(WorkerSerde)

            with self.subTest(f'{n}.des'):
                x = WorkerSerde.deserialize(gt, tv)
            with self.subTest(f'{n}.ser'):
                y = WorkerSerde.serialize(gt, x)
            with self.subTest(f'{n}.res'):
                self.assertEqual(tv, y)

    def test_forward_ref_cyclic(self):
        with self.subTest('is_creatable'):
            WorkerSerde = SerdeSet.walk(SERVER_SERDE_INST, ObjR1) \
                .merge(SerdeSet.walk(SERVER_SERDE_INST, ObjR2)).struct(SERVER_SERDE_INST)

        if sys.version_info >= (3, 7):
            with self.subTest('is_picklable'):
                pickle.dumps(WorkerSerde)

        xo = {'i': {'i': {'i': None}}}
        with self.subTest('des'):
            x = WorkerSerde.deserialize(ObjR1, xo)
        with self.subTest('ser'):
            y = WorkerSerde.serialize(ObjR1, x)
        with self.subTest('res'):
            self.assertEqual(y, xo)

    def test_forward_ref_foreign_mod(self):
        with self.subTest('is_creatable'):
            WorkerSerde = SerdeSet.walk(SERVER_SERDE_INST, ObjR4).struct(SERVER_SERDE_INST)

        with self.subTest('is_picklable'):
            import pickle
            pickle.dumps(WorkerSerde)

        xo = {'y': {'i': 6}}

        with self.subTest('des'):
            x = WorkerSerde.deserialize(ObjR4, xo)
        with self.subTest('ser'):
            y = WorkerSerde.serialize(ObjR4, x)
        with self.subTest('res'):
            self.assertEqual(y, xo)
