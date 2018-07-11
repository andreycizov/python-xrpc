import unittest
from typing import NamedTuple, TypeVar, Generic

from dataclasses import dataclass

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet
from xrpc.serde.error import SerdeException
from xrpc.serde.types import CallableArgsWrapper, CallableRetWrapper

T = TypeVar('T')


@dataclass
class Obj(Generic[T]):
    i: T = 0


class Runner(Generic[T]):
    def method(self, a: int, b: str) -> T:
        pass

    def method2(self, a: int, b: T) -> bool:
        pass


def build_worker_serde():
    a = SerdeSet.walk(SERVER_SERDE_INST, Obj[int])

    s = a.merge(a)
    return s.struct(SERVER_SERDE_INST)


def dir_items(x):
    for n in dir(x):
        yield n, getattr(x, n)


class TestGeneric(unittest.TestCase):
    def test_pickle_0(self):
        WorkerSerde = build_worker_serde()

        # import pickle
        # pickle.dumps(WorkerSerde)

        WorkerSerde.deserialize(Obj[int], {'i': 5})
        WorkerSerde.serialize(Obj[int], Obj(5))

    def test_pickle_1(self):
        WorkerSerde = build_worker_serde()

        # import pickle
        # pickle.dumps(WorkerSerde)

        x = WorkerSerde.deserialize(Obj[int], {'i': 5})
        y = WorkerSerde.serialize(Obj[int], Obj(5))

    def test_generic_method(self):
        fa = CallableArgsWrapper.from_func_cls(Runner[int], Runner.method2)
        a = SerdeSet.walk(
            SERVER_SERDE_INST,
            fa
        )

        xo = [(5, 7), {}]
        x = a.struct(SERVER_SERDE_INST).serialize(fa, xo)
        y = a.struct(SERVER_SERDE_INST).deserialize(fa, xo)

        self.assertEqual(x, y)

        try:
            a.struct(SERVER_SERDE_INST).serialize(fa, [(5, 'asd'), {}])
        except SerdeException as e:
            self.assertEqual('asd', e.resolve().val)
            self.assertEqual('int', e.resolve().kwargs['t'])

    def test_generic_method_ret(self):
        fa = CallableRetWrapper.from_func_cls(Runner[int], Runner.method)
        a = SerdeSet.walk(
            SERVER_SERDE_INST,
            fa
        )

        xo = 8
        x = a.struct(SERVER_SERDE_INST).serialize(fa, xo)
        y = a.struct(SERVER_SERDE_INST).deserialize(fa, xo)

        self.assertEqual(x, y)

        try:
            a.struct(SERVER_SERDE_INST).serialize(fa, 'str')
        except SerdeException as e:
            self.assertEqual('str', e.resolve().val)
            self.assertEqual('int', e.resolve().kwargs['t'])
