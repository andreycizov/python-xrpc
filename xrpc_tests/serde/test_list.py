import unittest
from typing import List, TypeVar, Generic

from dataclasses import dataclass

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet
from xrpc_tests.mp.abstract import ProcessHelperCase

T = TypeVar('T')


@dataclass
class Obj(Generic[T]):
    items: List[T]


@dataclass
class Obj2(Generic[T]):
    item: T


@dataclass
class Obj3(Generic[T]):
    items: List[Obj2[T]]


class TestList(ProcessHelperCase):
    def test_list_0(self):
        type_ = List[int]

        with self.subTest('walk'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, type_).struct(SERVER_SERDE_INST)

            initial = [1, 2, 3]
            with self.subTest('ser'):
                x = serde.serialize(type_, initial)

                with self.subTest('des'):
                    y = serde.deserialize(type_, x)

                    with self.subTest('eq'):
                        self.assertEqual(initial, y)

    def test_list_generic_0(self):
        type_ = Obj[int]

        with self.subTest('walk'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, type_).struct(SERVER_SERDE_INST)

            initial = Obj([1, 2, 3])
            with self.subTest('ser'):
                x = serde.serialize(type_, initial)

                with self.subTest('des'):
                    y = serde.deserialize(type_, x)

                    with self.subTest('eq'):
                        self.assertEqual(initial, y)

    def test_list_generic_1(self):
        type_ = Obj3[int]

        with self.subTest('walk'):
            serde = SerdeSet.walk(SERVER_SERDE_INST, type_).struct(SERVER_SERDE_INST)

            initial = Obj3([Obj2(1), Obj2(2)])
            with self.subTest('ser'):
                x = serde.serialize(type_, initial)

                with self.subTest('des'):
                    y = serde.deserialize(type_, x)

                    with self.subTest('eq'):
                        self.assertEqual(initial, y)
