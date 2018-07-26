import unittest

from typing import NamedTuple, Optional, Dict, TypeVar

from xrpc.serde.abstract import SerdeInst, SerdeSet
from xrpc.serde.types import ForwardRefSerde, UnionSerde, AtomSerde, NoneSerde, UUIDSerde, ListSerde, DictSerde, \
    EnumSerde, NamedTupleSerde, CallableArgsSerde, CallableArgsWrapper, TypeVarSerde

ALL_TYPES = [
    ForwardRefSerde(),
    UnionSerde(),
    AtomSerde(),
    NoneSerde(),
    UUIDSerde(),
    ListSerde(),
    DictSerde(),
    TypeVarSerde(),
    EnumSerde(),
    NamedTupleSerde(),
]

CALL_TYPES = [CallableArgsSerde()] + ALL_TYPES


class Simple2(NamedTuple):
    y: Optional[str] = 'asd'


T = TypeVar('T')


class TestWalkGeneric(unittest.TestCase):

    def test_caller(self):
        i = SerdeInst(CALL_TYPES)

        class Simpleton(NamedTuple):
            x: int

        def a(a: int, b: str = 'abc', *cs: Dict[str, str], g: str, **kwargs: int):
            pass

        class A:
            def a(self, a: int, c: Simpleton, b: str = 'abc', *cs: Dict[str, str], g: str, **kwargs: int):
                pass

        obj = A()

        wrapper = CallableArgsWrapper.from_func(a)
        wrapper2 = CallableArgsWrapper.from_func_cls(A, A.a)

        x1 = SerdeSet.walk(i, wrapper)
        x2 = SerdeSet.walk(i, wrapper2)

        x = x1.merge(x2)


        y = x.struct(i)


        z = y.deserialize(wrapper, [[5, 'asd', {'a': 'a'}, {'b': 'c'}], {'g': 'abc', 'd': 5}])


        args, kwargs = z

        a(*args, **kwargs)

        #

        z = y.deserialize(wrapper2, [[5, {'x': 5}, 'asd', {'a': 'a'}, {'b': 'c'}], {'g': 'abc', 'd': 5}])

        zb = y.serialize(wrapper2, z)

        args, kwargs = z

        obj.a(*args, **kwargs)
