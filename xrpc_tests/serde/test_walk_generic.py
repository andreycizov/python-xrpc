import sys
import unittest
from inspect import isclass
from pprint import pprint
from typing import NamedTuple, Optional, Dict, TypeVar, Generic

from xrpc.serde import types
from xrpc.serde.abstract import SerdeType, SerdeInst, SerdeSet
from xrpc.serde.types import ForwardRefSerde, UnionSerde, AtomSerde, NoneSerde, UUIDSerde, ListSerde, DictSerde, \
    EnumSerde, NamedTupleSerde, CallableArgsSerde, CallableArgsWrapper

ALL_TYPES = [
    ForwardRefSerde(),
    UnionSerde(),
    AtomSerde(),
    NoneSerde(),
    UUIDSerde(),
    ListSerde(),
    DictSerde(),
    EnumSerde(),
    NamedTupleSerde(),
]

CALL_TYPES = [CallableArgsSerde()] + ALL_TYPES


class Simple2(NamedTuple):
    y: Optional[str] = 'asd'


T = TypeVar('T')


class Simple(NamedTuple('Simple', [('x', Optional[T]), ('z', Simple2), ]), Generic[T]):
    pass



class TestWalkGeneric(unittest.TestCase):
    def test_empty(self):
        i = SerdeInst(ALL_TYPES)

        x = SerdeSet.walk(i, Simple, sys.modules[__name__])

        pprint(x)

        y = x.struct(i)

        pprint(y)

        z = y.deserialize(Simple, {'x': 5, 'z': {'y': 'abc'}})

        pprint(z)

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
        wrapper2 = CallableArgsWrapper.from_func(obj.a)

        x1 = SerdeSet.walk(i, wrapper, sys.modules[__name__])
        x2 = SerdeSet.walk(i, wrapper2, sys.modules[__name__])

        x = x1.merge(x2)

        pprint(x)

        y = x.struct(i)

        pprint(y)

        z = y.deserialize(wrapper, [[5, 'asd', {'a': 'a'}, {'b': 'c'}], {'g': 'abc', 'd': 5}])

        pprint(z)

        args, kwargs = z

        a(*args, **kwargs)

        #

        z = y.deserialize(wrapper2, [[5, {'x': 5}, 'asd', {'a': 'a'}, {'b': 'c'}], {'g': 'abc', 'd': 5}])

        pprint(['zee', z])

        zb = y.serialize(wrapper2, z)

        pprint(zb)

        args, kwargs = z

        obj.a(*args, **kwargs)
