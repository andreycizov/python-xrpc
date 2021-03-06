import sys
import unittest

from typing import NamedTuple, Optional, Dict

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet, SerdeStepContext
from xrpc.serde.types import CallableArgsWrapper


class Simple2(NamedTuple):
    y: Optional[str] = 'asd'


class Simple(NamedTuple):
    x: Optional[int]
    z: Simple2


class TestWalk(unittest.TestCase):
    def test_empty(self):
        i = SERVER_SERDE_INST

        x = SerdeSet.walk(i, Simple, SerdeStepContext(mod=sys.modules[__name__]))

        y = x.struct(i)

        z = y.deserialize(Simple, {'x': 5, 'z': {'y': 'abc'}})

    def test_caller(self):
        i = SERVER_SERDE_INST

        class Simpleton(NamedTuple):
            x: int

        def a(a: int, b: str = 'abc', *cs: Dict[str, str], g: str, **kwargs: int):
            pass

        class A:
            def a(self, a: int, c: Simpleton, b: str = 'abc', *cs: Dict[str, str], g: str, **kwargs: int):
                pass

        obj = A()

        wrapper = CallableArgsWrapper.from_func(a)
        wrapper2 = CallableArgsWrapper.from_func_cls(obj, A.a)

        x1 = SerdeSet.walk(i, wrapper, SerdeStepContext(mod=sys.modules[__name__]))
        x2 = SerdeSet.walk(i, wrapper2, SerdeStepContext(mod=sys.modules[__name__]))

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
