import json
import unittest
from functools import partial
from typing import TypeVar, Generic, Type, List
from urllib.parse import urlparse
import typing_inspect
from datetime import datetime

from xrpc.const import SERVER_SERDE_INST
from xrpc.net import RPCKey, RPCPacket, RPCPacketType
from xrpc.serde.abstract import SerdeSet, SerdeStepContext
from xrpc.trace import trc


class TestAm(unittest.TestCase):
    def test_am(self):
        try:
            raise ConnectionAbortedError('asdasd', 'asda')
        except ConnectionAbortedError as e:
            for x in dir(e):
                print(x, getattr(e, x))
            print(e.args)

    def test_other(self):
        x = urlparse('unix://')

        print(x)

    def test_generic(self):
        T = TypeVar('T')

        class A(Generic[T]):
            def __init__(self):
                pass

        x = typing_inspect.get_args(A[int])
        y = typing_inspect.get_args(A[int]().__class__)
        z = typing_inspect.get_origin(A[int])

        x1 = typing_inspect.get_parameters(A[int])
        y1 = typing_inspect.get_parameters(A[int]().__class__)

        print(x, y, z, x1, y1)

    def test_intellij_inspection(self):
        A, B = TypeVar('A'), TypeVar('B')

        T = TypeVar('T')

        class GenericClass(Generic[A, B]):
            def run(self, x: A) -> B:
                pass

        def factory_fun(cls: Type[T]) -> T:
            pass

        x = factory_fun(datetime)

        z = factory_fun(GenericClass[int, datetime])

    def test_list_sizes(self):
        cls = List[RPCKey]
        x: cls = [RPCKey.new() for x in range(1000)]

        ss = SerdeSet.walk(SERVER_SERDE_INST, cls, SerdeStepContext()).struct(SERVER_SERDE_INST)
        rtn = ss.serialize(cls, x)

        pkt = RPCPacket(RPCKey.new(), RPCPacketType.Req, 'asd', rtn)

        print(len(pkt.pack()))

    def test_property_constructor(self):
        class A:
            def __get__(self, x, y):
                trc().error('%s %s', x, y)
                return 5

            def __set__(self, x, y):
                trc().error('%s %s', x, y)

            def __del__(self):
                trc().error('%s %s', x)

            # def __getattr__(self, item):
            #     return property(
            #         partial(self.__get, item),
            #         partial(self.__set, item),
            #         partial(self.__del, item),
            #     )

        x = A()
        print(x.y)

    def test_list_iter(self):
        xs = [1, 2, 3, 4, 5, 6]

        for i, x in enumerate(xs):
            xs.remove(x)
            print(i, x)
