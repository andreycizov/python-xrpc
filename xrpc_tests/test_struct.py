import unittest
from typing import TypeVar, List, NamedTuple, Type

from xrpc.abstract import BinaryQueue, Queue, HeapQueue

A1 = [1, 2, 2, 3, 3, 3]
A2 = [1, 2, 3, 3, 3]
A3 = []
A4 = [1]

T = TypeVar('T')
H = TypeVar('H')


def ex(q: Queue[T, H]) -> List[T]:
    r = []
    while True:
        try:
            r.append(q.pop())
        except IndexError:
            return r


def default_test(tc: unittest.TestCase, q_cls: Type[Queue[int, H]]):
    with tc.subTest('queue_001'):
        q = q_cls(A1)
        q.push(2)
        tc.assertEqual([1, 2, 2, 2, 3, 3, 3], ex(q), )

    with tc.subTest('queue_002'):
        q = q_cls(A2)
        q.push(2)
        tc.assertEqual([1, 2, 2, 3, 3, 3], ex(q), )

    with tc.subTest('queue_003'):
        q = q_cls(A2)
        q.push(0)
        tc.assertEqual([0, 1, 2, 3, 3, 3], ex(q), )

    with tc.subTest('queue_004'):
        q = q_cls(A2)
        q.push(4)
        tc.assertEqual([1, 2, 3, 3, 3, 4], ex(q), )

    with tc.subTest('queue_005'):
        q = q_cls(A3)
        q.push(4)
        tc.assertEqual([4], ex(q))

    with tc.subTest('queue_006'):
        q = q_cls(A4)
        q.push(4)
        tc.assertEqual([1, 4], ex(q))

    with tc.subTest('queue_007'):
        q = q_cls(A4)
        q.push(0)
        tc.assertEqual([0, 1], ex(q))


class TestStruct(unittest.TestCase):
    def test_binary_queue_001(self):
        default_test(self, BinaryQueue)

    def test_heap_queue_001(self):
        default_test(self, HeapQueue)
