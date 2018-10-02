import unittest

from typing import TypeVar, List, Type

from xrpc.abstract import BinaryQueue, Queue, HeapQueue, KeyedQueue

A1 = [1, 2, 2, 3, 3, 3]
A2 = [1, 2, 3, 3, 3]
A3 = []
A4 = [1]

T = TypeVar('T')


def ex(q: Queue[T]) -> List[T]:
    r = []
    while True:
        try:
            r.append(q.pop())
        except IndexError:
            return r


def default_test(tc: unittest.TestCase, q_cls: Type[Queue[int]]):
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

    with tc.subTest('queue_007'):
        q = q_cls([])
        tc.assertIsNone(q.peek())

        val = 5
        q.push(val)
        v2 = q.peek()
        tc.assertEqual(v2, val)


class TestStruct(unittest.TestCase):
    def test_binary_queue_001(self):
        default_test(self, BinaryQueue)

    def test_heap_queue_001(self):
        default_test(self, HeapQueue)

    def test_keyed_heap_queue_001(self):
        q_cls = KeyedQueue

        for sq_cls in [HeapQueue, BinaryQueue]:
            a1 = list(enumerate(A1))
            a2 = list(enumerate(A2))
            a3 = list(enumerate(A3))
            a4 = list(enumerate(A4))

            ord_fn = lambda x: x[1]
            key_fn = lambda x: x[0]

            with self.subTest(str(sq_cls)):
                with self.subTest('queue_001'):
                    q = q_cls(a1, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a1), 2))
                    self.assertEqual([1, 2, 2, 2, 3, 3, 3], [ord_fn(x) for x in ex(q)])

                with self.subTest('queue_002'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a2), 2))
                    self.assertEqual([1, 2, 2, 3, 3, 3], [ord_fn(x) for x in ex(q)], )

                with self.subTest('queue_002'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a2), 3))
                    self.assertEqual([1, 2, 3, 3, 3, 3], [ord_fn(x) for x in ex(q)], )

                with self.subTest('queue_002'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a2), 3))
                    self.assertEqual(1, ord_fn(q.pop()))
                    self.assertEqual(2, ord_fn(q.pop()))
                    self.assertEqual(3, ord_fn(q.pop()))
                    q.push((len(a2) + 1, 2))
                    q.push((len(a2) + 2, 3))
                    self.assertEqual(2, ord_fn(q.pop()))
                    self.assertEqual(3, ord_fn(q.pop()))
                    self.assertEqual(3, ord_fn(q.pop()))
                    self.assertEqual(3, ord_fn(q.pop()))
                    self.assertEqual(3, ord_fn(q.pop()))

                    self.assertEqual([], [ord_fn(x) for x in ex(q)], )

                with self.subTest('queue_003'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a2), 0))
                    self.assertEqual([0, 1, 2, 3, 3, 3], [ord_fn(x) for x in ex(q)], )

                with self.subTest('queue_00300'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    # [1, 2, 3, 3, 3]
                    q.push((0, 0))
                    q.push((0, 1))
                    q.push((0, 3))
                    del q[0]
                    q.push((0, 5))
                    #self.assertEqual((0, 2), q.pop())
                    self.assertEqual([2, 3, 3, 3, 5], [ord_fn(x) for x in q.iter()])

                with self.subTest('queue_00301'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    # [1, 2, 3, 3, 3]
                    q.push((0, 0))
                    q.push((0, 1))
                    q.push((0, 1))
                    q.push((0, 1))
                    q.push((0, 0))
                    self.assertEqual((0, 0), q.pop())
                    q.push((0, 5))
                    self.assertEqual([2, 3, 3, 3, 5], [ord_fn(x) for x in q.iter()])

                with self.subTest('queue_0031'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((0, 5))
                    self.assertEqual([2, 3, 3, 3, 5], [ord_fn(x) for x in q.iter()])

                with self.subTest('queue_004'):
                    q = q_cls(a2, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a2), 4))
                    self.assertEqual([1, 2, 3, 3, 3, 4], [ord_fn(x) for x in ex(q)], )

                with self.subTest('queue_005'):
                    q = q_cls(a3, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a3), 4))
                    self.assertEqual([4], [ord_fn(x) for x in ex(q)])

                with self.subTest('queue_006'):
                    q = q_cls(a4, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a4), 4))
                    self.assertEqual([1, 4], [ord_fn(x) for x in ex(q)])

                with self.subTest('queue_007'):
                    q = q_cls(a4, ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    q.push((len(a4), 0))
                    self.assertEqual([0, 1], [ord_fn(x) for x in ex(q)])

                with self.subTest('queue_007'):
                    q = q_cls([], ord=ord_fn, key=key_fn, q_cls=sq_cls)
                    self.assertIsNone(q.peek())

                    val = (0, 5)
                    q.push(val)
                    v2 = q.peek()
                    self.assertEqual(val, v2)

                with self.subTest('queue_009'):
                    q = q_cls([])

                    with self.assertRaises(IndexError):
                        q.pop()

                with self.subTest('queue_010'):
                    q = q_cls(['a', 'b'])
                    del q['a']
                    self.assertEqual(q.pop(), 'b')

                    with self.assertRaises(IndexError):
                        q.pop()

                with self.subTest('queue_011'):
                    q = q_cls(['a', 'b'])

                    self.assertEqual(True, 'b' in q)
                    self.assertEqual(True, 'a' in q)
                    q.pop()
                    self.assertEqual(False, 'a' in q)

