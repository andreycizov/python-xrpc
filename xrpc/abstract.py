import heapq
from collections import deque

from datetime import datetime
from typing import TypeVar, Generic, List, Optional, Dict, Tuple, Callable, Type, Iterator

from xrpc.util import time_now


class MutableInt:
    def __init__(self, state: int = 0):
        self.state = int(state)

    def __set__(self, instance, value):
        pass

    def __iadd__(self, other):
        self.state += other

    def __isub__(self, other):
        self.state -= other

    def __le__(self, other):
        return self.state <= other

    def set(self, state: int):
        self.state = int(state)

    def reduce(self, x=1):
        self.state -= x

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self.state})'


class MutableDateTime:
    def __init__(self, t: datetime):
        self.t = t

    @classmethod
    def now(cls):
        return MutableDateTime(time_now())

    def get(self) -> datetime:
        return self.t

    def set(self, t: datetime) -> datetime:
        r = self.t
        self.t = t
        return r


V = TypeVar('T')
K = TypeVar('K')


class Queue(Generic[V]):
    def copy(self) -> 'Queue':
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def push(self, val: V):
        raise NotImplementedError()

    def pop(self) -> V:
        raise NotImplementedError()

    def peek(self) -> Optional[V]:
        raise NotImplementedError()

    def iter(self) -> Iterator[V]:
        raise NotImplementedError()


class BinaryQueue(Queue[V]):
    def __init__(self, initial: Optional[List[V]] = None):
        if initial:
            self.h = deque(sorted(initial))
        else:
            self.h = deque()

    def __len__(self):
        return len(self.h)

    def copy(self) -> 'Queue':
        bq = BinaryQueue()
        bq.h = deque(self.h)
        return bq

    def iter(self) -> Iterator[V]:
        for x in self.h:
            yield x

    def push(self, val: V):
        if len(self.h) == 0:
            self.h.append(val)
            return

        min_idx = 0
        max_idx = len(self.h) - 1

        while min_idx != max_idx and min_idx < max_idx - 1:
            mid_idx = (min_idx + max_idx) // 2

            mid_val = self.h[mid_idx]

            prev_range = (min_idx, max_idx)

            if mid_val == val:
                break
            elif mid_val > val:
                max_idx = mid_idx - 1
            else:
                min_idx = mid_idx + 1

            assert prev_range != (min_idx, max_idx), f'Ranges must always differ, {prev_range}, {(min_idx, max_idx)}'

        if min_idx == max_idx:
            if self.h[min_idx] < val:
                self.h.insert(min_idx + 1, val)
            else:
                self.h.insert(min_idx, val)
            return
        else:
            if self.h[min_idx] > val:
                self.h.insert(min_idx, val)
            elif self.h[max_idx] < val:
                self.h.insert(max_idx + 1, val)
            else:
                self.h.insert(min_idx + 1, val)
            return

    def pop(self) -> V:
        r = self.h.popleft()
        return r

    def peek(self) -> Optional[V]:
        if len(self.h):
            return self.h[0]
        else:
            return None


class HeapQueue(Queue[V]):
    def __init__(self, initial: Optional[List[V]] = None):
        if initial:
            self.h = list(initial)
            heapq.heapify(self.h)
        else:
            self.h = []

    def __len__(self):
        return len(self.h)

    def iter(self) -> Iterator[V]:
        copy = self.copy()

        while True:
            try:
                yield copy.pop()
            except IndexError:
                return

    def copy(self) -> 'Queue':
        hq = HeapQueue()
        hq.h = list(self.h)
        return hq

    def push(self, val: V):
        heapq.heappush(self.h, val)

    def pop(self) -> V:
        return heapq.heappop(self.h)

    def peek(self) -> Optional[V]:
        if len(self.h):
            return self.h[0]
        else:
            return None

    def pushpop(self, val: V):
        return heapq.heappushpop(self.h, val)

    def replace(self, val: V):
        return heapq.heapreplace(self.h, val)


Ord = TypeVar('Ord')


class KeyedQueue(Generic[Ord, K, V], Queue[V]):
    def __init__(self,
                 initial: Optional[List[V]] = None,
                 *,
                 ord: Callable[[V], Ord] = lambda x: x,
                 key: Callable[[V], K] = lambda x: x,
                 q_cls: Type[Queue] = HeapQueue
                 ):
        self.ord_fn = ord
        self.key_fn = key

        self.next_ctr: Dict[K, int] = {}
        self.ref_ctr: Dict[K, int] = {}
        self.values: Dict[K, V] = {}

        self.q_cls = q_cls
        self.q: Queue[Tuple[Ord, K, int]] = q_cls()

        if initial:
            for x in initial:
                self.push(x)

    def get(self, key: K, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __len__(self):
        return len(self.values)

    def __getitem__(self, key: K):
        return self.values[key]

    def __delitem__(self, key: K):
        del self.values[key]

    def __contains__(self, key: K):
        return key in self.values

    def keys(self):
        return self.values.keys()

    def iter(self) -> Iterator[V]:
        for _, key, idx in self.q.iter():
            if not self._pop_checks(key, idx):
                continue

            yield self.values[key]

    def copy(self):
        khq = KeyedQueue(ord=self.ord_fn, key=self.key_fn, q_cls=self.q_cls)
        khq.next_ctr = dict(self.next_ctr)
        khq.values = dict(self.values)
        khq.q = self.q.copy()
        return khq

    def push(self, val: V):
        key = self.key_fn(val)
        ord_ = self.ord_fn(val)

        if key not in self.next_ctr:
            self.next_ctr[key] = 0
            self.ref_ctr[key] = 0

        idx = self.next_ctr[key]
        self.next_ctr[key] += 1
        self.ref_ctr[key] += 1

        self.values[key] = val

        self.q.push((ord_, key, idx))

    def _pop_checks(self, key, idx):
        if key not in self.values:
            return False

        # this will not work, if a newer item had been subsequently added

        if self.next_ctr[key] - 1 != idx:
            return False

        return True

    def pop(self) -> V:
        while True:
            ord_, key, idx = self.q.pop()

            if not self._pop_checks(key, idx):
                continue

            self.ref_ctr[key] -= 1

            if self.ref_ctr[key] == 0:
                if key in self.next_ctr:
                    del self.next_ctr[key]
                del self.ref_ctr[key]
            return_ = self.values[key]
            del self.values[key]

            return return_

    def peek(self) -> Optional[V]:
        while True:
            val = self.q.peek()

            if val is None:
                return None

            ord_, key, idx = val

            if key not in self.values:
                self.q.pop()
                continue

            if self.next_ctr[key] - 1 != idx:
                self.q.pop()
                continue

            return self.values[key]
