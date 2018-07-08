import heapq
import math
from collections import deque
from datetime import datetime
from typing import TypeVar, Generic, List, Optional, Any, Deque

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


T = TypeVar('T')
H = TypeVar('H')


class Queue(Generic[T, H]):
    def __init__(self, initial: Optional[List[T]] = None):
        self.h: H = self._init(initial)

    def _init(self, initial: List[T]) -> H:
        raise NotImplementedError()

    def push(self, val: T):
        raise NotImplementedError()

    def pop(self) -> T:
        raise NotImplementedError()

    def peak(self) -> Optional[T]:
        raise NotImplementedError()


class BinaryQueue(Queue[T, Deque[T]]):
    def _init(self, initial: Optional[List[T]]) -> Deque[T]:
        if initial:
            return deque(sorted(initial))
        else:
            return deque()

    def push(self, val: T):
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

    def pop(self) -> T:
        r = self.h.popleft()
        return r

    def peak(self) -> Optional[T]:
        if len(self.h):
            return self.h[0]
        else:
            return None


class HeapQueue(Queue[T, List[T]]):
    def _init(self, initial: Optional[List[T]]) -> List[T]:
        if initial:
            initial = list(initial)
            heapq.heapify(initial)
            return initial
        else:
            return []

    def push(self, val: T):
        heapq.heappush(self.h, val)

    def pop(self) -> T:
        return heapq.heappop(self.h)

    def peak(self) -> Optional[T]:
        if len(self.h):
            return self.h[0]
        else:
            return None

    def pushpop(self, val: T):
        return heapq.heappushpop(self.h, val)

    def replace(self, val: T):
        return heapq.heapreplace(self.h, val)
