import heapq
from datetime import datetime
from typing import TypeVar, Generic, List, Optional

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


class HeapQueue(Generic[T]):
    def __init__(self, initial: Optional[List[T]] = None):
        if initial:
            initial = list(initial)
            heapq.heapify(initial)
        else:
            initial = []

        self.h = initial

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
