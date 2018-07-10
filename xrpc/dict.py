from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, TypeVar, Generic

from xrpc.error import HorizonPassedError
from xrpc.net import RPCKey
from xrpc.util import time_now


class RPCLogDict:
    def __init__(self, horizon: datetime):
        self.horizon = horizon
        self.values: Dict[RPCKey, Any] = {}

    def __getitem__(self, item: RPCKey):
        if item.timestamp < self.horizon:
            raise HorizonPassedError(self.horizon)
        return self.values[item]

    def __contains__(self, item):
        try:
            _ = self[item]
            return True
        except KeyError:
            return False

    def __setitem__(self, key: RPCKey, value: Any):
        if key.timestamp < self.horizon:
            raise HorizonPassedError(self.horizon)

        self.values[key] = value

    def set_horizon(self, new_horizon: datetime):
        if new_horizon < self.horizon:
            raise HorizonPassedError(self.horizon)

        for key in [k for k in self.values.keys() if k.timestamp < new_horizon]:
            del self.values[key]

        self.horizon = new_horizon


K = TypeVar('K')
V = TypeVar('V')


@dataclass
class CachedDictRecord:
    ts: datetime
    v: V


class Hidrate(Generic[K, V]):
    def __call__(self, cd: 'CachedDict', k: K) -> V:
        raise NotImplementedError()


class CachedDict(Generic[K, V]):
    def __init__(self, hidrate_fn: Hidrate, max_timeout: timedelta = timedelta(seconds=120)):
        self.values: Dict[K, CachedDictRecord] = {}
        self.hidrate_fn: Hidrate = hidrate_fn
        self.max_timeout = max_timeout

    def __setitem__(self, key, value):
        self.values[key] = CachedDictRecord(time_now(), value)

    def __getitem__(self, item):
        now = time_now()

        if item in self.values:
            item_rec = self.values[item]

            if item_rec.ts > now - self.max_timeout:
                return item_rec.v
            else:
                return self.hidrate_fn(self, item)

        return self.hidrate_fn(self, item)

class ObjectDict:
    def __init__(self, **kwargs):
        self._dict = dict(**kwargs)

    def __setattr__(self, key, value):
        if key == '_dict':
            super().__setattr__(key, value)
        else:
            self._dict[key] = value

    def __getattr__(self, item):
        try:
            return self._dict[item]
        except KeyError:
            raise AttributeError()
