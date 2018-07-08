from datetime import datetime
from typing import Any, Dict

from xrpc.error import HorizonPassedError
from xrpc.net import RPCKey


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