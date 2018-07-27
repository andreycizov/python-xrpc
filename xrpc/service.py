import socket

import datetime
from dataclasses import dataclass, replace
from functools import partial
from typing import Any, Optional, Type, Callable, Dict, List

from xrpc.const import SERVER_SERDE_INST
from xrpc.dsl import rpc, regular, socketio, signal, DEFAULT_GROUP
from xrpc.generic import build_generic_context
from xrpc.serde.abstract import SerdeStruct, SerdeSet
from xrpc.serde.types import CallableArgsWrapper, CallableRetWrapper
from xrpc.transform import get_rpc, get_regular, get_socketio, get_signal


class Bindable(dict):
    def bind(self, obj: Any):
        return self.__class__({k: replace(v, fn=partial(v.fn, obj)) for k, v in self.items()})


@dataclass
class RPCEntry:
    name: str
    fn: Callable
    conf: rpc
    req: Type[Any]
    res: Type[Any]


class RPCEntrySet(Bindable, Dict[str, RPCEntry]):
    def groups(self):
        r = set()

        for v in self.values():
            r.add(v.conf.group)

        return sorted(list(r))

    def by_group(self, name=DEFAULT_GROUP) -> 'RPCEntrySet':
        return RPCEntrySet({
            k: v for k, v in self.items() if v.conf.group == name
        })

    @property
    def serde(self) -> SerdeStruct:
        ss = SerdeSet.walk(SERVER_SERDE_INST, datetime.datetime)
        ss = ss | SerdeSet.walk(SERVER_SERDE_INST, Optional[str])

        for k, v in self.items():
            if v.conf.exc:
                continue
            ssreq = SerdeSet.walk(SERVER_SERDE_INST, v.req)
            ssres = SerdeSet.walk(SERVER_SERDE_INST, v.res)

            ss = ss | ssreq | ssres

        return ss.struct(SERVER_SERDE_INST)

    @classmethod
    def from_cls(cls, type_):
        type_, ctx = build_generic_context(type_)

        rpcs = get_rpc(type_)
        rpcs_return = RPCEntrySet()

        for rpc_name, rpc_def in rpcs.items():
            fa = CallableArgsWrapper.from_func_cls(type_, rpc_def.fn, )
            fb = CallableRetWrapper.from_func_cls(type_, rpc_def.fn, )

            if rpc_def.conf.exc:
                retannot = fb.spec.annotations.get('return')
                assert retannot == bool, (rpc_def.fn, retannot)

            rpcs_return[rpc_name] = RPCEntry(
                rpc_name, rpc_def.fn, rpc_def.conf, fa, fb
            )

        return rpcs_return


@dataclass
class RegularEntry:
    name: str
    fn: Callable
    conf: regular


class RegularEntrySet(Bindable, Dict[str, RegularEntry]):
    @classmethod
    def from_cls(cls, type_):
        regulars = get_regular(type_)

        r = cls()
        for n, defn in regulars.items():
            r[n] = RegularEntry(
                n,
                defn.fn,
                defn.conf,
            )
        return r


@dataclass
class SocketIOEntry:
    name: str
    fn: Callable[[Optional[List[bool]]], List[socket.socket]]
    conf: socketio


class SocketIOEntrySet(Bindable, Dict[str, SocketIOEntry]):
    @classmethod
    def from_cls(cls, type_):
        sios = get_socketio(type_)

        r = cls()
        for n, (conf, fn) in sios.items():
            r[n] = SocketIOEntry(
                n,
                fn,
                conf
            )
        return r


@dataclass
class SignalEntry:
    name: str
    fn: Callable
    conf: signal


class SignalEntrySet(Bindable, Dict[str, SignalEntry]):
    def to_signal_map(self) -> Dict[int, List[Callable]]:
        r = {}

        for k, v in self.items():
            for code in v.conf.codes:
                if code not in r:
                    r[code] = []
                r[code].append(v.fn)
        return r

    @classmethod
    def from_cls(cls, type_):
        sios = get_signal(type_)

        r = cls()
        for n, (conf, fn) in sios.items():
            r[n] = SignalEntry(
                n,
                fn,
                conf
            )
        return r


@dataclass
class ServiceDefn:
    serde: SerdeStruct
    rpcs: RPCEntrySet

    def __contains__(self, item):
        return item in self.rpcs

    def keys(self):
        return self.rpcs.keys()

    def __getitem__(self, item) -> RPCEntry:
        return self.rpcs[item]

    def bind(self, obj) -> 'ServiceDefn':
        return ServiceDefn(self.serde, self.rpcs.bind(obj))

    @classmethod
    def from_cls(cls, type_):
        x = RPCEntrySet.from_cls(type_)

        return ServiceDefn(x.serde, x)
