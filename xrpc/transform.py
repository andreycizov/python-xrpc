from inspect import getfullargspec
from typing import Dict, Any, Tuple, Callable, NamedTuple

from xrpc.dsl import ATTR_RPC, ATTR_REGULAR, rpc, regular, signal, ATTR_SIGNAL, ATTR_STARTUP, startup, \
    socketio, ATTR_SOCKETIO
from xrpc.generic import build_generic_context


def build_rpc_list(x, conf_attr_name=ATTR_RPC) -> Tuple[str, Any, Any]:
    x, _ = build_generic_context(x)

    r = []
    for attr_name in dir(x):
        attr_val = getattr(x, attr_name)

        if callable(attr_val) and hasattr(attr_val, conf_attr_name):
            r.append((attr_name, attr_val, getattr(attr_val, conf_attr_name)))

    return r


class RegularConf(NamedTuple):
    conf: regular
    fn: Callable


def get_regular(x) -> Dict[str, RegularConf]:
    rs = build_rpc_list(x, ATTR_REGULAR)
    r = {}

    for n, attr, x in rs:
        argspec = getfullargspec(attr)
        rtn_type = argspec.annotations.get('return', None)

        assert rtn_type in [float, int], f'Incorrect return type for {n}: {rtn_type}'
        r[n] = RegularConf(x, attr)

    return r


def get_signal(x) -> Dict[str, Tuple[signal, Callable]]:
    rs = build_rpc_list(x, ATTR_SIGNAL)
    r = {}

    for n, attr, x in rs:
        argspec = getfullargspec(attr)
        rtn_type = argspec.annotations.get('return', None)

        assert rtn_type in [bool, None], f'Incorrect return type for {n}: {rtn_type}'
        r[n] = (x, attr)

    return r


def get_startup(x) -> Dict[str, Tuple[startup, Callable]]:
    rs = build_rpc_list(x, ATTR_STARTUP)
    r = {}

    for n, attr, x in rs:
        r[n] = (x, attr)

    return r


def get_socketio(x) -> Dict[str, Tuple[socketio, Callable]]:
    rs = build_rpc_list(x, ATTR_SOCKETIO)
    r = {}

    for n, attr, x in rs:
        r[n] = (x, attr)

    return r


class RPCCallable(NamedTuple):
    fn: Callable
    conf: rpc


RPCS = Dict[str, RPCCallable]


def get_rpc(x) -> RPCS:
    rs = build_rpc_list(x)

    return {n: RPCCallable(fn, c) for n, fn, c in rs}
