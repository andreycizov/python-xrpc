import threading
from typing import Optional, Callable, Dict, TypeVar, Type, Any, List, Union
from urllib.parse import urlparse, ParseResult, urlunparse, urlencode, parse_qsl

from dataclasses import dataclass

import xrpc
from xrpc.client import ClientConfig, ServiceWrapper
from xrpc.loop import EventLoop
from xrpc.service import ServiceDefn
from xrpc.trace import log_tr_exec_in, log_tr_exec_out, trc
from xrpc.transport import Origin
from xrpc.util import time_now

RUNTIME_TL = threading.local()
CTX_NAME = 'rpc_config'
CACHE_NAME = 'rpc_cache'


@dataclass
class ExecutionContext:
    actor: 'xrpc.actor.Actor'
    el: EventLoop
    chans: Dict[str, int]
    """Channels of `el` available in this context by these names"""
    chan_def: Optional[str] = None
    """Default channel to send messages from (as the client)"""

    origin: Optional[Origin] = None
    reply: Optional[Callable[[Any], None]] = None

    def exec(self, __origin, __fn: Callable, *args, **kwargs):
        is_ok = False
        r = None

        idx = context_push(self)

        t_start = time_now()

        log_tr_exec_in.debug('[%s %s] Name=%s %s %s %s %s', idx, __origin, __fn, is_ok, args, kwargs, r)

        try:
            r = __fn(*args, **kwargs)
            is_ok = True
            return r
        finally:
            t_end = time_now()
            secs = (t_start - t_end).total_seconds()

            if secs > 0.01:
                log_tr_exec_out.error('[%s %s], Name=%s (%s)', idx, __origin, __fn, secs)

            log_tr_exec_out.debug('[%s %s] Name=%s %s %s %s %s', idx, __origin, __fn, is_ok, args, kwargs, r)
            context_pop(idx)


def context_push(config: Optional[ExecutionContext]):
    if not hasattr(RUNTIME_TL, CTX_NAME):
        setattr(RUNTIME_TL, CTX_NAME, [])

    ctx = getattr(RUNTIME_TL, CTX_NAME)
    ctx.append(config)

    return len(ctx) - 1


def context_pop(idx: int):
    ctx = getattr(RUNTIME_TL, CTX_NAME)

    # this is required for calls that have returned early, but since have started processing an another executable
    new_ctx = ctx[:idx] + ctx[idx + 1:]

    setattr(RUNTIME_TL, CTX_NAME, new_ctx)


def context() -> ExecutionContext:
    return getattr(RUNTIME_TL, CTX_NAME)[-1]


def context_raw() -> Optional[List[ExecutionContext]]:
    try:
        return getattr(RUNTIME_TL, CTX_NAME)
    except AttributeError:
        return None


T = TypeVar('T')


def cache_get(obj: Type[T]) -> ServiceDefn:
    if not hasattr(RUNTIME_TL, CACHE_NAME):
        nd = {}
        setattr(RUNTIME_TL, CACHE_NAME, nd)

    cache = getattr(RUNTIME_TL, CACHE_NAME)

    if obj not in cache:
        cache[obj] = ServiceDefn.from_cls(obj)

    return cache[obj]


def origin(chan=None) -> Origin:
    ctx = context()
    return ctx.el.transport(ctx.chans[chan or ctx.chan_def]).origin


def sender() -> Origin:
    origin = context().origin
    assert origin is not None, 'sender() can only be called in RPC functions'

    return origin


def reply(ret: Any):
    reply = context().reply
    assert reply is not None, 'reply() can only be called in RPC functions'

    reply(ret)


def reset(name: Union[Callable, str], new_val: Any):
    if callable(name):
        name = name.__name__

    reg: xrpc.actor.RegularRunner = context().actor.get('regular')
    reg.reset(name, new_val)


TA = TypeVar('TA')
TB = TypeVar('TB')


def _masquerade(origin: str, orig: ServiceDefn, new: ServiceDefn, **map: str) -> str:
    """build an origin URL such that the orig has all of the mappings to new defined by map"""

    origin: ParseResult = urlparse(origin)

    prev_maps = {}

    if origin.query:
        prev_maps = {k: v for k, v in parse_qsl(origin.query)}

    r_args = {}

    for new_k, orig_k in map.items():

        assert new_k in new.rpcs, [new_k, new.rpcs]
        assert orig_k in orig.rpcs, [orig_k, orig.rpcs]

        # todo: check if the definitions are the same

        new_v = new.rpcs[new_k]
        orig_v = orig.rpcs[orig_k]

        if orig_k in prev_maps:
            orig_k = prev_maps[orig_k]

        assert new_v.res == orig_v.res, [new_v.res, orig_v.res]
        assert new_v.req == orig_v.req, [new_v.req, orig_v.req]

        r_args[new_k] = orig_k

    return urlunparse(origin._replace(query=urlencode(r_args)))


def masquerade(origin: str, orig: Type[TA], new: Type[TB], **map: str) -> str:
    """Make ``orig`` appear as new"""

    return _masquerade(origin, cache_get(orig), cache_get(new), **map)


def service(obj: Type[T], dest: Origin, conf: Optional[ClientConfig] = None, group=None, **kwargs) -> T:
    if conf is None:
        conf = ClientConfig(**kwargs)

    ctx = context()

    defn = cache_get(obj)

    group = ctx.chan_def if group is None else group

    assert group is not None, 'service() can only be called within execution context that references channels'

    trc('1').debug('%s %s %s', obj, dest, group)

    try:
        chan_idx = ctx.chans[group]
    except KeyError:
        raise ValueError(f'{group} not found in {ctx.chans.keys()}')
    else:
        return ServiceWrapper(defn, conf, ctx.el.transport(chan_idx), dest)
