import sys
from typing import TypeVar, Tuple

from xrpc.serde.abstract import SerdeStepContext


def build_generic_context(t, ctx=SerdeStepContext()):
    def mmaps(pars, args):
        maps = dict(zip(pars, args))

        maps = {k: ctx.generic_vals.get(k, v) if isinstance(v, TypeVar) else v for k, v in maps.items()}

        uninst = {k: isinstance(maps[k], TypeVar) for k in maps}

        if any(uninst.values()):
            raise ValueError(f'Not all generic parameters are instantiated: {uninst.keys()}')

        return maps

    if sys.version_info >= (3, 7):
        if not hasattr(t, '__origin__'):
            return t, ctx

        if t.__origin__ is tuple:
            #raise NotImplementedError()
            args = tuple(ctx.generic_vals.get(x, x) for x in t.__args__)
            #raise NotImplementedError()
            return Tuple[args], ctx
        else:

            maps = mmaps(t.__origin__.__parameters__, t.__args__)

            return t.__origin__, SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})
    else:
        if not hasattr(t, '_gorg'):
            return t, ctx

        maps = mmaps(t._gorg.__parameters__, t.__args__ if t.__args__ else t._gorg.__parameters__)

        return t, SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})