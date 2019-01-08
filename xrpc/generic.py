import logging
import sys
from typing import TypeVar, Tuple, List, Dict

from dataclasses import is_dataclass

from xrpc.serde.abstract import SerdeStepContext


def build_generic_context(t, ctx=SerdeStepContext()):
    logging.getLogger(__name__ + '.ep').debug('%s %s', t, ctx)

    def mmaps(pars, args):
        maps = dict(zip(pars, args))

        maps = {k: ctx.generic_vals.get(v, v) if isinstance(v, TypeVar) else v for k, v in maps.items()}

        uninst = [k for k in maps if isinstance(maps[k], TypeVar)]

        if len(uninst):
            raise ValueError(f'Not all generic parameters are instantiated: {uninst}, {t} {ctx}')

        return maps

    if sys.version_info >= (3, 7):
        if not hasattr(t, '__origin__'):
            return t, ctx

        if t.__origin__ is tuple:
            params, args = t.__parameters__, t.__args__

            args = params[len(args):] + args

            args = tuple(ctx.generic_vals.get(x, x) for x in args)
            logging.getLogger(__name__ + '.tuple.a').debug('%s', args)
            maps = mmaps(params[len(args):], args)
            t = Tuple[args]
        elif t.__origin__ is list:
            params, args = t.__parameters__, t.__args__

            args = params[len(args):] + args

            args = tuple(ctx.generic_vals.get(x, x) for x in args)
            logging.getLogger(__name__ + '.list.a').debug('%s', args)
            maps = mmaps(params[len(args):], args)
            t = List[args[0]]
        elif is_dataclass(t.__origin__):
            params, args = t.__origin__.__parameters__, t.__args__

            logging.getLogger(__name__ + '.dc.a').debug('%s %s %s', params, args, ctx.generic_vals)

            args = params[len(args):] + args
            args = tuple(ctx.generic_vals.get(x, x) for x in args)

            logging.getLogger(__name__ + '.dc.b').debug('%s %s %s %s', t.__origin__, args, t.__origin__[args], ctx.generic_vals)
            t = t.__origin__[args]
            maps = mmaps(t.__origin__.__parameters__, args)
        elif t.__origin__ is dict:
            args = tuple(ctx.generic_vals.get(x, x) for x in t.__args__)

            t = Dict[args]
            maps = mmaps(t.__parameters__, t.__args__)
        else:
            maps = mmaps(t.__origin__.__parameters__, t.__args__)

        ctx = SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})

        logging.getLogger(__name__ + '.exit.3.7').debug('%s %s', t, ctx)

        return t, ctx
    else:
        if not hasattr(t, '_gorg') or t.__args__ is None:
            return t, ctx

        pars, args = t._gorg.__parameters__, t.__args__ if t.__args__ else t._gorg.__parameters__

        logging.getLogger(__name__).debug('%s %s %s %s', t, t._gorg.__parameters__, t.__args__, t._gorg.__parameters__)
        logging.getLogger(__name__).debug('%s %s', pars, args)

        maps = mmaps(pars, args)

        if issubclass(t, List):
            t = List[tuple(ctx.generic_vals.get(x, x) for x in t.__args__)]
        elif issubclass(t, Tuple):
            t = Tuple[tuple(ctx.generic_vals.get(x, x) for x in t.__args__)]
        elif is_dataclass(t):
            t = t._gorg[tuple(ctx.generic_vals.get(x, x) for x in t.__args__)]

        return t, SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})
