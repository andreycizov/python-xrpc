import sys

from xrpc.serde.abstract import SerdeStepContext


def build_generic_context(t, ctx=SerdeStepContext()):
    if sys.version_info >= (3, 7):
        if not hasattr(t, '__origin__'):
            return t, ctx

        maps = dict(zip(t.__origin__.__parameters__, t.__args__))

        return t.__origin__, SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})
    else:
        is_generic = hasattr(t, '_gorg')

        if is_generic:
            if t.__parameters__:
                raise ValueError(f'Not all generic parameters are instantiated: {t} {t.__parameters__}')

            maps = dict(zip(t._gorg.__parameters__, t.__args__))

            ctx = SerdeStepContext(mod=ctx.mod, generic_vals={**ctx.generic_vals, **maps})
        return t, ctx