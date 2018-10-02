import inspect
import sys

from argparse import ArgumentParser
from dataclasses import fields, dataclass, MISSING
from datetime import timedelta
from typing import Any, TypeVar, Type, Dict, List, Optional

from xrpc.generic import build_generic_context
from xrpc.logging import _dict_split_prefix
from xrpc.serde.types import is_union

T = TypeVar('T')


@dataclass
class ParsableConf:
    names: List[str]
    type: Optional[Any] = None
    action: Optional[str] = None


def is_list(t):
    t, _ = build_generic_context(t)

    if sys.version_info >= (3, 7):
        if hasattr(t, '__origin__'):
            return t.__origin__ is list
    if inspect.isclass(t):
        return issubclass(t, List)
    else:
        return False


def _guess_type_action(dest, type_, default) -> ParsableConf:
    action = 'store'

    if is_union(type_):
        args = type_.__args__

        if len(args) == 2 and args[-1] == type(None):
            type_ = args[0]
        else:
            assert False, type_

    if type_ is bool:
        type_ = None

        if default is True:
            action = 'store_false'
        else:
            action = 'store_true'
    elif type_ is timedelta:
        type_ = lambda x: timedelta(seconds=int(x))
    elif is_list(type_):
        type_, = type_.__args__
        action = 'append'
    return ParsableConf([dest], type_, action)


class Parsable:
    @classmethod
    def overrides(cls) -> Dict[str, ParsableConf]:
        return {}

    @classmethod
    def parser(cls, prefix: str, argparse: ArgumentParser):
        overrides = cls.overrides()

        for f in fields(cls):
            if f.default_factory is not MISSING:
                default = f.default_factory()
            else:
                default = f.default

            type_ = f.type

            name = f'{f.name}'

            if prefix:
                dest = f'{prefix}_{name}'
            else:
                dest = name

            if name in overrides:
                conf = overrides[name]
            else:
                conf = _guess_type_action('--' + dest, type_, default)

            help = None

            if hasattr(f, '__doc__'):
                help = f.__doc__

            if True:

                if help:
                    help += ' '
                else:
                    help = ''

                help += '(default: %(default)s)'

            type_action = {}
            if conf.type:
                type_action['type'] = conf.type

            if conf.action:
                type_action['action'] = conf.action

            if default is not MISSING:
                type_action['default'] = default

            argparse.add_argument(
                *conf.names,
                dest=dest,
                help=help,
                **type_action
            )

    @classmethod
    def from_parser(cls: Type[T], prefix, d, forget_other=True) -> T:
        filtered_fields, unfiltered_fields = _dict_split_prefix(d, prefix + '_' if prefix else '')

        field_set = {x.name for x in fields(cls)}

        filtered_fields = {k: v for k, v in filtered_fields.items() if k in field_set}
        unfiltered_fields = {k: v for k, v in filtered_fields.items() if k not in field_set}
        unfiltered_fields = {**unfiltered_fields, **unfiltered_fields}

        if forget_other:
            return cls(**filtered_fields)
        else:
            return unfiltered_fields, cls(**filtered_fields)
