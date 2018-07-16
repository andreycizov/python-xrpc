from argparse import ArgumentParser
from typing import Any, TypeVar, Type, Dict, Tuple, List, Optional

from dataclasses import fields, dataclass, _MISSING_TYPE, MISSING

from xrpc.logging import _dict_split_prefix
from xrpc.serde.types import is_union

T = TypeVar('T')


@dataclass
class ParsableConf:
    names: List[str]
    type: Optional[Any] = None
    action: Optional[str] = None


def _guess_type_action(dest, type_, default) -> ParsableConf:
    action = None

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
    elif issubclass(type_, List):
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
            dest = f'{prefix}_{name}'

            if name in overrides:
                conf = overrides[name]
            else:
                conf = _guess_type_action('--' + dest, type_, default)

            help = None

            if hasattr(f, '__doc__'):
                help = f.__doc__

            type_action = {}
            if conf.type:
                type_action['type'] = conf.type

            if conf.action:
                type_action['action'] = conf.action

            argparse.add_argument(
                *conf.names,
                dest=dest,
                default=default,
                help=help,
                **type_action
            )

    @classmethod
    def from_parser(cls: Type[T], prefix, d, forget_other=True) -> T:
        a, b = _dict_split_prefix(d, prefix + '_')

        if forget_other:
            return cls(**a)
        else:
            return b, cls(**a)
