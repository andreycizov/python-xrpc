from argparse import ArgumentParser
from typing import Any

from dataclasses import fields, dataclass, _MISSING_TYPE, MISSING

from xrpc.logging import _dict_split_prefix
from xrpc.serde.types import is_union




class Parsable:
    @classmethod
    def parser(cls, prefix: str, argparse: ArgumentParser):
        for f in fields(cls):
            if f.default_factory is not MISSING:
                default = f.default_factory()
            else:
                default = f.default

            type_ = f.type
            dest = f'{prefix}_{f.name}'

            if is_union(f.type):
                args = f.type.__args__

                if len(args) == 2 and args[-1] == type(None):
                    type_ = args[0]
                else:
                    assert False, f

            help = None

            if hasattr(f, '__doc__'):
                help = f.__doc__

            argparse.add_argument(
                '--' + dest,
                dest=dest,
                default=default,
                type=type_,
                help=help,
            )
            print(f)

    @classmethod
    def from_parser(cls, prefix, d):
        a, b = _dict_split_prefix(d, prefix + '_')

        return b, cls(**a)