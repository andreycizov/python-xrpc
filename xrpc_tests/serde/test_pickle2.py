import unittest
from typing import NamedTuple, Optional

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet


class ObjR3(NamedTuple):
    i: int
