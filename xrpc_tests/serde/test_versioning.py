import unittest
from typing import Optional

from dataclasses import dataclass

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet


@dataclass
class ObjV1:
    a: int


@dataclass
class ObjV2Err(ObjV1):
    b: int


@dataclass
class ObjV2(ObjV1):
    b: Optional[int]


class TestNewVersioning(unittest.TestCase):
    def setUp(self):
        self.serde = SerdeSet.walk(SERVER_SERDE_INST, ObjV1)
        self.serde = self.serde.merge(SerdeSet.walk(SERVER_SERDE_INST, ObjV2Err))
        self.serde = self.serde.merge(SerdeSet.walk(SERVER_SERDE_INST, ObjV2))
        self.serde = self.serde.struct(SERVER_SERDE_INST)

    def test_v1_v2_err(self):
        with self.assertRaises(ValueError):
            x = self.serde.deserialize(ObjV2Err, self.serde.serialize(ObjV1, ObjV1(5)))

    def test_v1_v2_ok(self):
        x = self.serde.deserialize(ObjV2, self.serde.serialize(ObjV1, ObjV1(5)))
        self.assertEqual(None, x.b)
