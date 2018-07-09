import unittest
import dataclasses
from typing import TypeVar

from xrpc.const import SERVER_SERDE_INST
from xrpc.serde.abstract import SerdeSet

T = TypeVar('T')


@dataclasses.dataclass
class Obj:
    i: int = 0


def build_worker_serde():
    a = SerdeSet.walk(SERVER_SERDE_INST, Obj)

    s = a.merge(a)
    return s.struct(SERVER_SERDE_INST)


class TestDataclasses(unittest.TestCase):
    def test_pickle_0(self):
        WorkerSerde = build_worker_serde()

        import pickle
        pickle.dumps(WorkerSerde)

        WorkerSerde.deserialize(Obj, {'i': 5})
        WorkerSerde.serialize(Obj, Obj(5))
