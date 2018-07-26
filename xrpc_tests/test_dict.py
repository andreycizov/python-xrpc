import unittest

from datetime import timedelta

from xrpc.dict import RPCLogDict, ObjectDict
from xrpc.error import HorizonPassedError
from xrpc.net import RPCKey
from xrpc.util import time_now


class TestDict(unittest.TestCase):
    def test_rpclogdict(self):

        cr = time_now()
        x = RPCLogDict(cr)

        with self.subTest('a'):
            with self.assertRaises(HorizonPassedError):
                x[RPCKey(time_now() - timedelta(seconds=10))] = False

        kv = RPCKey()

        with self.subTest('b'):
            val = True
            x[kv] = val

            self.assertEqual(x[kv], val)

        with self.subTest('c'):

            x.set_horizon(time_now())

            with self.assertRaises(HorizonPassedError):
                x[kv]

    def test_object_dict(self):
        v = ObjectDict()

        with self.assertRaises(AttributeError):
            v.attr