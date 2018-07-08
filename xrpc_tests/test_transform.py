import unittest
from pprint import pprint

from xrpc.dsl import ATTR_REGULAR
from xrpc.examples.exemplary_rpc import ExemplaryRPC
from xrpc.transform import build_rpc_list


class TestTransform(unittest.TestCase):
    def test_transform(self):
        rs = build_rpc_list(ExemplaryRPC)

        pprint(rs)

    def test_other(self):
        rs = build_rpc_list(ExemplaryRPC)


        pprint(rs)

    def test_regu(self):
        rs = build_rpc_list(ExemplaryRPC, ATTR_REGULAR)


        pprint(rs)
