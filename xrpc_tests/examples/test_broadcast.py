import logging
import unittest
from contextlib import contextmanager
from multiprocessing.pool import Pool
from typing import TypeVar, Type, ContextManager

from time import sleep

from xrpc.client import build_wrapper, ClientConfig
from xrpc.error import HorizonPassedError
from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc.examples.generic import GenericRPC, Data
from xrpc.server import run_server
from xrpc.service import ServiceDefn
from xrpc.transport import Transport, RPCTransportStack
from xrpc_tests.examples.test_pong import wait_items
from xrpc.popen import cov


def run_times(a, b):
    with cov():
        try:
            rpc = BroadcastClientRPC(b)

            run_server(BroadcastClientRPC, rpc, [a])
        except KeyboardInterrupt:
            pass
        except:
            logging.getLogger(__name__).exception('')
            raise


def run_times_2(a, b):
    with cov():
        try:
            rpc = BroadcastRPC(b)

            run_server(BroadcastRPC, rpc, [a])
        except KeyboardInterrupt:
            pass
        except:
            logging.getLogger(__name__).exception('')
            raise


class TestBroadcast(unittest.TestCase):
    def test_udp(self):
        with Pool(2) as p:
            a = p.apply_async(run_times)
            b = p.apply_async(run_times_2)

            wait_items([b, a])

        p.join()
