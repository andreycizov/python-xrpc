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
from xrpc.logging import logging_config, logging_setup
from xrpc.server import run_server
from xrpc.service import ServiceDefn
from xrpc.transport import Transport, RPCTransportStack
from xrpc_tests.examples.test_pong import wait_items
from xrpc.popen import cov, popen, PopenStack
from xrpc_tests.impl.test_broker import build_logging


def run_times(lc, a, b):
    with logging_setup(lc), cov():
        try:
            rpc = BroadcastClientRPC(b)

            run_server(BroadcastClientRPC, rpc, [a])
        except KeyboardInterrupt:
            pass
        except:
            logging.getLogger(__name__).exception('')
            raise


def run_times_2(lc, a):
    with logging_setup(lc), cov():
        try:
            rpc = BroadcastRPC()

            run_server(BroadcastRPC, rpc, [a])
        except KeyboardInterrupt:
            pass
        except:
            logging.getLogger(__name__).exception('')
            raise


class TestBroadcast(unittest.TestCase):
    def test_udp(self):
        with build_logging(), PopenStack(timeout=10.) as s:
            ua = 'udp://127.0.0.1:23443'
            ub = 'udp://127.0.0.1:23445'
            a = popen(run_times, logging_config(), ua, ub)
            b = popen(run_times_2, logging_config(), ub)

            s.add(a)
            s.add(b)

            wait_items([b, a])
