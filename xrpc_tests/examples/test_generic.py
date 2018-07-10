import logging
import unittest
from contextlib import contextmanager
from multiprocessing.pool import Pool
from typing import TypeVar, Type, ContextManager

from time import sleep

from xrpc.client import build_wrapper, ClientConfig
from xrpc.error import HorizonPassedError
from xrpc.examples.generic import GenericRPC, Data
from xrpc.server import run_server
from xrpc.service import ServiceDefn
from xrpc.transport import Transport, RPCTransportStack
from xrpc_tests.examples.test_pong import wait_items
from xrpc.popen import cov


def run_times():
    with cov():
        try:
            rpc = GenericRPC(int, timeout=5)

            run_server(GenericRPC[int], rpc, ['udp://127.0.0.1:7483'])
        except KeyboardInterrupt:
            pass
        except:
            logging.getLogger(__name__).exception('')
            raise


T = TypeVar('T')


@contextmanager
def build_ts(rpc: Type[T], addr='udp://127.0.0.1:7483', conf=ClientConfig(timeout_total=5)) -> ContextManager[T]:
    t = Transport.from_url('udp://127.0.0.1')

    with t:
        ts = RPCTransportStack([t])
        pt = ServiceDefn.from_obj(rpc)
        r: T = build_wrapper(pt, ts, addr, conf=conf)

        yield r


def run_times_2():
    with cov():
        try:
            rpc = GenericRPC[int]

            t = Transport.from_url('udp://127.0.0.1')

            with t:
                ts = RPCTransportStack([t])
                pt = ServiceDefn.from_obj(rpc)
                r: GenericRPC[int] = build_wrapper(pt, ts, 'udp://127.0.0.1:7483', conf=ClientConfig(timeout_total=5))

                while True:
                    try:
                        a = r.process(5)

                        assert a == Data(5), a
                        break
                    except HorizonPassedError:
                        sleep(0)

                while True:
                    try:
                        a = r.process_blunt(5)

                        assert a == 5, a
                        return
                    except HorizonPassedError:
                        sleep(0)
        except:
            logging.getLogger(__name__).exception('')
            raise


class TestGenericServer(unittest.TestCase):
    def test_udp(self):
        with Pool(2) as p:
            a = p.apply_async(run_times)
            b = p.apply_async(run_times_2)

            wait_items([b, a])

        p.join()
