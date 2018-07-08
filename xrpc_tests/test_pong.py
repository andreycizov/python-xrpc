import multiprocessing
import unittest
from datetime import timedelta
from multiprocessing.pool import Pool
from time import sleep

from xrpc.server import run_server
from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc.util import time_now


def run_server_a(addr, other_addr, ):
    run_server(BroadcastClientRPC(other_addr), [addr])


def run_server_b(addr):
    run_server(BroadcastRPC(), [addr])


class TestTransform(unittest.TestCase):
    def test_udp(self):
        p = Pool(2)
        wait_till = time_now() + timedelta(seconds=20)
        url_a = 'udp://127.0.0.1:11134'
        url_b = 'udp://127.0.0.1:11146'
        a = p.apply_async(run_server_a, args=(url_a, url_b))
        b = p.apply_async(run_server_b, args=(url_b,))

        total_get = 0
        waiting = [a, b]

        while wait_till > time_now():
            to_remove = []
            for x in waiting:
                try:
                    x.get(0)
                    total_get += 1

                    if total_get == len([a,b]):
                        return
                    to_remove.append(x)
                except multiprocessing.context.TimeoutError:
                    print('timout', waiting)
            for x in to_remove:
                waiting.remove(x)
            sleep(1)
        raise TimeoutError()
