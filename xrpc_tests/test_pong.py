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


def wait_items(waiting, max_wait=40):
    wait_till = time_now() + timedelta(seconds=max_wait)
    waiting = list(waiting)

    while wait_till > time_now() and len(waiting):
        to_remove = []
        for x in waiting:
            try:
                x.get(0)

                if len(waiting) == 0:
                    return
                to_remove.append(x)
            except multiprocessing.context.TimeoutError:
                pass
        for x in to_remove:
            waiting.remove(x)
        sleep(1)

    if len(waiting):
        raise TimeoutError(f'{waiting}')


class TestTransform(unittest.TestCase):
    def test_udp(self):
        p = Pool(2)
        url_a = 'udp://127.0.0.1:11134'
        url_b = 'udp://127.0.0.1:11146'
        a = p.apply_async(run_server_a, args=(url_a, url_b))
        b = p.apply_async(run_server_b, args=(url_b,))

        waiting = [a, b]

        wait_items(waiting)

        p.close()
        p.join()
