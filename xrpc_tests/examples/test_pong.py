import multiprocessing
import subprocess
import unittest
from contextlib import contextmanager
from datetime import timedelta
from multiprocessing.pool import Pool
from time import sleep

from xrpc.server import run_server
from xrpc.examples.exemplary_rpc import BroadcastClientRPC, BroadcastRPC
from xrpc.util import time_now


@contextmanager
def cov():
    import coverage

    cov = None
    try:
        cov = coverage.process_startup()
        yield
    finally:
        if cov:
            cov.save()


def run_server_a(addr, other_addr, ):
    with cov():
        run_server(BroadcastClientRPC(other_addr), [addr])


def run_server_b(addr):
    with cov():
        run_server(BroadcastRPC(), [addr])


def wait_items(waiting, max_wait=40):
    wait_till = time_now() + timedelta(seconds=max_wait)
    waiting = list(waiting)

    while wait_till > time_now() and len(waiting):
        to_remove = []
        for x in waiting:
            try:
                x.wait(0)

                to_remove.append(x)
            except multiprocessing.context.TimeoutError:
                pass
            except subprocess.TimeoutExpired:
                pass
        for x in to_remove:
            waiting.remove(x)
        sleep(1)

    if len(waiting) and wait_till > time_now():
        raise TimeoutError(f'{waiting}')


class TestTransform(unittest.TestCase):
    def test_udp(self):
        with Pool(2) as p:
            url_a = 'udp://127.0.0.1:11134'
            url_b = 'udp://127.0.0.1:11146'
            a = p.apply_async(run_server_a, args=(url_a, url_b))
            b = p.apply_async(run_server_b, args=(url_b,))

            waiting = [a, b]

            wait_items(waiting)
        print('p.join')
        p.join()
