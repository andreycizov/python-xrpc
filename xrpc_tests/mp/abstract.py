import logging
import multiprocessing
import subprocess
import sys
import unittest
from contextlib import ExitStack, contextmanager
from datetime import timedelta, datetime
from time import sleep
from typing import Optional

from dataclasses import field, dataclass

from xrpc.logging import LoggerSetup, LL, logging_setup
from xrpc.popen import PopenStack, cov, popen
from xrpc.server import run_server
from xrpc.util import time_now


def helper_main(ls, fn, *args, **kwargs):
    with logging_setup(ls), cov():
        fn(*args, **kwargs)


def server_main(factory_fn, addr, *args, **kwargs):
    logging.getLogger(__name__ + '.server_main').debug('%s %s %s %s', factory_fn, addr, args, kwargs)
    try:
        tp, rpc = factory_fn(addr, *args, **kwargs)

        run_server(tp, rpc, [addr])
    except KeyboardInterrupt:
        return
    except:
        logging.exception('')
        raise


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


@dataclass(frozen=False)
class Timer:
    started: datetime = field(default_factory=time_now)
    max: Optional[float] = None

    def get(self, now=None) -> timedelta:
        if now is None:
            now = time_now()

        elapsed = now - self.started

        if self.max and elapsed.total_seconds() > self.max:
            raise TimeoutError()

        return elapsed

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.get()

    def sleep(self, seconds=0):
        sleep(seconds)
        return self.get()


@dataclass(frozen=False)
class ProcessHelper:
    ls: LoggerSetup = field(default_factory=lambda: LoggerSetup(LL(None, logging.DEBUG), [], ['stream:///stderr']))
    es: ExitStack = field(default_factory=ExitStack)
    ps: PopenStack = field(default_factory=lambda: PopenStack(10))

    def __post_init__(self):
        self.es.enter_context(logging_setup(self.ls))
        self.es.enter_context(self.ps)

    def popen(self, fn, *args, **kwargs):
        b = popen(helper_main, self.ls, fn, *args, **kwargs)
        self.ps.add(b)
        return b

    def wait(self, items, max_wait=10):
        return wait_items(items, max_wait)

    def timer(self, max: Optional[float] = 5.) -> Timer:
        return Timer(max=max)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.es.__exit__(*args)


class ProcessHelperCase(unittest.TestCase):
    def make_ph(self):
        return ProcessHelper()

    def setUp(self):
        self.ps = self.make_ph().__enter__()

    def tearDown(self):
        self.ps.__exit__(*sys.exc_info())
