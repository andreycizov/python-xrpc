import logging
import multiprocessing
import shutil
import signal
import subprocess
import sys
import unittest
from contextlib import ExitStack
from datetime import timedelta, datetime
from itertools import count
from os import environ
from tempfile import mkdtemp
from time import sleep
from typing import Optional

from dataclasses import field, dataclass

from xrpc.actor import run_server
from xrpc.logging import LoggerSetup, LL, logging_setup
from xrpc.popen import PopenStack, cov, popen, _popen_defn
from xrpc.util import time_now


def helper_main(ls, fn, *args, **kwargs):
    with logging_setup(ls), cov():
        try:
            fn(*args, **kwargs)
        except:
            defn = _popen_defn()
            tb = None if defn is None else defn.traceback
            if tb:
                logging.getLogger('helper_main').exception('From %s %s %s\nPopen-called from:\n%s', fn, args, kwargs,
                                                           tb)
            else:
                logging.getLogger('helper_main').exception('From %s %s %s', fn, args, kwargs)
            raise


def server_main(factory_fn, addr, *args, **kwargs):
    logging.getLogger(__name__ + '.server_main').debug('%s %s %s %s', factory_fn, addr, args, kwargs)
    try:
        tp, rpc = factory_fn(addr, *args, **kwargs)

        run_server(tp, rpc, [addr])
    finally:
        logging.getLogger('server_main').exception('Exited with: %s %s', factory_fn, sys.exc_info())


def server_main_new(factory_fn, addrs, *args, **kwargs):
    logging.getLogger(__name__ + '.server_main').debug('%s %s %s %s', factory_fn, addrs, args, kwargs)
    try:
        tp, rpc = factory_fn(*args, **kwargs)

        run_server(tp, rpc, addrs)
    finally:
        logging.getLogger('server_main').exception('Exited with: %s %s', factory_fn, sys.exc_info())


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
        sleep(0.03)

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


DEFAULT_LEVEL = logging.INFO

if environ.get('DEBUG', None):
    DEFAULT_LEVEL = logging.DEBUG


@dataclass(frozen=False)
class ProcessHelper:
    ls: LoggerSetup = field(default_factory=lambda: LoggerSetup(LL(None, DEFAULT_LEVEL), [], ['stream:///stderr']))
    es: ExitStack = field(default_factory=ExitStack)
    ps: PopenStack = field(default_factory=lambda: PopenStack(10))

    def popen(self, fn, *args, **kwargs):
        b = popen(helper_main, self.ls, fn, *args, **kwargs)
        self.ps.add(b)
        return b

    def wait(self, items, max_wait=10):
        return wait_items(items, max_wait)

    def timer(self, max: Optional[float] = 5.) -> Timer:
        return Timer(max=max)

    def __enter__(self):
        self.es.enter_context(logging_setup(self.ls))
        self.es.enter_context(self.ps)

        return self

    def __exit__(self, *args):
        self.es.__exit__(*args)


class ProcessHelperCase(unittest.TestCase):
    def _get_ls(self) -> LoggerSetup:
        return LoggerSetup(LL(None, DEFAULT_LEVEL), [
        ], ['stream:///stderr'])

    def step(self):
        logging.getLogger(self.__class__.__name__).warning(f'[{next(self.steps)}]')

    def make_ph(self):
        r = ProcessHelper(self._get_ls())
        return r

    def signal_handler(self, signal, frame):
        self.tearDown()

    def setUp(self):
        self.steps = count()
        self.ps = self.make_ph().__enter__()
        self.dtemp = mkdtemp()
        signal.signal(signal.SIGINT, self.signal_handler)

    def tearDown(self):
        self.ps.__exit__(*sys.exc_info())
        shutil.rmtree(self.dtemp)
