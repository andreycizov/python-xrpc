import logging
import sys
import unittest
from contextlib import ExitStack

from dataclasses import field, dataclass

from xrpc.logging import LoggerSetup, LL, logging_setup
from xrpc.popen import PopenStack, cov, popen
from xrpc.server import run_server


def helper_main(ls, fn, *args, **kwargs):
    with logging_setup(ls), cov():
        fn(*args, **kwargs)


def server_main(factory_fn, addr, *args, **kwargs):
    try:
        tp, rpc = factory_fn(addr, *args, **kwargs)

        run_server(tp, rpc, [addr])
    except KeyboardInterrupt:
        return
    except:
        logging.exception('')
        raise


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
