import base64
import logging
import multiprocessing
import os
import sys
import zlib
from contextlib import ExitStack, contextmanager

import dill
import signal
import subprocess
from dataclasses import dataclass
from datetime import timedelta
from time import sleep
from typing import Callable, Any, Tuple, Dict, List, Optional

from xrpc.util import signal_context, time_now, _build_callstack


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


def argv_encode(x: Any):
    return base64.b64encode(zlib.compress(dill.dumps(x), level=9)).decode()


def argv_decode(x: Any):
    return dill.loads(zlib.decompress(base64.b64decode(x)))


def wait_all(*waiting, max_wait=5.) -> List[int]:
    wait_till = time_now() + timedelta(seconds=max_wait)
    waiting = {i: v for i, v in enumerate(waiting)}

    r: Dict[int, int] = {}

    while wait_till > time_now() and len(waiting):
        for i, x in list(waiting.items()):
            try:
                r[i] = x.wait(0)

                del waiting[i]
            except multiprocessing.context.TimeoutError:
                pass
            except subprocess.TimeoutExpired:
                pass
        sleep(0.03)

    if len(waiting):
        raise TimeoutError(f'{waiting}')

    return [v for _, v in sorted(r.items())]


@dataclass
class PopenStruct:
    traceback: str
    fn: Callable
    args: Tuple[Any]
    kwargs: Dict[str, Any]


def popen_encode(fn, *args, **kwargs) -> List[str]:
    return [
        sys.executable,
        '-m',
        __name__,
        f'{fn.__module__}.{fn.__name__}',
        argv_encode(PopenStruct(
            _build_callstack(ignore=2),
            fn,
            args,
            kwargs
        )),
    ]


def popen(fn, *args, **kwargs) -> subprocess.Popen:
    """
    Please ensure you're not killing the process before it had started properly
    :param fn:
    :param args:
    :param kwargs:
    :return:
    """

    args = popen_encode(fn, *args, **kwargs)

    logging.getLogger(__name__).debug('Start %s', args)

    p = subprocess.Popen(args)
    return p


class PopenStackException(Exception):
    def __init__(self, abuser: Optional[subprocess.Popen] = None):
        super().__init__(abuser)

        self.abuser = abuser

    def __str__(self):
        return f'We have timed out and therefore killed all of the subprocesses (ABUSER: {self.abuser})'


class PopenStack:
    """This is mainly for GC purposes. We wait instead of SIGKILL immediately."""

    def __init__(self, timeout=None):
        self.stack: List[subprocess.Popen] = []
        self.timeout = timeout

    def add(self, x: subprocess.Popen):
        self.stack.append(x)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.getLogger(__name__).debug('Exit %s %s %s', exc_type, exc_val, exc_tb)
        if exc_type:
            for x in self.stack:
                x.send_signal(signal.SIGKILL)

            raise PopenStackException()
        else:
            should_kill = False
            item = None

            for x in self.stack:
                try:
                    code = x.wait(timeout=self.timeout)
                except subprocess.TimeoutExpired:
                    item = x
                    should_kill = True
                    break

            if should_kill:
                for x in self.stack:
                    x.send_signal(signal.SIGKILL)
                raise PopenStackException(item)


def _popen_defn() -> Optional[PopenStruct]:
    return argv_decode(sys.argv[2])


def popen_main():
    with ExitStack() as es:
        if os.environ.get('COVERAGE_PROCESS_START'):
            es.enter_context(cov())

        def popen_signal_handler(code, frame):
            if callable(prev_handler[code]):
                prev_handler[code](code, frame)

        codes = (signal.SIGINT, signal.SIGTERM)

        with signal_context(signals=codes, handler=popen_signal_handler) as prev_handlers:
            prev_handler = {k: v for k, v in zip(codes, prev_handlers)}

            defn = None

            try:
                defn: PopenStruct = _popen_defn()
            except:
                import traceback
                import pprint

                fmtd = pprint.pformat(sys.argv)
                print(defn.traceback, file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                sys.stderr.flush()
                raise ValueError(f'Cannot unpickle arguments, called with {fmtd}')

            defn.fn(*defn.args, **defn.kwargs)


if __name__ == '__main__':
    popen_main()
