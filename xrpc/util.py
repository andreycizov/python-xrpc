import traceback
from contextlib import contextmanager

import pytz
import signal
from datetime import datetime
from typing import Union, Any


@contextmanager
def signal_context(signals=(signal.SIGINT, signal.SIGTERM), handler: Union[int, Any] = signal.SIG_IGN):
    prev_hdlrs = [signal.signal(s, handler) for s in signals]

    try:
        yield prev_hdlrs
    finally:
        for s, prev_hdlr in zip(signals, prev_hdlrs):
            signal.signal(s, prev_hdlr)


ISO8601 = '%Y-%m-%dT%H:%M:%S.%fZ'
ISO8601_SYMSAFE = '%Y%m%dT%H%M%S%fZ'


def time_now() -> datetime:
    return datetime.now(tz=pytz.utc)


def time_parse(v, format) -> datetime:
    return datetime.strptime(v, format).replace(tzinfo=pytz.utc)


def _build_callstack(ignore=1):
    assert ignore > 0

    INDENT = '  '

    callstack = '\n'.join([INDENT + line.strip() for line in traceback.format_stack()][:-ignore])

    return callstack


def _log_called_from(logger, pat='', *args):
    if len(pat):
        pat += '\n'

    logger.exception(pat + 'Called from\n%s\n', *args, _build_callstack())


def _log_traceback(logger, pat=''):
    if len(pat):
        pat += '\n'

    try:
        raise KeyError()
    except:
        logger.debug(pat + _build_callstack(ignore=2))
