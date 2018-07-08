import signal
from contextlib import contextmanager
from datetime import datetime
from typing import Union, Callable

import pytz


@contextmanager
def signal_context(signals=(signal.SIGINT, signal.SIGTERM), handler: Union[Callable, int] = signal.SIG_IGN):
    prev_hdlrs = [signal.signal(s, handler) for s in signals]

    try:
        yield prev_hdlrs
    finally:
        for s, prev_hdlr in zip(signals, prev_hdlrs):
            signal.signal(s, prev_hdlr)


def time_now() -> datetime:
    return datetime.now(tz=pytz.utc)


def time_parse(v, format) -> datetime:
    return pytz.utc.localize(datetime.strptime(v, format))
