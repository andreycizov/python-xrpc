import logging
import sys
from contextlib import contextmanager
from urllib.parse import urlparse, ParseResult, parse_qs

import argparse
import threading
from dataclasses import dataclass
from pygelf import GelfUdpHandler
from typing import NamedTuple, List, Union, Optional, Dict, Callable, TypeVar, Tuple


class LoggerLevel(NamedTuple):
    name: Optional[str]
    level: int

    @property
    def level_name(self) -> str:
        return logging._levelToName.get(self.level, str(self.level))

    def __repr__(self):
        prefix = ''
        if self.name:
            prefix = self.name + '='
        return prefix + self.level_name


LoggerLevelList = List[LoggerLevel]


class LoggerSetup(NamedTuple):
    level: LoggerLevel
    level_names: LoggerLevelList
    urls: List[str]

    @classmethod
    def current(cls):
        return LoggerSetup(
            LL(None, logging.getLogger().level),
            [LL(name, logger.level) for name, logger in logging.root.manager.loggerDict.items()],
            []
        )

    def set_level(self, logger: logging.Logger) -> LoggerLevel:
        r = logger.level
        logger.setLevel(self.level.level)
        return LoggerLevel(None, r)

    def set_level_names(self, logger: logging.Logger, should_log=True) -> List[LoggerLevel]:
        lls: Dict[str, LoggerLevel] = {v.name: v for v in self.level_names}

        if should_log:
            logging.getLogger(__name__).debug('Root logger set at level %s', self.level.level_name)

        r = []
        for logger_name in sorted(lls.keys()):
            ll = lls[logger_name]

            if should_log:
                logging.getLogger(__name__).debug('Logger `%s` set at level %s', logger_name, ll.level_name)

            r.append(LoggerLevel(logger_name, logging.getLogger(logger_name).level))
            logger.getChild(logger_name).setLevel(ll.level)
        return r

    def set_handlers(self, logger: logging.Logger) -> List[logging.Handler]:
        r = []

        log_urls = self.urls

        if not len(log_urls):
            log_urls = ['stream:///stderr']

        for url in log_urls:
            o: ParseResult = urlparse(url)
            r.append(LOGGERS[o.scheme](logger, url))
        return r


def LL(name, level: Union[int, str]) -> LoggerLevel:
    if isinstance(level, str):
        level = logging._nameToLevel[level]

    return LoggerLevel(name, level)


DEFAULT_LOGGER_DEFN = [
    # paramiko shits in INFO all the time
    LL('paramiko', logging.ERROR),
]


def type_level(level, name=None) -> LoggerLevel:
    level = level.upper()

    try:
        return LoggerLevel(name, logging._nameToLevel[level])
    except KeyError:
        raise argparse.ArgumentTypeError(level)


def type_level_name(defn) -> LL:
    logger_name, logger_level = defn.split('=', maxsplit=2)

    return type_level(logger_level, name=logger_name)


def type_logger(url) -> str:
    o: ParseResult = urlparse(url)

    if o.scheme in LOGGERS:
        return url
    else:
        raise argparse.ArgumentTypeError(url.scheme)


LOGGER_PREFIX = 'logger_'


def logging_parser(parser: argparse.ArgumentParser, preset_levels: LoggerLevelList = DEFAULT_LOGGER_DEFN):
    for x in preset_levels:
        assert isinstance(x, LoggerLevel), x

    if preset_levels is None:
        preset_levels = []

    parser = parser.add_argument_group('logging', 'Logging setup group')

    parser.add_argument(
        '-L',
        '--log',
        dest=f'{LOGGER_PREFIX}level',
        type=type_level,
        default=LoggerLevel(None, logging.INFO),
        metavar="LEVEL",
        help="Python logging level (default:  %(default)s)",
        choices=[LL(None, x) for x in logging._levelToName.values()]
    )

    parser.add_argument(
        '-LL',
        dest=f'{LOGGER_PREFIX}level_names',
        type=type_level_name,
        default=preset_levels,
        metavar="LEVEL_SPEC",
        help="Python logging levels per logger name  as per `paramiko.transport=DEBUG` (default:  %(default)s)",
        action='append',
    )

    # how do we manage something like this ?

    parser.add_argument(
        '--logger',
        dest=f'{LOGGER_PREFIX}urls',
        default=[],
        action="append",
        type=type_logger,
        help="Should log to stdout. Not setting any of the loggers overrides this to True",
    )


LoggerHandlerFn = Callable[[logging.Logger, str], logging.Handler]


def _logger_stream(logger: logging.Logger, url: str) -> logging.Handler:
    o: ParseResult = urlparse(url)

    AVAIL = {
        'stderr': sys.stderr,
        'stdout': sys.stdout,
    }

    ch = logging.StreamHandler(AVAIL[o.path.lstrip('/')])

    format = logging.Formatter("[%(asctime)s][%(levelname)s][%(name)s]\t%(message)s")
    ch.setFormatter(format)
    ch.setLevel(logging.NOTSET)

    logger.addHandler(ch)

    return ch


def _logger_gelf(logger: logging.Logger, url: str) -> logging.Handler:
    def _split_path_fields(path):
        def by_len(x):
            if len(x) == 2:
                return x
            elif len(x) == 1:
                return [x[0], "1"]
            else:
                return [None, None]

        return {
            k: v for k, v in
            (by_len(x) for x in (x.split('=', 1) for x in path.split('/') if len(x)))
            if k
        }

    o: ParseResult = urlparse(url)

    kwds_a = _split_path_fields(o.path)

    kwds_b = {}

    if o.query:
        kwds_b = {k: v[-1] if v else '1' for k, v in parse_qs(o.query, keep_blank_values=True).items()}

    kwds = {**kwds_a, **kwds_b}

    ch = GelfUdpHandler(host=o.hostname, port=o.port, chunk_size=1350, debug=True,
                        static_fields=kwds)

    logger.addHandler(ch)

    return ch


LOGGERS: Dict[str, LoggerHandlerFn] = {
    'gelf+udp': _logger_gelf,
    'stream': _logger_stream,
}

TL = threading.local()
TL.logging: Optional[LoggerSetup] = None


def logging_config():
    if TL.logging:
        return TL.logging
    else:
        logging.warning('logging_config called without previously setting up the logging')
        return LoggerSetup.current()


@contextmanager
def logging_setup(conf: LoggerSetup, root: Optional[logging.Logger] = None):
    if root is None:
        root = logging.getLogger()

    a = conf.set_level(root)

    try:
        handlers = conf.set_handlers(root)
        try:
            c = conf.set_level_names(root)
            try:
                last_conf = TL.logging
                TL.logging = conf
                try:
                    yield root
                finally:
                    TL.logging = last_conf
            finally:
                LoggerSetup(a, c, []).set_level_names(root, should_log=False)
        finally:
            for handler in handlers:
                root.removeHandler(handler)
    finally:
        logging.getLogger().setLevel(a.level)


@dataclass
class logging_spec:
    setup: LoggerSetup

    def __call__(self, fn):
        from functools import wraps

        @wraps(fn)
        def wrapped(*args, **kwargs):
            with logging_setup(self.setup) as x:
                return fn(*args, **kwargs)

        return wrapped


T = TypeVar('T')


def _dict_split_prefix(dict: Dict[str, T], prefix: str) -> Tuple[Dict[str, T], Dict[str, T]]:
    a = {}
    b = {}

    for k, v in dict.items():
        if k.startswith(prefix):
            a[k[len(prefix):]] = v
        else:
            b[k] = v
    return a, b


@contextmanager
def circuitbreaker(main_logger='main'):
    try:
        yield
    except SystemExit as e:
        # do not spam the console if ``exit`` is called
        exit(e.code)
    except:
        logging.getLogger(main_logger).exception('Main routine caused an exception')
        exit(-1)


def cli_main(fn, parser: argparse.ArgumentParser, argv=None):
    import sys

    if argv is None:
        argv = sys.argv[1:]

    args: argparse.Namespace = parser.parse_args(argv)

    logger_args, fn_args = _dict_split_prefix(vars(args), f'{LOGGER_PREFIX}')

    fn_args[LOGGER_PREFIX] = LoggerSetup(**logger_args)

    with logging_setup(fn_args[LOGGER_PREFIX]):
        with circuitbreaker():
            fn(**fn_args)
