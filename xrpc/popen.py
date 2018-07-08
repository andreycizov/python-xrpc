import base64
import pickle
import subprocess
import sys
from typing import NamedTuple, Callable, Any, Tuple, Dict

from xrpc.util import signal_context


def argv_encode(x: Any):
    return base64.b64encode(pickle.dumps(x)).decode()


def argv_decode(x: Any):
    return pickle.loads(base64.b64decode(x))


class PopenStruct(NamedTuple):
    fn: Callable
    args: Tuple[Any]
    kwargs: Dict[str, Any]


def popen(fn, *args, **kwargs):
    args = [
        sys.executable,
        '-m',
        __name__,
        argv_encode(PopenStruct(
            fn,
            args,
            kwargs
        )),
    ]

    p = subprocess.Popen(args)
    return p


def popen_main():
    def popen_signal_handler(frame, code):
        prev_handler()

    with signal_context(handler=popen_signal_handler) as prev_handler:

        try:
            defn: PopenStruct = argv_decode(sys.argv[1])
        except:
            import traceback
            import pprint

            fmtd = pprint.pformat(sys.argv)
            traceback.print_exc(file=sys.stderr)
            sys.stderr.flush()
            raise ValueError(f'Cannot unpickle arguments, called with {fmtd}')

        fn = defn.fn

        fn(*defn.args, **defn.kwargs)


if __name__ == '__main__':
    popen_main()
