import gc
import inspect
import logging

from typing import Optional

log_tr = logging.getLogger('xrpc.tr')

log_tr_exec_in = log_tr.getChild('x.i')
log_tr_exec_out = log_tr.getChild('x.o')

log_tr_act = lambda x: log_tr.getChild('a').getChild(x)

log_tr_net_sel_in = log_tr.getChild('n.s.i')
log_tr_net_sel_err = log_tr.getChild('n.s.e')

log_tr_net_obj_in = log_tr.getChild('n.o.i')
log_tr_net_obj_out = log_tr.getChild('n.o.i')

log_tr_net_raw_in = log_tr.getChild('n.r.i')
log_tr_net_raw_out = log_tr.getChild('n.r.o')
log_tr_net_raw_err = log_tr.getChild('n.r.e')

log_tr_net_pkt_in = log_tr.getChild('n.p.i')
log_tr_net_pkt_out = log_tr.getChild('n.p.o')
log_tr_net_pkt_err = log_tr.getChild('n.p.e')

log_tr_net_meta_in = log_tr.getChild('n.m.i')
log_tr_net_meta_out = log_tr.getChild('n.m.o')


def trc(postfix: Optional[str] = None, *, depth=1) -> logging.Logger:
    """
    Automatically generate a logger from the calling function

    :param postfix: append another logger name on top this
    :param depth: depth of the call stack at which to capture the caller name
    :return: instance of a logger with a correct path to a current caller

    """
    x = inspect.stack()[depth]

    mod = inspect.getmodule(x.frame)

    code = x[0].f_code

    try:
        func = next(obj for obj in gc.get_referrers(code) if inspect.isfunction(obj))
        parts = (mod.__name__, f'{func.__qualname__}:{x.lineno}')
    except StopIteration:
        parts = (f'{code.co_filename}:{x.lineno}',)

    if postfix:
        parts += (postfix,)

    logger_name = '.'.join(parts)

    return logging.getLogger(logger_name)
