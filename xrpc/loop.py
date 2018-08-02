import logging
import socket
import time
from contextlib import contextmanager
from datetime import datetime
from functools import reduce
from itertools import count
from typing import Dict, List, Union, Optional, Callable, Tuple

from dataclasses import dataclass

from xrpc.net import RPCPacket
from xrpc.trace import log_tr_net_pkt_in, log_tr_net_pkt_err, log_tr_net_pkt_out
from xrpc.transport import Transport, \
    select_helper, Packet, RPCPacketRaw
from xrpc.util import time_now, _log_called_from, _log_traceback

EVPktMatcherFun = Callable[[RPCPacketRaw], bool]
EVPktHdlr = Callable[[RPCPacketRaw], None]
EVPollHdlr = Callable[[Optional[List[bool]]], None]
EVSleepSchedFun = Callable[[None], Optional[float]]
EVSleepTimerHdlr = Callable[[None], None]

EVExcHdlr = Callable[[BaseException], bool]


def default_exc_handler(exc: BaseException):
    _log_called_from(logging.getLogger(__name__), 'Ignoring uncaught')

    return True


def raise_exc_handler(exc: BaseException, should_log=True):
    if should_log:
        _log_called_from(logging.getLogger(__name__), 'Uncaught')

    return False


@dataclass
class ELPollEntry:
    # what happens if the event loop raises an exception ?
    # what happens if the event loop raises a TerminationException ?
    # who is supposed to catch them ?
    processor: EVPollHdlr
    exc: EVExcHdlr = default_exc_handler


@dataclass
class ELExcEntry:
    exc: EVExcHdlr = default_exc_handler


@dataclass
class ELPktEntry:
    matcher: EVPktMatcherFun
    processor: EVPktHdlr
    exc: EVExcHdlr = default_exc_handler


@dataclass
class ELWaitEntry:
    scheduler: EVSleepSchedFun
    processor: EVSleepTimerHdlr
    exc: EVExcHdlr = default_exc_handler


@contextmanager
def exc_handler(exc: EVExcHdlr):
    try:
        yield
    except BaseException as e:
        try:
            fe = not exc(e)
        except:
            logging.getLogger(__name__).exception('Could not handle the exception here')
            raise
        else:
            if fe:
                raise


class EventLoopEmpty(BaseException):
    pass


@dataclass
class ELSpinLockDetector:
    par: 'EventLoop'

    max_ctr: int = 3
    max_iter_secs: float = 0.01

    sleep_secs: float = 0.02

    flgs: Optional[List[bool]] = None
    flgs_ctr = 0
    time: Optional[datetime] = None

    def step(self, polled_flags):
        if any(polled_flags) and polled_flags == self.flgs:
            self.flgs_ctr += 1
        elif self.flgs is None:
            self.flgs = polled_flags
            self.flgs_ctr = 0
            self.time = time_now()
        else:
            self.flgs = polled_flags
            self.flgs_ctr = 0
            self.time = time_now()

        fa = any(self.flgs) and self.flgs_ctr > self.max_ctr
        vb = fa and ((time_now() - self.time).total_seconds() / self.flgs_ctr)
        fb = fa and vb < self.max_iter_secs

        if fa and fb:
            lgr = self.par.logger('spinlock')
            lgr.warning('%s %s %s', self.flgs_ctr, self.flgs, vb)
            lgr.warning('%s', self.par.nonflat_fds())
            lgr.warning('%s', self.par.transports)
            lgr.warning('%s', self.par.waits)

            _log_traceback(lgr, 'We are in a spinlock')

            time.sleep(self.sleep_secs)


def flatten(z):
    return reduce(lambda a, b: a + b, (x for _, x in z), [])


class EventLoop:
    def __init__(self):
        self.id_ctr = count()
        self.transports: Dict[int, Transport] = {}
        self.stacks: Dict[int, List[Union[ELPollEntry, ELPktEntry]]] = {}
        self.stacks_exc: Dict[int, List[ELExcEntry]] = {}
        self.waits: Dict[int, ELWaitEntry] = {}

    def logger(self, sn=None):
        name = __name__
        if sn:
            name = name + '.' + sn

        return logging.getLogger(name)

    def nonflat_fds(self) -> List[Tuple[int, List[socket.socket]]]:
        r = []
        for k, transport in sorted(self.transports.items()):
            r2 = []
            for y in transport.fds:
                r2.append(y)
            r.append((k, r2))
        return r

    def _is_raw(self, idx: int):
        stack = self.stacks[idx]

        return len(stack) and isinstance(stack[0], ELPktEntry)

    @property
    def max_waits(self) -> Dict[int, Optional[float]]:
        r: Dict[int, Optional[float]] = {}
        for k, v in self.waits.items():
            sched_item = v.scheduler()

            if sched_item is None:
                r[k] = None
                continue

            if sched_item < 0.:
                if -sched_item > 0.010:
                    self.logger('max_waits.e').warning('`%s` (%s < 0.)', v.scheduler, sched_item)
                sched_item = 0.
            r[k] = sched_item

        self.logger('max_waits.o').debug('%s', r)
        return r

    def loop(self, max_wait: Optional[float] = None):
        ts = time_now()

        def bin_none(a, b, fn=min):
            if a is None:
                return b
            elif b is None:
                return a
            else:
                return fn(a, b)

        elsld = ELSpinLockDetector(self)

        while True:
            running_for = (time_now() - ts).total_seconds()

            max_wait_left = None if max_wait is None else max(max_wait - running_for, 0)

            max_wait_owned: Optional[float] = None

            max_waits_curr = self.max_waits

            for k, x in max_waits_curr.items():
                max_wait_owned = bin_none(max_wait_owned, x)

                if x is not None and x <= 0.:
                    wait = self.waits.get(k)

                    if wait is None:
                        continue

                    with exc_handler(wait.exc):
                        wait.processor()

            max_wait_step = bin_none(max_wait_owned, max_wait_left)

            while True:
                if not len(self.waits) and not len(self.transports):
                    self.logger('ex').debug('Exit')
                    raise EventLoopEmpty()

                fds = self.nonflat_fds()

                # todo: SIGTERM is called here.
                try:
                    polled_flags = select_helper(flatten(fds), max_wait=max_wait_step)
                except ConnectionAbortedError:
                    self.logger('coabt.s').debug('%s', fds)
                    continue

                elsld.step(polled_flags)

                self.recv(fds, polled_flags)

                break

            if max_wait_left is not None and max_wait_left == 0:
                break

    def recv_connabt(self, idx: int, has_data, max_iter=5):
        for ctr in count():
            if idx not in self.transports:
                return

            if ctr >= max_iter:
                return

            transport = self.transports[idx]

            try:
                reader = transport.read(has_data)
                raw_packet = next(reader)
                yield raw_packet
            except StopIteration:
                return
            except Exception as e:
                exc_stack = self.stacks_exc[idx]

                for x in exc_stack[::-1]:
                    fl = x.exc(e)

                    if not fl:
                        raise
                    else:
                        return

                self.logger('coerr').debug('%s %s', e, transport)
                return

    def recv(self, nonflat_fds: List[Tuple[int, List[socket.socket]]], polled_flags: Optional[List[bool]] = None):
        # we have a set of transports
        # we've got a set of

        initial_polled_flags = polled_flags

        if polled_flags is None:
            polled_flags = [(k, [True for _ in fds]) for k, fds in nonflat_fds]
        else:
            polled_flags_nonflat = []
            idx = 0

            for k, fds in nonflat_fds:

                curr_polled_flags = []
                for _ in fds:
                    curr_polled_flags.append(polled_flags[idx])
                    idx += 1

                polled_flags_nonflat.append((k, curr_polled_flags))

            polled_flags = polled_flags_nonflat

        self.logger('recv.ep').debug('%s %s %s', nonflat_fds, polled_flags, initial_polled_flags)

        try:

            if not polled_flags:
                return []

            for idx, has_data in polled_flags:
                fa, fb = not any(has_data), idx not in self.transports

                if fa:
                    continue

                if fb:
                    continue

                transport = self.transports[idx]
                stack = self.stacks[idx]

                if len(stack) == 0:
                    self.logger('0st').warning('%s %s', transport, stack)
                    continue

                if len(stack) and isinstance(stack[-1], ELPollEntry):
                    with exc_handler(stack[-1].exc):

                        stack[-1].processor(has_data)
                else:
                    for raw_packet in self.recv_connabt(idx, has_data):
                        packet = RPCPacket.unpack(raw_packet.data)

                        log_tr_net_pkt_in.debug('%s %s %s', transport.origin, raw_packet.addr, packet)

                        raw_rpc_packet = RPCPacketRaw(raw_packet.addr, packet)

                        for st in stack[::-1]:
                            fm = st.matcher(raw_rpc_packet)
                            if fm:
                                with exc_handler(st.exc):
                                    st.processor(raw_rpc_packet)
                                break
                        else:
                            log_tr_net_pkt_err.error('[NO_MATCH] %s %s', raw_packet.addr, packet)
        finally:
            self.logger('recv.ex').debug('%s %s %s', nonflat_fds, polled_flags, initial_polled_flags)

    def send(self, idx: int, packet: RPCPacketRaw):
        chan = self.transports[idx]

        log_tr_net_pkt_out.debug('%s %s %s', chan.origin, packet.addr, packet.packet)

        raw_packet = Packet(packet.addr, packet.packet.pack())

        chan.send(raw_packet)

    def push(self, idx: int, entry: ELPktEntry):
        assert len(self.stacks[idx]) == 0 or all(isinstance(x, ELPktEntry) for x in self.stacks[idx]), self.stacks
        self.stacks[idx].append(entry)

    def push_exc(self, idx: int, entry: ELExcEntry):
        self.stacks_exc[idx].append(entry)

    def push_raw(self, idx: int, entry: ELPollEntry):
        assert len(self.stacks[idx]) == 0, self.stacks[idx]
        self.stacks[idx].append(entry)

    def pop(self, idx: int):
        assert idx in self.transports, (idx, self.transports)
        return self.stacks[idx].pop()

    def transport(self, idx: int) -> 'ELTransportRef':
        self.transports[idx]
        return ELTransportRef(self, idx)

    def wait(self, idx: int) -> 'ELWaitRef':
        assert idx in self.waits
        return ELWaitRef(self, idx)

    def wait_add(self, entry: ELWaitEntry) -> 'ELWaitRef':
        new_id = next(self.id_ctr)

        self.logger('wait.add').debug('%s %s %s', self, entry, new_id)

        self.waits[new_id] = entry

        return ELWaitRef(self, new_id)

    def wait_remove(self, idx: int):
        self.logger('wait.rm').debug('%s %s', self, idx)

        del self.waits[idx]

    def transport_add(self, transport: Transport) -> 'ELTransportRef':
        new_id = next(self.id_ctr)

        self.logger('tran.add').debug('%s %s %s', self, transport, new_id)

        self.transports[new_id] = transport
        self.stacks[new_id] = []
        self.stacks_exc[new_id] = []

        return self.transport(new_id)

    def transport_remove(self, idx: int) -> Transport:
        self.logger('tran.rm').debug('%s %s', self, idx)
        r = self.transports[idx]
        del self.transports[idx]
        del self.stacks[idx]
        del self.stacks_exc[idx]
        return r


@dataclass
class ELWaitRef:
    parent: EventLoop
    idx: int

    def remove(self):
        return self.parent.wait_remove(self.idx)


@dataclass
class ELTransportRef:
    parent: EventLoop
    idx: int

    def loop(self, max_wait: Optional[float] = None):
        return self.parent.loop(max_wait)

    def __getattr__(self, item):
        if item.startswith('__'):
            raise AttributeError(item)

        return getattr(self.parent.transports[self.idx], item)

    def is_alive(self) -> bool:
        try:
            self.parent.transport(self.idx)
            return True
        except KeyError:
            return False

    def send(self, packet: RPCPacketRaw):
        return self.parent.send(self.idx, packet)

    def push(self, entry: ELPktEntry):
        return self.parent.push(self.idx, entry)

    def push_exc(self, entry: ELExcEntry):
        return self.parent.push_exc(self.idx, entry)

    def push_raw(self, entry: ELPollEntry):
        return self.parent.push_raw(self.idx, entry)

    def pop(self):
        return self.parent.pop(self.idx)

    def remove(self):
        return self.parent.transport_remove(self.idx)
