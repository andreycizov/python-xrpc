import logging
import os
import random
import traceback
from collections import deque
from contextlib import ExitStack
from inspect import getfullargspec, ismethod

import threading
import types
from argparse import ArgumentParser
from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from signal import SIGKILL, SIGTERM, SIGINT
from typing import Callable, Optional, Dict, Deque, TypeVar, Generic, Type, Tuple, Union, List, Any, Iterable, Set

from xrpc.abstract import KeyedQueue
from xrpc.actor import run_server
from xrpc.cli import Parsable
from xrpc.client import ClientConfig, build_wrapper
from xrpc.dsl import rpc, RPCType, regular, signal, DEFAULT_GROUP
from xrpc.error import TimeoutError, TerminationException
from xrpc.logging import logging_config, LoggerSetup, logging_setup, circuitbreaker, cli_main, logging_parser
from xrpc.loop import EventLoop
from xrpc.net import RPCKey
from xrpc.popen import popen
from xrpc.runtime import service, sender, origin, reset, context
from xrpc.serde.types import build_types, ARGS_RET, PairSpec
from xrpc.service import ServiceDefn
from xrpc.trace import trc
from xrpc.transport import Origin, Transport
from xrpc.util import time_now, signal_context


@dataclass
class ClusterConf(Parsable):
    heartbeat: float = 5.
    max_pings: int = 5
    metrics: float = 10.
    retry_delta: float = 0.10
    pending_max: int = 10


@dataclass
class WorkerConf(Parsable):
    processes: int = 3
    threads: int = 7

    @property
    def total(self):
        return self.processes * self.threads


@dataclass
class BrokerConf(Parsable):
    backlog: int = 100
    """Maximum number of unassigned items in the queue"""
    running_backlog: Optional[int] = None
    """How many maximum running tasks are allowed to be"""
    flush_backlog: int = 100
    """Maximum number of items that have not been passed as a result upstream"""


@dataclass
class WorkerMetric:
    brid: Optional[RPCKey]
    jobs: int
    pending: int
    workers: int
    workers_free: int
    broker_url: str


@dataclass
class BrokerMetric:
    workers: int
    capacity: int
    jobs_pending: int
    jobs: int
    assigned: int

    resigns: int
    dones: int

    @property
    def running(self) -> int:
        return self.jobs_pending + self.assigned

    @property
    def flushing(self) -> int:
        return self.jobs - self.running


NodeMetric = Union[WorkerMetric, BrokerMetric]

RequestType = TypeVar('RequestType', Any, str)
ResponseType = TypeVar('ResponseType', Any, str)

WorkerCallable = Callable[[RequestType], ResponseType]


class MetricCollector:
    @rpc(RPCType.Signalling)
    def metrics(self, metric: NodeMetric):
        pass


def get_func_types(fn: WorkerCallable) -> Tuple[Type[RequestType], Type[ResponseType]]:
    if not isinstance(fn, types.FunctionType):
        fn = fn.__call__

    spec = getfullargspec(fn)
    is_method = ismethod(fn)
    annot = build_types(spec, is_method, allow_missing=True)
    arg = next(PairSpec(spec, is_method)(None))

    return annot[arg.name], annot[ARGS_RET]


def worker_inst(logger_config: LoggerSetup, fn: WorkerCallable, unix_url: str):
    def sigint_handler(code, frame):
        stack_list = traceback.StackSummary.extract(
            traceback.walk_stack(frame),
            limit=None,
            capture_locals=True,
        )
        stack_list.reverse()

        for f, x in zip(stack_list, traceback.format_list(stack_list)):
            locs = None

            if hasattr(f, 'locals'):
                locs = f.locals

            xs = x.split('\n')
            if len(xs) and xs[-1] == '':
                xs = xs[:-1]
            for y in xs:
                logging.getLogger('traceback').error('%s', y)

            # logging.getLogger('traceback.locals').error('%s', locs)

    with ExitStack() as es:
        stacks = [
            logging_setup(logger_config),
            circuitbreaker(main_logger='broker'),
            signal_context(signals=(SIGINT,), handler=sigint_handler)
        ]

        for stack in stacks:
            es.enter_context(stack)

        logging.getLogger('worker_inst').info(f'Start %s', unix_url)

        # use the callable's type hints in order to serialize and deserialize parameters

        cls_req, cls_res = get_func_types(fn)

        should_inst = False

        try:
            run_server(WorkerForkInst, WorkerForkInst(), {DEFAULT_GROUP: unix_url})
        except ForkException:
            should_inst = True

        if should_inst:
            run_server(WorkerInst[cls_req, cls_res], WorkerInst(fn), {
                DEFAULT_GROUP: unix_url,
                BACKEND: 'unix://#bind'
            })


class ForkException(BaseException):
    pass


class WorkerForkInst:
    @rpc(RPCType.Signalling)
    def fork(self):
        trc('0').error('%s', sender())
        if os.fork() == 0:
            context().actor.get('signal')._fork_transport_patch()
            raise ForkException()

    @rpc(exc=True)
    def exc(self, exc: BaseException) -> bool:
        trc('0').error('%s', repr(exc))
        return False

    @signal()
    def exit(self):
        raise TerminationException()

    @regular()
    def announce(self) -> Optional[float]:
        s = service(WorkerAnnounce, origin())
        s.bk_announce(True, os.getpid())

        return None


def worker_thread_main(fn, unix_url: str):
    cls_req, cls_res = get_func_types(fn)

    run_server(WorkerThreadInst[cls_req, cls_res], WorkerThreadInst(fn), {
        DEFAULT_GROUP: unix_url
    })


class WorkerThreadInst(Generic[RequestType, ResponseType]):
    def __init__(self, fn: WorkerCallable):
        self.fn = fn
        self.cls_req, self.cls_res = get_func_types(self.fn)

    @regular()
    def init(self) -> Optional[float]:
        x: WorkerInst[self.cls_req, self.cls_res] = service(WorkerInst[self.cls_req, self.cls_res], origin())
        x.bk_started()
        return None

    @rpc(RPCType.Signalling)
    def put(self, payload: RequestType):
        ret = self.fn(payload)

        x: WorkerInst[self.cls_req, self.cls_res] = service(WorkerInst[self.cls_req, self.cls_res], origin())
        x.bk_done(ret)


BACKEND = 'backend'


class WorkerInst(Generic[RequestType, ResponseType]):
    def __init__(self, fn: WorkerCallable):
        self.fn = fn
        self.cls_req, self.cls_res = get_func_types(self.fn)
        self.started = False
        self.thread_addr: Optional[str] = None
        self.thread = None

    @rpc(RPCType.Signalling)
    def put(self, payload: Optional[RequestType]):
        s: WorkerThreadInst[self.cls_req, self.cls_res] = service(
            WorkerThreadInst[self.cls_req, self.cls_res],
            self.thread_addr,
            group=BACKEND,
        )
        s.put(payload)

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_started(self):
        self.thread_addr = sender()
        reset(self.check_started, None)
        reset(self.announce, 0)

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_done(self, ret: ResponseType):
        s = service(Worker[self.cls_req, self.cls_res], origin(DEFAULT_GROUP), group=DEFAULT_GROUP)
        s.bk_done(ret)

    @rpc(exc=True, group=BACKEND)
    def bk_exc(self, exc: ConnectionAbortedError) -> bool:
        trc().error('Lost connection to the worker thread')
        raise TerminationException()

    @rpc(exc=True)
    def exc(self, exc: ConnectionAbortedError) -> bool:
        raise TerminationException()

    @regular()
    def start_bg(self):
        self.thread = threading.Thread(target=worker_thread_main, args=(self.fn, origin(BACKEND)))
        self.thread.daemon = True
        self.thread.start()
        self.started = True

    @regular()
    def announce(self) -> Optional[float]:
        s = service(WorkerAnnounce, origin())
        s.bk_announce(False, os.getpid())

        return None

    @regular(initial=5)
    def check_started(self):
        raise TerminationException()

    @signal(codes=(SIGTERM,))
    def exit(self):
        raise TerminationException()


@dataclass(frozen=False)
class WorkerLoad:
    occupied: int = 0
    capacity: int = 0


SchedKeyQueue = KeyedQueue[datetime, RPCKey, 'SchedKey']


@dataclass(frozen=True)
class SchedKey:
    when: datetime
    key: RPCKey

    @classmethod
    def now(cls, key: RPCKey):
        return SchedKey(time_now(), key)

    @classmethod
    def queue(cls) -> SchedKeyQueue:
        return KeyedQueue(ord=SchedKey.ord_fn,
                          key=SchedKey.key_fn)

    @classmethod
    def ord_fn(cls, x: 'SchedKey'):
        return x.when

    @classmethod
    def key_fn(cls, x: 'SchedKey'):
        return x.key


def process_queue(
        jobs_pending_done: SchedKeyQueue,
        fn: Callable[[RPCKey], Optional[float]],
        tol: float = 0.
) -> Optional[float]:
    # sb = service(Broker[self.cls_req, self.cls_res], self.url_broker, group=BACKEND)

    now = time_now()

    while True:
        val = jobs_pending_done.peek()

        if val is None:
            return None

        dsec = (val.when - now).total_seconds()

        if dsec > tol:
            return dsec

        val = jobs_pending_done.pop()

        next_timeout = fn(val.key)

        if next_timeout:
            jobs_pending_done.push(SchedKey(now + timedelta(seconds=next_timeout), val.key))


class WorkerAnnounce:
    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_announce(self, is_fork: bool, wpid: int):
        raise NotImplementedError()


class Worker(Generic[RequestType, ResponseType], WorkerAnnounce):
    def __init__(
            self,
            cls_req: Type[RequestType], cls_res: Type[ResponseType],
            conf: ClusterConf,
            url_broker: Origin,
            fn: WorkerCallable[RequestType, ResponseType],
            url_metrics: Optional[str] = None,
            par_conf: WorkerConf = WorkerConf(),
    ):
        self.brid: Optional[RPCKey] = None
        self.has_started = False
        self.has_terminated = False
        self.par_conf = par_conf

        self.load = WorkerLoad()

        self.cls_req = cls_req
        self.cls_res = cls_res

        self.conf = conf
        self.url_broker = url_broker
        self.url_metrics = url_metrics

        self.fn = fn

        # todo: a worker A with PID 1 dies. Then we create a new worker B and it gets the same PID
        # todo: we still don't know that A is dead but already know about B
        # todo: we then realise A had died, so we try to evict it, instead killing B
        self.workers_fork_addrs: Dict[str, int] = {}
        self.worker_addrs: Dict[str, int] = {}

        self.jobs: Dict[RPCKey, RequestType] = {}
        self.jobs_res: Dict[RPCKey, ResponseType] = {}
        self.jobs_workers: Dict[RPCKey, str] = {}
        self.workers_jobs: Dict[str, RPCKey] = {}
        self.workers_free: KeyedQueue[str, str, str] = KeyedQueue()

        self.workers_fork: Deque[str] = deque()

        self.jobs_pending_done: SchedKeyQueue = SchedKey.queue()

    def _start_worker_inst(self):
        popen(worker_inst, logging_config(), self.fn, origin(BACKEND))

    @regular()
    def ep(self) -> Optional[float]:
        for y in range(self.par_conf.processes):
            self._start_worker_inst()
        return None

    def _free(self, jid: RPCKey) -> Optional[str]:
        if jid not in self.jobs_workers:
            return None

        wid = self.jobs_workers[jid]
        del self.jobs_workers[jid]
        del self.workers_jobs[wid]
        return wid

    def _evict(self, jid: RPCKey):
        del self.jobs[jid]
        wid = self._free(jid)

        if jid in self.jobs_res:
            del self.jobs_res[jid]

        if wid is not None:
            if wid in self.workers_free:
                del self.workers_free[wid]

            wpid = self.worker_addrs[wid]
            del self.worker_addrs[wid]

            self.load.capacity -= 1
            self.load.occupied -= 1

            os.kill(wpid, SIGKILL)

    def _fork(self):
        sel_wfid = random.choice(list(self.workers_fork_addrs.keys()))
        self.workers_fork.append(sel_wfid)
        reset(self.push_fork, 0)

    def _evict_all(self, terminating=False):
        for jid in list(self.jobs.keys()):
            self._evict(jid)

            if not terminating:
                self._fork()

    def _brid_new(self, brid: RPCKey):
        should_reset = self.brid is not None and self.brid != brid
        self.brid = brid

        # todo when an brid change happens
        # todo there's a chance the jid will not be assigned to any workers
        # todo yet it's gointg to be listed in jobs.

        if should_reset:
            self._evict_all()

    @rpc(RPCType.Signalling)
    def assign(self, brid: RPCKey, jid: RPCKey, jreq: RequestType):
        # todo: currently sends to an address will cause an exception if we're lucky enough to
        # todo: send a task right after the worker had died (and before transport realised that)

        sb = service(Broker[self.cls_req, self.cls_res], self.url_broker)

        if brid != self.brid:
            trc('brid').error('%s != %s %s', brid, self.brid, jid)
            reset(self.push_announce, 0)
            sb.bk_assign(brid, jid, False)
            return

        if jid in self.jobs:
            # is there any scenario where a job may be assigned to something else ?
            trc('kno').error('%s', jid)
            sb.bk_assign(brid, jid, True)
            return

        if len(self.workers_free) == 0 or len(self.jobs_res) >= self.conf.pending_max:
            sb.bk_assign(brid, jid, False)
            return

        nwid = self.workers_free.pop()

        self.jobs[jid] = jreq
        self.jobs_workers[jid] = nwid
        self.workers_jobs[nwid] = jid

        s = service(WorkerInst[self.cls_req, self.cls_res], nwid, group=BACKEND)
        s.put(jreq)

        self.load.occupied += 1

        sb.bk_assign(brid, jid, True)

    @rpc(RPCType.Signalling)
    def resign(self, brid: RPCKey, jid: RPCKey, reason: Optional[str] = None):
        if brid != self.brid:
            trc('brid').error('%s != %s %s', brid, self.brid, jid)
            reset(self.push_announce, 0)
            return

        self._push_done(jid)

        if jid not in self.jobs:
            # [w-1] resignation notice may appear after worker had successfully finished the job
            # [w-1] in such a scenario, a broker must report resignation as a failure by checking it's finish log
            trc('unk').error('%s', jid)
            return

        if jid in self.jobs_res:
            trc('done').error('%s', jid)
            return

        self._evict(jid)
        self._fork()
        return

    @rpc(RPCType.Signalling)
    def registered(self, brid: RPCKey):
        if self.brid != brid:
            self._brid_new(brid)

    @rpc(RPCType.Signalling)
    def done_ack(self, brid: RPCKey, jid: RPCKey):
        if brid != self.brid:
            trc('brid').error('%s != %s %s', brid, self.brid, jid)
            reset(self.push_announce, 0)
            return

        if jid in self.jobs_res:
            self._evict(jid)

            if jid in self.jobs_pending_done:
                del self.jobs_pending_done[jid]

    def _push_done(self, jid: RPCKey):
        sb = service(Broker[self.cls_req, self.cls_res], self.url_broker)

        if jid in self.jobs:
            if jid in self.jobs_res:
                sb.bk_done(self.brid, jid, True, self.jobs_res[jid])
            else:
                sb.bk_done(self.brid, jid, False)

        return self.conf.retry_delta

    @regular()
    def push_done(self) -> Optional[float]:
        return process_queue(
            self.jobs_pending_done,
            self._push_done,
            self.conf.retry_delta * 0.1
        )

    @regular()
    def push_fork(self) -> Optional[float]:
        while len(self.workers_fork):
            wid = self.workers_fork.popleft()

            if self.load.capacity >= self.par_conf.total:
                continue

            s = service(WorkerForkInst, wid, group=BACKEND)
            # this specific call _may_ cause the event loop to re-loop.
            s.fork()
        return None

    @regular(initial=None)
    def push_announce(self) -> float:
        s = service(Broker[self.cls_req, self.cls_res], self.url_broker, group=DEFAULT_GROUP)
        s.bk_announce(self.brid, self.load)

        return self.conf.heartbeat

    @rpc(RPCType.Repliable)
    def metrics(self) -> WorkerMetric:
        return WorkerMetric(
            self.brid,
            len(self.jobs),
            len(self.jobs_pending_done),
            len(self.workers_free) + len(self.workers_jobs),
            len(self.workers_free),
            self.url_broker,
        )

    @regular(initial=None)
    def push_metrics(self) -> float:
        # todo worker should not expose any of the jobs running to the metrics, this is a broker's job
        if self.url_metrics:
            s = service(MetricCollector, self.url_metrics)
            s.metrics(self.metrics())
        return self.conf.metrics if self.url_metrics else None

    @rpc(exc=True, group=BACKEND)
    def bk_exc(self, exc: ConnectionAbortedError) -> bool:
        # one of the workers had disconnected

        host, reason = exc.args
        trc('1').info('%s %s', host, reason)

        if host in self.workers_jobs:
            jid = self.workers_jobs[host]

            self.resign(self.brid, jid)
        elif host in self.workers_fork_addrs and not self.has_terminated:
            raise ValueError(f'WorkerInst `{host}` had died')

        return True

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_done(self, res: ResponseType):
        wid = sender()

        if wid not in self.workers_jobs:
            trc('1').error('%s', wid)
            return

        jid = self.workers_jobs[wid]

        if jid not in self.jobs_workers:
            trc('1').error('%s', jid)
            return

        self.jobs_res[jid] = res
        self.jobs_pending_done.push(SchedKey(time_now(), jid))
        reset(self.push_done, 0)

        wid = self.jobs_workers[jid]
        del self.jobs_workers[jid]
        del self.workers_jobs[wid]

        self.load.occupied -= 1
        self.workers_free.push(wid)

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_announce(self, is_fork: bool, wpid: int):
        sdr = sender()

        trc('0').error('%s %s %s', sdr, is_fork, wpid)

        if is_fork:
            if sdr in self.workers_fork_addrs:
                return
            else:
                self.workers_fork_addrs[sdr] = wpid

                s = service(WorkerForkInst, sdr, group=BACKEND)

                for _ in range(self.par_conf.threads):
                    trc('01').error('%s', _)
                    s.fork()
            return

        if sdr in self.worker_addrs:
            return

        trc('1').debug('%s', sdr)

        self.worker_addrs[sdr] = wpid
        self.workers_free.push(sdr)
        self.load.capacity += 1

        if self.load.capacity == self.par_conf.processes * self.par_conf.threads:
            trc('capa').debug('capacity reached')
            self._started()

        if self.has_started:
            self.push_announce()

    def _started(self):
        self.has_started = True

        reset(self.startup_timeout, None)
        reset(self.push_announce, 0)

        if self.url_metrics:
            reset(self.push_metrics, 0)

    @regular(initial=10.)
    def startup_timeout(self):
        assert False, f'Could not reach the required capacity ' \
                      f'{self.load.capacity}/{self.par_conf.processes * self.par_conf.threads}'

    @signal()
    def exit(self):
        self.has_terminated = True

        for wfpid in self.workers_fork_addrs.values():
            os.kill(wfpid, SIGTERM)

        while len(self.workers_free):
            wid = self.workers_free.pop()
            wpid = self.worker_addrs[wid]
            del self.worker_addrs[wid]

            os.kill(wpid, SIGKILL)

        self._evict_all(terminating=True)

        try:
            s = service(Broker[self.cls_req, self.cls_res], self.url_broker, ClientConfig(timeout_total=1.))
            s.bk_unregister()
        except TimeoutError:
            logging.getLogger('exit').error('Could not contact broker')

        raise TerminationException()


@dataclass(frozen=False)
class WorkerState:
    pings_remaining: int
    load: WorkerLoad


@dataclass(frozen=False)
class JobState(Generic[RequestType, ResponseType]):
    req: RequestType
    created: datetime
    attempts: int = 0
    started: Optional[datetime] = None
    res: Optional[ResponseType] = None
    finished: Optional[datetime] = None


class BrokerResult(Generic[ResponseType]):
    @staticmethod
    def _finished_ack(jid: RPCKey):
        s = service(BrokerFlushAck, sender(), group=DEFAULT_GROUP)
        s.flush_ack(jid)

    @rpc(RPCType.Signalling)
    def finished(self, jid: RPCKey, jres: ResponseType):
        logging.getLogger('finished').warning('unused %s', jres)

        self._finished_ack(jid)


class BrokerFlushAck:
    @rpc(RPCType.Signalling)
    def flush_ack(self, jid: RPCKey):
        raise NotImplementedError()


class BrokerEntry(Generic[ResponseType]):
    @rpc(RPCType.Repliable)
    def assign(self, id_: RPCKey, pars: RequestType) -> bool:
        pass


class Broker(Generic[RequestType, ResponseType], BrokerEntry[ResponseType], BrokerFlushAck):
    def __init__(
            self,
            cls_req: Type[RequestType],
            cls_res: Type[ResponseType],
            conf: ClusterConf,
            url_results: Optional[str] = None,
            url_metrics: Optional[str] = None,
            par_conf: BrokerConf = BrokerConf(),
    ):
        self.cls_req = cls_req
        self.cls_res = cls_res

        self.conf = conf
        self.par_conf = par_conf
        self.url_results = url_results
        self.url_metrics = url_metrics

        self.workers: Dict[Origin, WorkerState] = {}
        self.workers_brids: Dict[Origin, RPCKey] = {}

        self.jobs: Dict[RPCKey, JobState[RequestType, ResponseType]] = {}
        self.jobs_pending: KeyedQueue[RPCKey, RPCKey, RPCKey] = KeyedQueue()

        self.jobs_workers: Dict[RPCKey, Origin] = {}
        self.workers_jobs: Dict[Origin, List[RPCKey]] = {}

        self.jobs_pending_assign: SchedKeyQueue = SchedKey.queue()
        self.jobs_pending_resign: SchedKeyQueue = SchedKey.queue()
        self.jobs_pending_flush: SchedKeyQueue = SchedKey.queue()

        self.jobs_cancel: Set[RPCKey] = set()

        self.resigns = 0
        self.dones = 0

        # todo transform broker to a ticket-based system instead

    def _job_new(self, jid: RPCKey, jreq: RequestType):
        trc('0').debug('%s %s', jid, jreq)

        self.jobs[jid] = JobState(jreq, time_now())
        self.jobs_pending.push(jid)

        reset(self.push_push_assign, 0)

    def _job_clean(self, jid: RPCKey) -> str:
        trc('0').error('%s', jid)
        wid = self.jobs_workers[jid]
        del self.jobs_workers[jid]
        self.workers_jobs[wid].remove(jid)
        reset(self.push_push_assign, 0)
        return wid

    def _job_resign(self, jid: RPCKey):
        self.resigns += 1

        self._job_clean(jid)

        if jid in self.jobs_pending:
            del self.jobs_pending[jid]
        if jid in self.jobs_pending_assign:
            del self.jobs_pending_assign[jid]
        if jid in self.jobs_pending_resign:
            del self.jobs_pending_resign[jid]

        if jid in self.jobs_cancel:
            del self.jobs[jid]
            self.jobs_cancel.remove(jid)
        else:
            self.jobs_pending.push(jid)

            self.jobs[jid].started = None
            reset(self.push_push_assign, 0)

    def _job_done(self, jid: RPCKey, jres: ResponseType):
        self.dones += 1

        self._job_clean(jid)
        self.jobs[jid].finished = time_now()
        self.jobs[jid].res = jres

        self.jobs_pending_flush.push(SchedKey.now(jid))
        reset(self.push_flush, 0)

    def _job_flush(self, jid: RPCKey):
        del self.jobs[jid]

        if jid in self.jobs_pending_flush:
            del self.jobs_pending_flush[jid]

    def _push_assign(self, jid: RPCKey) -> Optional[float]:
        wid = self.jobs_workers[jid]
        brid = self.workers_brids[wid]
        s = service(Worker[self.cls_req, self.cls_res], wid)
        s.assign(brid, jid, self.jobs[jid].req)

        return self.conf.retry_delta

    @regular(initial=None)
    def push_assign(self):
        return process_queue(
            self.jobs_pending_assign,
            self._push_assign,
            self.conf.retry_delta * 0.1,
        )

    def _push_resign(self, jid: RPCKey) -> Optional[float]:
        wid = self.jobs_workers[jid]
        brid = self.workers_brids[wid]
        s = service(Worker[self.cls_req, self.cls_res], wid)
        s.resign(brid, jid)

        return self.conf.retry_delta

    @regular(initial=None)
    def push_resign(self):
        return process_queue(
            self.jobs_pending_resign,
            self._push_resign,
            self.conf.retry_delta * 0.1,
        )

    def _push_flush(self, jid: RPCKey) -> Optional[float]:
        if not self.url_results:
            self.flush_ack(jid)
        else:
            assert self.jobs[jid].finished
            s = service(BrokerResult[self.cls_res], self.url_results, group=DEFAULT_GROUP)
            s: BrokerResult[ResponseType]
            s.finished(jid, self.jobs[jid].res)
            return self.conf.retry_delta

    @regular(initial=None)
    def push_flush(self):
        return process_queue(
            self.jobs_pending_flush,
            self._push_flush,
            self.conf.retry_delta * 0.1,
        )

    @rpc(RPCType.Signalling)
    def flush_ack(self, jid: RPCKey):
        if jid not in self.jobs:
            return

        self._job_flush(jid)

    @regular(initial=None)
    def push_push_assign(self):
        def spread(iter_obj: Iterable[Tuple[str, int]]):
            for wid, capa in iter_obj:
                for i in range(capa):
                    yield wid

        def eat(obj: Deque[RPCKey]):
            while len(obj):
                try:
                    yield obj.pop()
                except IndexError:
                    return

        w_caps = (
            (wid, max(0, self.workers[wid].load.capacity - len(self.workers_jobs[wid])))
            for wid, wst
            in self.workers.items()
        )

        jobs_workers = zip(spread(w_caps), eat(self.jobs_pending))

        for wid, jid in jobs_workers:
            trc('1').debug('%s %s', wid, jid)

            self.jobs[jid].started = time_now()
            self.jobs[jid].attempts += 1

            self.jobs_workers[jid] = wid
            self.workers_jobs[wid].append(jid)

            self.jobs_pending_assign.push(SchedKey.now(jid))
            reset(self.push_assign, 0)

    @rpc(RPCType.Repliable)
    def query(
            self,
            since: Optional[RPCKey] = None,
            until: Optional[RPCKey] = None,
            order_by: Optional[Tuple[str, bool]] = None,
            limit: Optional[int] = None
    ) -> Optional[List[Tuple[RPCKey, JobState[RequestType, ResponseType]]]]:
        items = self.jobs.items()

        if order_by:
            order_by_field, order_by_forward = order_by

            order_by_field_defn = [x for x in fields(JobState) if x.name == order_by_field]

            if not len(order_by_field_defn):
                return None

            items = [x for x in items if getattr(x[1], order_by_field) is not None]

            items = sorted(items, key=lambda x: getattr(x[1], order_by_field))

        if limit:
            items = items[:limit]

        r = []

        for k, v in items:
            if since is not None and k < since:
                continue

            if until is not None and k > until:
                continue

            r.append((k, v))

        return r

    def _brid_check(self, brid: RPCKey):
        wid = sender()
        if self.workers_brids.get(wid) != brid:
            trc('brid', depth=2).error('%s', brid)
            return True
        return False

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_assign(self, brid: RPCKey, jid: RPCKey, ok: bool):
        sdr = sender()
        if sdr not in self.workers:
            trc('3').error('%s', sdr)
            return

        if self._brid_check(brid):
            return

        if jid not in self.jobs:
            trc('1').error('%s', jid)
            return

        if jid not in self.jobs_workers:
            trc('2').error('%s', jid)
            return

        if jid not in self.workers_jobs[sdr]:
            trc('5').error('%s', jid)
            return

        if jid in self.jobs_pending_assign:
            del self.jobs_pending_assign[jid]

        self._worker_pings(brid, sdr)

        if ok:
            return
        else:
            if jid in self.jobs_pending_resign:
                del self.jobs_pending_resign[jid]

            self._job_resign(jid)

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_done(self, brid: RPCKey, jid: RPCKey, ok: bool, res: Optional[ResponseType] = None):
        w = service(Worker[self.cls_req, self.cls_res], sender())
        w.done_ack(brid, jid)

        if self._brid_check(brid):
            return

        if jid not in self.jobs:
            trc('1').error('%s', jid)
            return

        if self.jobs[jid].finished:
            trc('2').warning('%s', jid)
            return

        if jid in self.jobs_pending_assign:
            del self.jobs_pending_assign[jid]

        if jid in self.jobs_pending_resign:
            del self.jobs_pending_resign[jid]

        if jid not in self.jobs_workers or sender() != self.jobs_workers[jid]:
            return

        self._worker_pings(brid, sender())

        if ok:
            self._job_done(jid, res)
        else:
            self._job_resign(jid)

    @rpc()
    def metrics(self) -> BrokerMetric:
        return BrokerMetric(
            len(self.workers),
            sum(x.load.capacity for x in self.workers.values()),
            len(self.jobs_pending),
            len(self.jobs),
            len(self.jobs_workers),
            self.resigns,
            self.dones
        )

    @regular(initial=None)
    def push_metrics(self) -> float:
        s = service(MetricCollector, self.url_metrics)
        s.metrics(self.metrics())

        return self.conf.metrics

    @rpc(RPCType.Repliable)
    def assign(self, jid: RPCKey, jreq: RequestType) -> bool:
        sp = len(self.jobs_pending)

        if sp > 0 and sp >= self.par_conf.backlog:
            trc('pending').debug('%s', jid)
            return False

        sr = len(self.jobs_workers)

        if self.par_conf.running_backlog and sr > 0 and sr >= self.par_conf.running_backlog:
            trc('running').debug('%s', jid)
            return False

        sf = len(self.jobs_pending_flush)
        if sf > 0 and sf >= self.par_conf.flush_backlog:
            trc('flush').debug('%s', jid)
            return False

        if jid not in self.jobs:
            self._job_new(jid, jreq)

        return True

    @rpc(RPCType.Repliable)
    def cancel(self, jid: RPCKey) -> bool:
        if jid not in self.jobs:
            return False

        if jid in self.jobs_workers:
            self.jobs_cancel.add(jid)
            return self.resign(jid)
        else:
            self._job_flush(jid)

            if jid in self.jobs_pending:
                del self.jobs_pending[jid]

        return True

    @rpc(RPCType.Repliable)
    def resign(self, jid: RPCKey, reason: Optional[str] = None) -> bool:
        if jid not in self.jobs:
            trc('0').debug('%s %s', jid, reason)
            return False

        if jid not in self.jobs_workers:
            trc('1').debug('%s %s', jid, reason)
            return False

        self.jobs_pending_resign.push(SchedKey.now(jid))
        reset(self.push_resign, 0)

        return True

    def _worker_conf_changed(self):
        if self.url_metrics:
            reset(self.push_metrics, 0)
        reset(self.push_push_assign, 0)

    def _worker_new(self, wbrid: RPCKey, wid: Origin, load: WorkerLoad):
        trc().debug('%s %s %s', wbrid, wid, load)

        self.workers[wid] = WorkerState(self.conf.max_pings, WorkerLoad(occupied=0, capacity=load.capacity))
        self.workers_jobs[wid] = []
        self.workers_brids[wid] = wbrid

        self._worker_conf_changed()

    def _worker_lost(self, wid: Origin):
        trc().debug('%s', wid)

        for jid in list(self.workers_jobs[wid]):
            self._job_resign(jid)

        del self.workers[wid]
        del self.workers_jobs[wid]
        del self.workers_brids[wid]

        self._worker_conf_changed()

    @rpc(group=BACKEND)
    def bk_register(self, load: WorkerLoad) -> Optional[RPCKey]:
        wid = sender()

        if wid in self.workers:
            # a worker needs to be unregistered first
            return None

        wbrid = RPCKey.new()

        self._worker_new(wbrid, wid, load)

    @rpc(RPCType.Durable, group=BACKEND)
    def bk_unregister(self):
        wid = sender()

        if wid in self.workers:
            self._worker_lost(wid)

    def _worker_pings(self, brid: RPCKey, wid: str):
        if wid not in self.workers:
            return

        if self.workers_brids[wid] != brid:
            return

        wst = self.workers[wid]

        wst.pings_remaining = self.conf.max_pings

    @rpc(type=RPCType.Signalling, group=BACKEND)
    def bk_announce(self, wbrid: Optional[RPCKey], load: WorkerLoad):
        wid = sender()

        trc('1').debug('%s', wid)

        if wid not in self.workers:
            self._worker_new(RPCKey.new(), wid, load)

        brid = self.workers_brids[wid]

        if wbrid != brid:
            s = service(Worker[self.cls_req, self.cls_res], wid, ClientConfig(timeout_total=1))
            s.registered(self.workers_brids[wid])

        # todo only ping workers through announce

        wst = self.workers[wid]

        flag_changed = wst.load.capacity != load.capacity

        wst.load.capacity = load.capacity
        self._worker_pings(brid, wid)

        if flag_changed:
            self._worker_conf_changed()

    @regular()
    def gc(self) -> float:
        for k in list(self.workers.keys()):
            self.workers[k].pings_remaining -= 1

            trc().debug('%s %s', k, self.workers[k])

            if self.workers[k].pings_remaining <= 0:
                self._worker_lost(k)

        return self.conf.heartbeat

    @signal()
    def exit(self):
        raise TerminationException()

    @regular()
    def startup(self):
        if self.url_metrics:
            reset(self.push_metrics, 0)

        return None


def main(server_url,
         conf=ClientConfig(timeout_total=5),
         **kwargs):
    service_type = Broker[str, str]
    T: Type[service_type] = service_type.__class__

    t = Transport.from_url('udp://0.0.0.0')

    with t:
        ts = EventLoop()
        ets = ts.transport_add(t)
        pt = ServiceDefn.from_cls(service_type)
        r: T = build_wrapper(pt, ets, server_url, conf=conf)

        print(r.metrics())


def parser():
    parser = ArgumentParser()

    logging_parser(parser)

    parser.add_argument(
        '-A'
        '--address',
        dest='server_url',
        default='udp://127.0.0.1:2345'
    )

    return parser


if __name__ == '__main__':
    cli_main(main, parser())
