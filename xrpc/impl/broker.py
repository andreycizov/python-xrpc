import logging
import os
import types
from argparse import ArgumentParser
from contextlib import ExitStack
from datetime import datetime, timedelta
from inspect import getfullargspec, ismethod
from signal import SIGKILL
from typing import Callable, Optional, Dict, Deque, TypeVar, Generic, Type, Tuple, Union, List, Any, Iterable, Set

from dataclasses import dataclass

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
from xrpc.runtime import service, sender, origin, reset
from xrpc.serde.types import build_types, ARGS_RET, PairSpec
from xrpc.service import ServiceDefn
from xrpc.trace import trc
from xrpc.transport import Origin, Transport
from xrpc.util import time_now


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


@dataclass
class BrokerConf(Parsable):
    backlog: int = 100
    """Maximum number of unassigned items in the queue"""
    result_backlog: int = 100
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
    jobs_pending: int
    jobs: int
    assigned: int


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


def worker_inst(logger_config: LoggerSetup, multiplex: int, fn: WorkerCallable, unix_url: str):
    for i in range(multiplex - 1):
        if os.fork() == 0:
            break

    with ExitStack() as es:
        stacks = [
            logging_setup(logger_config),
            circuitbreaker(main_logger='broker'),
        ]

        for stack in stacks:
            es.enter_context(stack)

        logging.getLogger('worker_inst').error(f'Start %s', unix_url)

        # use the callable's type hints in order to serialize and deserialize parameters

        cls_req, cls_res = get_func_types(fn)

        run_server(WorkerInst[cls_req, cls_res], WorkerInst(fn), {DEFAULT_GROUP: unix_url})


class WorkerInst(Generic[RequestType, ResponseType]):
    def __init__(self, fn: WorkerCallable):
        self.idx = os.getpid()
        self.fn = fn
        self.cls_req, self.cls_res = get_func_types(self.fn)

    @rpc(RPCType.Signalling)
    def put(self, payload: Optional[RequestType]):
        ret = self.fn(payload)

        s = service(Worker[self.cls_req, self.cls_res], origin())
        s.bk_done(ret)

    @rpc(exc=True)
    def bk_exc(self, exc: ConnectionAbortedError) -> bool:
        raise TerminationException()

    @regular()
    def announce(self) -> float:
        s = service(Worker[self.cls_req, self.cls_res], origin())
        s.bk_announce(self.idx)

        return 10.

    @signal()
    def exit(self):
        raise TerminationException()


@dataclass(frozen=False)
class WorkerLoad:
    occupied: int = 0
    capacity: int = 0


BACKEND = 'backend'

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


class Worker(Generic[RequestType, ResponseType]):
    def __init__(
            self,
            cls_req: Type[RequestType], cls_res: Type[ResponseType],
            conf: ClusterConf,
            broker_addr: Origin,
            fn: WorkerCallable[RequestType, ResponseType],
            url_metrics: Optional[str] = None,
            par_conf: WorkerConf = WorkerConf(),
    ):
        self.brid: Optional[RPCKey] = None
        self.par_conf = par_conf

        self.load = WorkerLoad()

        self.cls_req = cls_req
        self.cls_res = cls_res

        self.conf = conf
        self.url_broker = broker_addr
        self.url_metrics = url_metrics

        # todo: we've got a set of jobs assigned by the broker
        # todo: each job is assigned to a different process and a thread

        self.fn = fn

        # self.workers: Dict[str, Optional[JobID]] = {
        # todo: a worker A with PID 1 dies. Then we create a new worker B and it gets the same PID
        # todo: we still don't know that A is dead but already know about B
        # todo: we then realise A had died, so we try to evict it, instead killing B
        self.worker_addrs: Dict[str, int] = {}
        # self.worker_pids: Dict[int, Popen] = {}

        # todo: open issues: how do we terminate threads or processes, or both?
        # todo: e.g. a process can be terminated through the API,
        # todo: a thread can't be

        self.jobs: Dict[RPCKey, RequestType] = {}
        self.jobs_res: Dict[RPCKey, ResponseType] = {}
        self.jobs_workers: Dict[RPCKey, str] = {}
        self.workers_jobs: Dict[str, RPCKey] = {}
        self.workers_free: KeyedQueue[str, str, str] = KeyedQueue()

        self.jobs_pending_done: SchedKeyQueue = SchedKey.queue()

    def _start_worker_inst(self):
        popen(worker_inst, logging_config(), self.par_conf.threads, self.fn, origin(BACKEND))

    @regular()
    def ep(self) -> Optional[float]:
        for y in range(self.par_conf.processes):
            self._start_worker_inst()
        return None

    def _free(self, jid: RPCKey) -> str:
        wid = self.jobs_workers[jid]
        del self.jobs_workers[jid]
        del self.workers_jobs[wid]
        return wid

    def _evict(self, jid: RPCKey):
        del self.jobs[jid]
        wid = self._free(jid)

        if wid in self.workers_free:
            del self.workers_free[wid]

        wpid = self.worker_addrs[wid]
        del self.worker_addrs[wid]

        self.load.capacity -= 1
        self.load.occupied -= 1

        # todo for now, we do not replace the workers

        os.kill(wpid, SIGKILL)

    def _evict_all(self):
        for jid in list(self.jobs.keys()):
            self._evict(jid)

    def _brid_new(self, brid: RPCKey):
        should_reset = self.brid is not None and self.brid != brid
        self.brid = brid

        if should_reset:
            self._evict_all()

    @rpc(RPCType.Signalling)
    def assign(self, brid: RPCKey, jid: RPCKey, jreq: RequestType):
        # todo broker may try to assign a job, but timeout, and the worker may assign it still
        # todo in such a scenario the whole worker is trashed on the broker side
        # todo it will be evicted and all of it's jobs would be taken away.

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

        sb = service(Broker[self.cls_req, self.cls_res], self.url_broker)
        sb.bk_done(self.brid, jid, False)

        if jid not in self.jobs:
            # [w-1] resignation notice may appear after worker had successfully finished the job
            # [w-1] in such a scenario, a broker must report resignation as a failure by checking it's finish log
            trc('unk').error('%s', jid)
            return

        if jid in self.jobs_res:
            trc('done').error('%s', jid)
            return

        self._evict(jid)
        self._push_done(jid)
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

        if jid in self.jobs:
            del self.jobs[jid]
            del self.jobs_res[jid]

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

    @regular(initial=None)
    def push_announce(self) -> float:
        # announce and register
        # we regularly announce to be sure that we're in sync with the brid

        # todo the issue is here of the fact that broker may be down for a while
        # todo we may try to re-register
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
        trc('1').error('%s %s', host, reason)

        if host in self.workers_jobs:
            jid = self.workers_jobs[host]
            self._evict(jid)

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
    def bk_announce(self, wpid: int):
        sdr = sender()

        if wpid in self.worker_addrs:
            return

        trc('1').debug('%s', sdr)

        self.worker_addrs[sdr] = wpid
        self.workers_free.push(sdr)
        self.load.capacity += 1

        if self.load.capacity == self.par_conf.processes * self.par_conf.threads:
            self._started()

    def _started(self):
        reset(self.startup_timeout, None)
        reset(self.push_announce, 0)

        if self.url_metrics:
            reset(self.push_metrics, 0)

    @regular(initial=3.)
    def startup_timeout(self):
        assert False, 'Could not reach the required capacity '

    @signal()
    def exit(self):
        while len(self.workers_free):
            wid = self.workers_free.pop()
            wpid = self.worker_addrs[wid]
            del self.worker_addrs[wid]

            os.kill(wpid, SIGKILL)

        self._evict_all()

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
class JobState:
    req: RequestType
    created: datetime
    attempts: int = 0
    started: Optional[datetime] = None
    res: Optional[ResponseType] = None
    finished: Optional[datetime] = None


class BrokerResult(Generic[ResponseType]):
    @rpc(RPCType.Signalling)
    def finished(self, jid: RPCKey, job: ResponseType):
        logging.getLogger('finished').warning('unused %s', job)

        s = service(Broker[RequestType, ResponseType], sender(), group=DEFAULT_GROUP)
        s: Broker[RequestType, ResponseType]
        s.flush_ack(jid)


class BrokerEntry(Generic[ResponseType]):
    @rpc(RPCType.Repliable)
    def assign(self, id_: RPCKey, pars: RequestType) -> bool:
        pass


class Broker(Generic[RequestType, ResponseType], BrokerEntry[ResponseType]):
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

        self.jobs: Dict[RPCKey, JobState] = {}
        self.jobs_pending: KeyedQueue[RPCKey, RPCKey, RPCKey] = KeyedQueue()

        self.jobs_workers: Dict[RPCKey, Origin] = {}
        self.workers_jobs: Dict[Origin, List[RPCKey]] = {}

        self.jobs_pending_assign: SchedKeyQueue = SchedKey.queue()
        self.jobs_pending_resign: SchedKeyQueue = SchedKey.queue()
        self.jobs_pending_flush: SchedKeyQueue = SchedKey.queue()

        self.jobs_cancel: Set[RPCKey] = set()

    def _job_new(self, jid: RPCKey, jreq: RequestType):
        trc('0').debug('%s %s', jid, jreq)

        self.jobs[jid] = JobState(jreq, time_now())
        self.jobs_pending.push(jid)

        reset(self.push_push_assign, 0)

    def _job_clean(self, jid: RPCKey) -> str:
        wid = self.jobs_workers[jid]
        del self.jobs_workers[jid]
        self.workers_jobs[wid].remove(jid)
        return wid

    def _job_resign(self, jid: RPCKey):
        self._job_clean(jid)

        if jid in self.jobs_cancel:
            self._job_clean(jid)
            del self.jobs[jid]
            self.jobs_cancel.remove(jid)
            if jid in self.jobs_pending:
                del self.jobs_pending[jid]
            if jid in self.jobs_pending_assign:
                del self.jobs_pending_assign[jid]
            if jid in self.jobs_pending_resign:
                del self.jobs_pending_resign[jid]
        else:
            self.jobs_pending.push(jid)

            self.jobs[jid].started = None
            reset(self.push_push_assign, 0)

    def _job_done(self, jid: RPCKey, jres: ResponseType):
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

    def _brid_check(self, brid: RPCKey):
        wid = sender()
        if self.workers_brids.get(wid) != brid:
            trc('brid', depth=2).error('%s', brid)
            return True
        return False

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_assign(self, brid: RPCKey, jid: RPCKey, ok: bool):
        if self._brid_check(brid):
            return

        if jid not in self.jobs:
            trc('1').error('%s', jid)
            return

        if jid in self.jobs_pending_assign:
            del self.jobs_pending_assign[jid]

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

        if ok:
            self._job_done(jid, res)
        else:
            self._job_resign(jid)

    @rpc()
    def metrics(self) -> BrokerMetric:
        return BrokerMetric(
            len(self.workers),
            len(self.jobs_pending),
            len(self.jobs),
            len(self.jobs_workers),
        )

    @regular(initial=None)
    def push_metrics(self) -> float:
        s = service(MetricCollector, self.url_metrics)
        s.metrics(self.metrics())

        return self.conf.metrics

    @rpc(RPCType.Repliable)
    def assign(self, jid: RPCKey, jreq: RequestType) -> bool:
        if len(self.jobs_pending) + len(self.jobs_workers) + len(
                self.jobs_pending_flush) >= self.par_conf.backlog + self.par_conf.result_backlog:
            return False

        if jid not in self.jobs:
            self._job_new(jid, jreq)

        return True

    @rpc(RPCType.Repliable)
    def cancel(self, jid: RPCKey) -> bool:
        if jid not in self.jobs:
            return False

        if jid in self.workers:
            self._job_resign(jid)
            self.jobs_cancel.add(jid)
        else:
            self._job_clean(jid)

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

    def _worker_new(self, wbrid: RPCKey, wid: Origin, load: WorkerLoad):
        trc().debug('%s %s %s', wbrid, wid, load)

        self.workers[wid] = WorkerState(self.conf.max_pings, WorkerLoad(occupied=0, capacity=load.capacity))
        self.workers_jobs[wid] = []
        self.workers_brids[wid] = wbrid

        reset(self.push_push_assign, 0)

    def _worker_lost(self, wid: Origin):
        trc().debug('%s', wid)

        for jid in self.workers_jobs[wid]:
            self._job_resign(jid)

        del self.workers[wid]

        reset(self.push_push_assign, 0)

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
        wst.pings_remaining = self.conf.max_pings

        if flag_changed:
            reset(self.push_push_assign, 0)

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
