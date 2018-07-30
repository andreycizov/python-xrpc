import logging
import os
import types
from argparse import ArgumentParser
from contextlib import ExitStack
from datetime import datetime, timedelta
from inspect import getfullargspec, ismethod
from signal import SIGKILL
from typing import Callable, Optional, Dict, Deque, TypeVar, Generic, Type, Tuple, Union, List, Any

from collections import deque
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
from xrpc.runtime import service, sender, origin, reply, reset
from xrpc.serde.types import build_types, ARGS_RET, PairSpec
from xrpc.service import ServiceDefn
from xrpc.trace import trc
from xrpc.transport import Origin, Transport
from xrpc.util import time_now


@dataclass
class BrokerConf(Parsable):
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


WPT = TypeVar('WorkerPacketType')


@dataclass
class WorkerEnvelope(Generic[WPT]):
    payload: Optional[WPT] = None
    has_payload: bool = False


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


@dataclass(frozen=True)
class WorkerSched:
    when: datetime
    key: RPCKey

    @classmethod
    def ord_fn(cls, x: 'WorkerSched'):
        return x.when

    @classmethod
    def key_fn(cls, x: 'WorkerSched'):
        return x.key


class Worker(Generic[RequestType, ResponseType]):
    def __init__(
            self,
            cls_req: Type[RequestType], cls_res: Type[ResponseType],
            conf: BrokerConf,
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
        self.workers_free: Deque[str] = deque()

        self.jobs_pending_done: KeyedQueue[datetime, RPCKey, WorkerSched] = KeyedQueue(ord=WorkerSched.ord_fn,
                                                                                       key=WorkerSched.key_fn)

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

        sb = service(Broker[self.cls_req, self.cls_res], self.url_broker, group=BACKEND)

        if brid != self.brid:
            trc('brid').error('%s != %s %s', brid, self.brid, jid)
            reset(self.push_announce, 0)
            sb.bk_assign_nack(brid, jid)
            return

        if jid in self.jobs:
            # is there any scenario where a job may be assigned to something else ?
            trc('kno').error('%s', jid)
            sb.bk_assign_ack(brid, jid)
            return

        if len(self.workers_free) == 0 or len(self.jobs_res) >= self.conf.pending_max:
            sb.bk_assign_nack(brid, jid)
            return

        nwid = self.workers_free.popleft()

        self.jobs[jid] = jreq
        self.jobs_workers[jid] = nwid
        self.workers_jobs[nwid] = jid

        s = service(WorkerInst[self.cls_req, self.cls_res], nwid, group=BACKEND)
        s.put(jreq)

        self.load.occupied += 1

        sb.bk_assign_ack(brid, jid)

    @rpc(RPCType.Signalling)
    def resign(self, brid: RPCKey, jid: RPCKey, reason: Optional[str] = None):
        sb = service(Broker[self.cls_req, self.cls_res], self.url_broker, group=BACKEND)

        if brid != self.brid:
            trc('brid').error('%s != %s %s', brid, self.brid, jid)
            reset(self.push_announce, 0)
            sb.bk_resign_nack(brid, jid)
            return

        if jid not in self.jobs:
            # [w-1] resignation notice may appear after worker had successfully finished the job
            # [w-1] in such a scenario, a broker must report resignation as a failure by checking it's finish log
            trc('unk').error('%s', jid)
            sb.bk_resign_ack(brid, jid)
            return

        if jid in self.jobs_res:
            trc('done').error('%s', jid)
            sb.bk_resign_nack(brid, jid)
            return

        self._evict(jid)
        sb.bk_resign_ack(brid, jid)
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

    @regular()
    def push_done(self) -> Optional[float]:
        sb = service(Broker[self.cls_req, self.cls_res], self.url_broker, group=BACKEND)

        now = time_now()

        while True:
            val = self.jobs_pending_done.peek()

            if val is None:
                return None

            dsec = (val.when - now).total_seconds()

            if dsec > self.conf.retry_delta * 0.2:
                return dsec

            val = self.jobs_pending_done.pop()

            if val.key in self.jobs:
                if val.key in self.jobs_res:
                    sb.bk_done_ok(val.key, self.jobs_res[val.key])
                else:
                    sb.bk_done_fail(val.key)
                self.jobs_pending_done.push(WorkerSched(now + timedelta(seconds=self.conf.retry_delta), val.key))

    @regular(initial=None)
    def push_announce(self) -> float:
        # announce and register
        # we regularly announce to be sure that we're in sync with the brid

        # todo the issue is here of the fact that broker may be down for a while
        # todo we may try to re-register
        s = service(Broker[self.cls_req, self.cls_res], self.url_broker)
        s.bk_announce(self.brid)

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
    def bk_done(self, jid: RPCKey, res: ResponseType):
        if jid not in self.jobs_workers:
            trc('1').error('%s', jid)
            return

        self.jobs_res[jid] = res
        self.jobs_pending_done.push(WorkerSched(time_now(), jid))
        reset(self.push_done, 0)

        wid = self.jobs_workers[jid]
        del self.jobs_workers[jid]
        del self.workers_jobs[wid]

        self.load.occupied -= 1
        self.workers_free.append(wid)

    @rpc(RPCType.Signalling, group=BACKEND)
    def bk_announce(self, wpid: int):
        sdr = sender()

        if wpid in self.worker_addrs:
            return

        trc('1').debug('%s', sdr)

        self.worker_addrs[sdr] = wpid
        self.workers_free.append(sdr)
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
    @rpc(RPCType.Repliable)
    def finished(self, id_: RPCKey, job: ResponseType) -> bool:
        logging.getLogger('finished').warning('unused %s', job)


class BrokerEntry(Generic[ResponseType]):
    @rpc(RPCType.Repliable)
    def assign(self, id_: RPCKey, pars: RequestType) -> bool:
        pass


class Broker(Generic[RequestType, ResponseType], BrokerEntry[ResponseType]):
    def __init__(
            self,
            cls_req: Type[RequestType],
            cls_res: Type[ResponseType],
            conf: BrokerConf,
            url_results: Optional[str] = None,
            url_metrics: Optional[str] = None
    ):
        self.cls_req = cls_req
        self.cls_res = cls_res

        self.conf = conf
        self.url_results = url_results
        self.url_metrics = url_metrics

        self.workers: Dict[Origin, WorkerState] = {}
        self.workers_brids: Dict[Origin, RPCKey] = {}

        self.jobs: Dict[RPCKey, JobState] = {}
        self.jobs_pending: Deque[RPCKey] = deque()
        self.jobs_pending_flush: Deque[RPCKey] = deque()

        self.jobs_workers: Dict[RPCKey, Origin] = {}
        self.workers_jobs: Dict[Origin, List[RPCKey]] = {}

    def job_new(self, jid: RPCKey, jreq: RequestType):
        logging.getLogger('job_new').debug('%s %s', jid, jreq)

        self.jobs[jid] = JobState(jreq, time_now())
        self.jobs_pending.append(jid)

        self.jobs_try_assign()

    def job_resign(self, jid: RPCKey):
        w_id = self.jobs_workers[jid]

        del self.jobs_workers[jid]

        self.workers_jobs[w_id].remove(jid)

        self.jobs_pending.append(j)

        # todo
        self.jobs[jid].started = None

    def jobs_try_assign(self):
        # we model assignments based on capacity returned by the workers

        free_workers = list(set(self.workers.keys()) - set(self.workers_jobs.keys()))

        while len(free_workers) and len(self.jobs_pending):
            # we need to then find a new worker for the job.

            jid = self.jobs_pending.popleft()

            # todo
            self.jobs[jid].started = time_now()
            self.jobs[jid].attempts += 1
            # todo

            wrkr = free_workers.pop()
            self.workers_jobs[wrkr] = pars

            s = service(Worker[self.cls_req, self.cls_res], wrkr)

            # we could possibly assign these messages in a different way

            logging.getLogger('jobs_try_assign').debug('%s %s', wrkr, pars)

            # if we only allow for assigns to happen once, then we can model the worker state
            # perfectly.

            # otherwise we need an absolute index of capacity values over time

            try:
                s.assign(pars)
            except TimeoutError:
                # todo if we timeout assigning a job to a worker, we evict the worker, rather than evict the job from
                # todo the worker
                logging.getLogger('jobs_try_assign').error('Timeout %s', wrkr)

                if wrkr in self.workers_jobs:
                    j = self.workers_jobs[wrkr]

                    if j == pars:
                        del self.workers_jobs[wrkr]
                        free_workers.append(wrkr)
            else:
                logging.getLogger('jobs_try_assign').debug('%s %s', wrkr, pars)

    def get_metrics(self) -> BrokerMetric:
        return BrokerMetric(
            len(self.workers),
            len(self.jobs_pending),
            len(self.jobs),
            len(self.workers_jobs)
        )

    @rpc()
    def metrics(self) -> BrokerMetric:
        return self.get_metrics()

    @regular()
    def reg_metrics(self) -> float:
        # how to we allow for reflection in the API?

        if self.url_metrics:
            s = service(MetricCollector, self.url_metrics)
            s.metrics(self.get_metrics())

        return self.conf.metrics

    @rpc(RPCType.Repliable)
    def assign(self, jid: RPCKey, jreq: RequestType) -> bool:
        if len(self.jobs_pending) + len(self.jobs_pending_flush) > self.max_jobs_flush:
            return False

        if jreq not in self.jobs:
            self.job_new(jid, jreq)

        return True

    def pending_flush_add(self, jid: RPCKey):
        if self.url_results:
            self.jobs_pending_flush.append(jid)

            # todo how do we enable proper sequential syncing between two Actors?
        else:
            logging.getLogger('pending_flush_add').warning('[1] %s')

    @rpc(RPCType.Durable)
    def resign(self, jid: RPCKey, reason: Optional[str] = None) -> bool:
        if jid not in self.jobs:
            logging.getLogger('resign').error('[1] %s %s', jid, reason)
            return False

        if jid not in self.jobs_workers:
            logging.getLogger('resign').error('[2] %s %s', jid, reason)
            return False

        wid = self.jobs_workers[jid]

        try:
            s = service(Worker[self.cls_req, self.cls_res], wid, ClientConfig(timeout_total=1))
            s.resign(jid, reason)
        except TimeoutError:
            pass

        self.job_resign(jid)

        logging.getLogger('resign').debug('Resigned %s', jid)

        self.jobs_try_assign()

        return True

    def _worker_new(self, wbrid: RPCKey, wid: Origin, load: WorkerLoad):
        logging.getLogger('worker_new').debug('%s %s %s', wbrid, wid, load)

        self.workers[wid] = WorkerState(self.conf.max_pings, WorkerLoad(occupied=0, capacity=load.capacity))
        self.workers_jobs[wid] = []
        self.workers_brids[wid] = wbrid

        # todo if called from bk_announce, the worker may still not have received it's brid yet
        self.jobs_try_assign()

    def _worker_lost(self, wid: Origin):
        logging.getLogger('worker_lost').debug('%s', wid)

        for jid in self.workers_jobs[wid]:
            self.job_resign(jid)

        del self.workers[wid]

        # todo change the jobs_try_assign to a regular, so that we could  pend them after the call
        self.jobs_try_assign()

    def _worker_done(self, wid: Origin, jid: RPCKey):
        if wid not in self.workers:
            logging.getLogger('job_done').error('[1] %s', wid)
            return

        if wid not in self.workers_jobs:
            logging.getLogger('job_done').error('[2] %s %s', wid, jid)
            return

        if jid not in self.workers_jobs[wid]:
            logging.getLogger('job_done').error('[3] %s %s', wid, jid)
            return

        self.workers_jobs[wid].remove(jid)
        del self.jobs_workers[jid]

        # todo model the worker free (maybe)
        # todo how do we model the free workers ?

        self.jobs_try_assign()

    @rpc(RPCType.Durable, group=BACKEND)
    def bk_done(self, jid: RPCKey, jres: ResponseType):
        wid = sender()

        # if we need to sync a set of things, we'd rather put them somewhere and send all of them together

        s = service(Worker[self.cls_req, self.cls_res], wid, ClientConfig(timeout_total=1))
        s.done_ack(jid)

        if wid not in self.workers:
            trc('2').error('%s %s %s', wid, jid, jres)
            return

        # todo if a job is done after the broker had been changed
        # todo but that worker hasn't yet received a new brid

        if jid not in self.jobs:
            trc('1').error('%s %s %s', wid, jid, jres)
            return

        self._worker_done(wid, jid)

        self.pending_flush_add(jid)

        jobj = self.jobs[jid]

        jobj.finished = time_now()
        jobj.res = jres

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
    def bk_announce(self, wbrid: Optional[RPCKey]):
        wid = sender()

        if wid not in self.workers:
            self._worker_new(RPCKey.new(), wid, load)

        brid = self.workers_brids[wid]

        if wbrid != brid:
            s = service(Worker[self.cls_req, self.cls_res], wid, ClientConfig(timeout_total=1))
            s.registered(self.workers_brids[wid])

        # todo only ping workers through announce

        self.workers[wid].pings_remaining = self.conf.max_pings

        wst = self.workers[wid]
        wbrid = self.workers_brids[wid]

        if wbrid != wbrid:
            self._worker_lost(wid)
            # todo: evict all jobs related to the worker
            return False

        wst.load.capacity = load.capacity
        # todo we may need to change the state of the arbitrageur

        return True

        wbrid = self.workers_brids.get(nwid)

        # todo when we fail to assign a job to a worker, we completely evict it

        # todo: a brid is unknown

        if wbrid != wbrid:
            wbrid = RPCKey.new()
            nwid = service(Worker[self.cls_req, self.cls_res], nwid, ClientConfig(timeout_total=1))
            nwid._brid_new(wbrid)

        reply(wbrid)

        if nwid not in self.workers:
            self._worker_new(wbrid, nwid, load)
        else:
            self.workers[nwid].pings_remaining = self.conf.max_pings
            self.workers[nwid].load = load

    @regular()
    def gc(self) -> float:
        for k in list(self.workers.keys()):
            self.workers[k].pings_remaining -= 1

            logging.getLogger('gc').debug('%s %s', k, self.workers[k])

            if self.workers[k].pings_remaining <= 0:
                self._worker_lost(k)

        return self.conf.heartbeat

    @signal()
    def exit(self):
        raise TerminationException()


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
