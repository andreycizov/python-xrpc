import logging
from collections import deque
from contextlib import ExitStack
from inspect import getfullargspec, ismethod
from itertools import count

import types
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from signal import SIGTERM
from subprocess import TimeoutExpired, Popen
from typing import NamedTuple, Callable, Optional, Dict, Deque, TypeVar, Generic, Type, Tuple, Union

from xrpc.abstract import MutableInt
from xrpc.actor import run_server
from xrpc.cli import Parsable
from xrpc.client import ClientConfig, build_wrapper
from xrpc.const import SERVER_SERDE_INST
from xrpc.dsl import rpc, RPCType, regular, signal, DEFAULT_GROUP
from xrpc.error import HorizonPassedError, TimeoutError, TerminationException
from xrpc.logging import logging_config, LoggerSetup, logging_setup, circuitbreaker, cli_main, logging_parser
from xrpc.loop import EventLoop
from xrpc.popen import popen
from xrpc.runtime import service, sender, origin
from xrpc.serde.abstract import SerdeSet, SerdeStruct
from xrpc.serde.types import build_types, ARGS_RET, PairSpec
from xrpc.service import ServiceDefn
from xrpc.transport import Origin, Transport
from xrpc.util import time_now, signal_context


@dataclass
class BrokerConf(Parsable):
    heartbeat: float = 5.
    max_pings: int = 5
    metrics: float = 10.


@dataclass
class WorkerConf(Parsable):
    processes: int = 1
    threads: int = 1


@dataclass
class WorkerMetric:
    running_since: Optional[datetime]
    payload_str: Optional[str]
    broker_url: str


@dataclass
class BrokerMetric:
    workers: int
    jobs_pending: int
    jobs: int
    assigned: int


NodeMetric = Union[WorkerMetric, BrokerMetric]

RequestType = TypeVar('RequestType')
ResponseType = TypeVar('ResponseType')

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


def worker_inst(logger_config: LoggerSetup, idx: int, fn: WorkerCallable, unix_url: str):
    def sig_handler(code, frame):
        logging.getLogger('worker_inst').error(f'Received {code}')
        raise KeyboardInterrupt('')

    with ExitStack() as es:
        stacks = [
            logging_setup(logger_config),
            circuitbreaker(main_logger='broker'),
            signal_context(handler=sig_handler),
        ]

        for stack in stacks:
            es.enter_context(stack)

        logging.getLogger('worker_inst').error(f'Start %s', unix_url)

        # use the callable's type hints in order to serialize and deserialize parameters

        cls_req, cls_res = get_func_types(fn)

        run_server(WorkerInst[cls_req, cls_res], WorkerInst(idx, fn), {DEFAULT_GROUP: unix_url})


class WorkerInst(Generic[RequestType, ResponseType]):
    def __init__(self, idx: int, fn: WorkerCallable):
        self.idx = idx
        self.fn = fn
        self.cls_req, self.cls_res = get_func_types(self.fn)

    @rpc(RPCType.Durable)
    def put(self, payload: Optional[RequestType]):
        ret = self.fn(payload)

        s = service(Worker[self.cls_req, self.cls_res], origin())
        s.bk_done(ret)

    @regular()
    def announce(self) -> float:
        s = service(Worker[self.cls_req, self.cls_res], origin())
        s.bk_announce(self.idx)

        return 10.

    @signal
    def exit(self):
        raise TerminationException()


def build_serde(*items) -> SerdeStruct:
    ss = None

    for item in items:
        ssi = SerdeSet.walk(SERVER_SERDE_INST, item)

        if ss is None:
            ss = ssi
        else:
            ss = ss.merge(ssi)

    return ss.struct(SERVER_SERDE_INST)


BACKEND = 'backend'


class Worker(Generic[RequestType, ResponseType]):
    def __init__(
            self,
            cls_req: Type[RequestType], cls_res: Type[ResponseType],
            conf: BrokerConf,
            broker_addr: Origin,
            fn: WorkerCallable[RequestType, ResponseType],
            url_metrics: Optional[str] = None
    ):
        self.cls_req = cls_req
        self.cls_res = cls_res

        self.cls_backend_req = WorkerEnvelope[cls_req]
        self.cls_backend_res = WorkerEnvelope[cls_res]

        self.serde = build_serde(self.cls_req, self.cls_res, self.cls_backend_req, self.cls_backend_res)

        self.conf = conf
        self.broker_addr = broker_addr
        self.url_metrics = url_metrics

        # todo: we've got a set of jobs assigned by the broker
        # todo: each job is assigned to a different process and a thread
        self.assigned: Optional[RequestType] = None
        self.running_since: Optional[datetime] = None

        self.fn = fn

        self.idx_ctr = count()
        self.workers: Dict[int, list] = {}
        self.workers_popen: Dict[int, Popen] = {}

    def start_worker_inst(self):
        logging.getLogger(__name__).warning('start_worker_inst')
        idx = next(self.idx_ctr)
        r = popen(worker_inst, logging_config(), idx, self.fn, origin(BACKEND))
        self.workers_popen[idx] = r

    def restart_worker_inst(self):
        for k, v in list(self.workers_popen.items()):
            v.kill()
            del self.workers_popen[k]

            if k in self.workers:
                del self.workers[k]

        self.start_worker_inst()

    @property
    def inst(self):
        return list(self.workers_popen.values())[0]

    @rpc()
    def get_assigned(self) -> Optional[ResponseType]:
        return self.assigned

    @rpc(RPCType.Durable)
    def assign(self, pars: RequestType):
        if self.assigned is not None and pars != self.assigned:
            self.resign('assigned')
            return

        if len(self.workers) == 0:
            self.resign('workers')
            return

        self.assigned = pars
        self.running_since = time_now()

        try:
            s = service(WorkerInst[self.cls_req, self.cls_res], list(self.workers.values())[0][0], group=BACKEND)
            s.put(pars)
        except TimeoutError:
            self.resign()

    @rpc(RPCType.Repliable)
    def pid(self) -> int:
        return int(self.inst.pid)

    @rpc(RPCType.Durable)
    def resign(self, reason: Optional[str] = None):
        if self.assigned is not None and len(self.workers):
            self.restart_worker_inst()

        self.assigned = None
        self.running_since = None

        s = service(Broker[self.cls_req, self.cls_res], self.broker_addr)
        s.resign(reason)

    def is_killed(self) -> bool:
        try:
            self.inst.wait(0)
            logging.getLogger(__name__).warning('Worker process had been killed')
            return True
        except TimeoutExpired:
            return False

    def possibly_killed(self, definitely=False):
        if definitely or self.is_killed():
            logging.getLogger('main').exception('Killed %s %s', definitely, self.is_killed())
            self.exit()
            raise TerminationException()

    @regular()
    def heartbeat(self) -> float:
        self.possibly_killed()

        s = service(Broker[self.cls_req, self.cls_res], self.broker_addr)
        s.remind()

        return self.conf.heartbeat

    @rpc(exc=True, group=BACKEND)
    def backend_exc(self, exc: ConnectionAbortedError) -> bool:
        # one of the workers had disconnected

        host, reason = exc.args
        logging.getLogger(__name__).warning('%s %s', host, reason)

        return True

    @rpc(RPCType.Durable, group=BACKEND)
    def bk_done(self, res: ResponseType):
        self.assigned = None
        self.running_since = None

        s = service(Broker[self.cls_req, self.cls_res], self.broker_addr, group=DEFAULT_GROUP)

        try:
            s.done(res)
        except HorizonPassedError:
            logging.getLogger('bg').exception('Seems like the broker had been killed while I was working')

        self.assigned = None
        self.running_since = None

    @rpc(RPCType.Durable, group=BACKEND)
    def bk_announce(self, idx: int):
        if idx not in self.workers_popen:
            logging.getLogger('bk_announce').warning('Stray worker idx: %s', idx)
            return

        sdr = sender()

        if idx not in self.workers:
            self.workers[idx] = list()

        self.workers[idx].append(sdr)

    @regular()
    def bk_workers_lifetime(self) -> float:
        if len(self.workers_popen) == 0:
            self.start_worker_inst()
        return 5.

    @regular()
    def metrics(self) -> float:
        if self.url_metrics:
            s = service(MetricCollector, self.url_metrics)
            s.metrics(WorkerMetric(
                self.running_since,
                repr(self.assigned) if self.assigned else None,
                self.broker_addr,
            ))
        return self.conf.metrics

    @regular()
    def ep(self) -> Optional[float]:
        self.restart_worker_inst()
        return None

    @signal()
    def exit(self):
        try:
            s = service(Broker[self.cls_req, self.cls_res], self.broker_addr, ClientConfig(timeout_total=1.))
            s.leaving()
        except TimeoutError:
            logging.getLogger('exit').error('Could not contact broker')
        self.inst.send_signal(SIGTERM)
        try:
            self.inst.wait(1)
        except TimeoutExpired:
            logging.getLogger('exit').error('Could stop worker graciously')
            self.inst.kill()

        raise TerminationException()


class WorkerState(NamedTuple):
    pings_remaining: MutableInt


class JobState(NamedTuple):
    created: datetime

    @classmethod
    def new(cls):
        return JobState(created=time_now())


class BrokerResult(Generic[ResponseType]):
    @rpc(RPCType.Durable)
    def finished(self, job: ResponseType):
        logging.getLogger('finished').warning('unused %s', job)


class BrokerEntry(Generic[ResponseType]):
    @rpc(RPCType.Durable)
    def assign(self, pars: RequestType):
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

        self.jobs: Dict[RequestType, JobState] = {}
        self.jobs_pending: Deque[RequestType] = deque()

        self.workers_jobs: Dict[Origin, RequestType] = {}

    def job_new(self, pars: RequestType):
        logging.getLogger('job_new').debug('%s', pars)

        self.jobs[pars] = JobState.new()
        self.jobs_pending.append(pars)

        self.jobs_try_assign()

    def job_resign(self, k: Origin):
        j = self.workers_jobs[k]

        del self.workers_jobs[k]

        self.jobs_pending.appendleft(j)

    def jobs_try_assign(self):
        free_workers = list(set(self.workers.keys()) - set(self.workers_jobs.keys()))

        while len(free_workers) and len(self.jobs_pending):
            pars = self.jobs_pending.popleft()
            wrkr = free_workers.pop()
            self.workers_jobs[wrkr] = pars

            s = service(Worker[self.cls_req, self.cls_res], wrkr, ClientConfig(timeout_total=0.05))

            # we could possibly assign these messages in a different way

            logging.getLogger('jobs_try_assign').debug('%s %s', wrkr, pars)

            try:
                s.assign(pars)
            except TimeoutError:
                # todo if a worker had actually received the payload
                # todo but we do not know of that, then a double assignment will happen
                logging.getLogger('jobs_try_assign').error('Timeout %s', wrkr)

                if wrkr in self.workers_jobs:
                    j = self.workers_jobs[wrkr]

                    if j == pars:
                        del self.workers_jobs[wrkr]
                        free_workers.append(wrkr)
            else:
                logging.getLogger('jobs_try_assign').debug('%s %s', wrkr, pars)

    def worker_new(self, k: Origin):
        logging.getLogger('worker_new').debug('%s', k)

        self.workers[k] = WorkerState(MutableInt(self.conf.max_pings))

        self.jobs_try_assign()

    def worker_lost(self, k: Origin):
        logging.getLogger('worker_lost').debug('%s', k)

        if k in self.workers_jobs:
            self.job_resign(k)

        del self.workers[k]

        self.jobs_try_assign()

    def worker_done(self, w: Origin):
        if w not in self.workers:
            logging.getLogger('job_done').warning('Not registered %s', w)
            return

        if w not in self.workers_jobs:
            logging.getLogger('job_done').warning('Worker is not assigned any jobs %s %s %s', w, self.workers_jobs,
                                                  self.jobs_pending)
            return

        j = self.workers_jobs[w]

        del self.jobs[j]
        del self.workers_jobs[w]

        self.jobs_try_assign()

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

    @rpc(RPCType.Durable)
    def assign(self, pars: RequestType):
        """
        Assign a job to the broker
        :param pars:
        :return:
        """
        if pars not in self.jobs:
            self.job_new(pars)
        else:
            logging.getLogger('assign').warning('Job is still working %s', pars)

    @rpc(RPCType.Durable)
    def done(self, jr: ResponseType):
        if self.url_results:
            try:
                s = service(BrokerResult[self.cls_res], self.url_results, conf=ClientConfig(timeout_total=None))

                s.finished(jr)
            except HorizonPassedError:
                pass
        else:
            logging.getLogger('done').error('Return type not used %s', jr)

        # todo 1) keep a log of completed jobs
        # todo 2) reply to sender
        # todo 3) send downstream

        self.worker_done(sender())

    @rpc(RPCType.Durable)
    def resign(self, reason: Optional[str] = None):
        k = sender()

        if k in self.workers_jobs:
            self.job_resign(k)
            logging.getLogger('resign').debug('Resigned %s %s %s %s %s', reason, k, self.jobs, self.jobs_pending,
                                              self.workers_jobs)
        else:
            logging.getLogger('resign').error('Unknown %s %s', reason, k)

        self.jobs_try_assign()

    @rpc(RPCType.Durable)
    def leaving(self):
        s = sender()

        if s in self.workers:
            self.worker_lost(s)

    @rpc(RPCType.Signalling)
    def remind(self):
        s = sender()

        if s not in self.workers:
            self.worker_new(s)
        else:
            self.workers[s].pings_remaining.set(self.conf.max_pings)

    @regular()
    def gc(self) -> float:
        for k in list(self.workers.keys()):
            self.workers[k].pings_remaining.reduce(1)

            logging.getLogger('gc').debug('%s %s', k, self.workers[k])

            if self.workers[k].pings_remaining <= 0:
                self.worker_lost(k)

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
