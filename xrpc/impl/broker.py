import json
import logging
import os
from inspect import getfullargspec
from subprocess import TimeoutExpired

import shutil
import socket
import tempfile
from signal import SIGTERM
from argparse import ArgumentParser
from collections import deque
from datetime import datetime
from itertools import count
from time import sleep
from typing import NamedTuple, Callable, Optional, Dict, Deque, TypeVar, Generic, Type, Tuple

from xrpc.logging import logging_config, LoggerSetup, logging_setup, circuitbreaker
from xrpc.popen import popen
from xrpc.abstract import MutableInt
from xrpc.client import ClientConfig
from xrpc.const import SERVER_SERDE_INST
from xrpc.dsl import rpc, RPCType, regular, socketio, signal
from xrpc.error import HorizonPassedError, TimeoutError
from xrpc.runtime import service, sender
from xrpc.serde.abstract import SerdeSet, SerdeStruct
from xrpc.serde.types import pair_spec, build_types, ARGS_RET
from xrpc.transport import recvfrom_helper, Packet, Origin
from xrpc.util import time_now, signal_context


class BrokerConf(NamedTuple):
    heartbeat: float = 5.
    max_pings: int = 5
    metrics: float = 10.

    @classmethod
    def from_parser(cls, **kwargs):
        return BrokerConf(
            kwargs['heartbeat'],
            kwargs['max_pings'],
            kwargs['metrics'],
        )

    @classmethod
    def add_parser(cls, parser: ArgumentParser):
        parser.add_argument(
            '--hearbeat',
            dest='heartbeat',
            type=float,
            default=5.
        )

        parser.add_argument(
            '--pings',
            dest='max_pings',
            type=int,
            default=5
        )

        parser.add_argument(
            '--metrics',
            dest='metrics',
            type=float,
            default=5
        )


class JobParams(NamedTuple):
    payload: bytes


class JobReturn(NamedTuple):
    payload: bytes


RequestType = TypeVar('RequestType')
ResponseType = TypeVar('ResponseType')

WorkerCallable = Callable[[RequestType], ResponseType]


def build_worker_serde():
    a = SerdeSet.walk(SERVER_SERDE_INST, JobReturn)
    b = SerdeSet.walk(SERVER_SERDE_INST, JobParams)

    s = a.merge(b)
    return s.struct(SERVER_SERDE_INST)


# WorkerSerde = build_worker_serde()

def get_func_types(fn: WorkerCallable) -> Tuple[Type[RequestType], Type[ResponseType]]:
    spec = getfullargspec(fn)

    annot = build_types(spec, False, allow_missing=True)
    arg_name, _ = next(pair_spec(spec, False, None))

    return annot[arg_name], annot[ARGS_RET]


def worker_inst(logger_config: LoggerSetup, fn: WorkerCallable, path: str):
    def sig_handler(code, frame):
        logging.getLogger(__name__).error(f'Received {code}')
        raise KeyboardInterrupt('')

    with logging_setup(logger_config), circuitbreaker(), signal_context(handler=sig_handler):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)  # UDP

        logging.getLogger('worker_inst').debug('Binding to %s', path)
        sock.bind(path)
        sock.listen(1)

        connection, origin = sock.accept()
        logging.getLogger('worker_inst.accept').debug('%s', origin)

        # use the callable's type hints in order to serialize and deserialize parameters

        cls_req, cls_res = get_func_types(fn)

        serde = build_serde(cls_req, cls_res)

        try:
            for x in recvfrom_helper(connection, logger_name='worker_inst.net.trace.raw'):
                logging.getLogger('worker_inst.net.trace.raw.i').debug('[%d] %s %s', len(x.data), x.addr,
                                                                       x.data)
                jp: cls_req = serde.deserialize(cls_req, json.loads(x.data))

                ret = fn(jp)

                op = Packet(None, json.dumps(serde.serialize(cls_res, ret)).encode())

                logging.getLogger('worker_inst.net.trace.raw.o').debug('[%d] %s %s', len(op.data), op.addr,
                                                                       op.data)

                connection.send(op.pack())
        except KeyboardInterrupt:
            logging.getLogger('worker_inst').debug('Mildly inconvenient exit')


def build_serde(req: Type[ResponseType], res: Type[ResponseType]) -> SerdeStruct:
    a = SerdeSet.walk(SERVER_SERDE_INST, req)
    b = SerdeSet.walk(SERVER_SERDE_INST, res)

    return a.merge(b).struct(SERVER_SERDE_INST)


class Worker(Generic[RequestType, ResponseType]):
    def __init__(
            self,
            cls_req: Type[RequestType], cls_res: Type[ResponseType],
            conf: BrokerConf, broker_addr: Origin, fn: WorkerCallable[RequestType, ResponseType]):
        self.cls_req = cls_req
        self.cls_res = cls_res

        self.serde = build_serde(self.cls_req, self.cls_res)

        self.conf = conf
        self.broker_addr = broker_addr
        self.assigned: Optional[JobParams] = None

        self.dir = None
        self.dir = tempfile.mkdtemp()

        unix_url = os.path.join(self.dir, 'unix.sock')
        self.unix_url = unix_url

        self.inst = popen(worker_inst, logging_config(), fn, unix_url)

        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.settimeout(1)
        self.socket.setblocking(0)

        sleep(0.3)

        for attempt in count():
            try:
                self.socket.connect(unix_url)
                break
            except:
                if attempt >= 5:
                    raise ValueError('Could not instantiate a worker')
                logging.exception('At %d', attempt)
                sleep(1)

    @rpc()
    def get_assigned(self) -> Optional[ResponseType]:
        return self.assigned

    @rpc(RPCType.Durable)
    def assign(self, pars: RequestType):
        if self.assigned is not None:
            raise ValueError('Double assignment')

        op = Packet(self.unix_url, json.dumps(self.serde.serialize(self.cls_req, pars)).encode())

        logging.getLogger('net.trace.raw.o').debug('[%d] %s %s', len(op.data), op.addr, op.data)

        self.socket.send(op.pack())
        self.assigned = pars

    @regular()
    def heartbeat(self) -> float:
        s = service(Broker[self.cls_req, self.cls_res], self.broker_addr)
        s.remind()

        return self.conf.heartbeat

    @socketio()
    def bg(self):
        for x in recvfrom_helper(self.socket):
            logging.getLogger('net.trace.raw.i').debug('[%d] %s %s', len(x.data), x.addr, x.data)

            ret = self.serde.deserialize(self.cls_res, json.loads(x.data))

            logging.getLogger('bg').debug('Returned %s', x)

            self.assigned = None

            s = service(Broker[self.cls_req, self.cls_res], self.broker_addr)

            try:
                s.done(ret)
            except HorizonPassedError:
                logging.getLogger('bg').exception('Seems like the broker had been killed while I was working')

        return self.socket

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
        if self.dir:
            shutil.rmtree(self.dir)
        return True


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


class Broker(Generic[RequestType, ResponseType]):
    def __init__(
            self,
            cls_req: Type[RequestType], cls_res: Type[ResponseType],
            conf: BrokerConf, url_results: Optional[str] = None
    ):
        self.cls_req = cls_req
        self.cls_res = cls_res

        self.conf = conf
        self.url_results = url_results

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

            s = service(Worker[self.cls_req, self.cls_res], wrkr, ClientConfig(timeout_total=1.))

            try:
                s.assign(pars)
            except TimeoutError:
                logging.getLogger('jobs_try_assign').error('Timeout %s', wrkr)
                self.jobs_pending.appendleft(pars)
                continue
            else:
                logging.getLogger('jobs_try_assign').debug('%s %s', wrkr, pars)
                self.workers_jobs[wrkr] = pars

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
            logging.getLogger('job_done').warning('Worker is not assigned any jobs %s', w)
            return

        j = self.workers_jobs[w]

        del self.jobs[j]
        del self.workers_jobs[w]

        self.jobs_try_assign()

    @rpc()
    def stats(self) -> Tuple[int]:
        return len(self.workers),

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
            s = service(BrokerResult[self.cls_res], self.url_results)
            s.finished(jr)
        else:
            logging.getLogger('done').error('Return type not used %s', jr)

        # todo 1) keep a log of completed jobs
        # todo 2) reply to sender
        # todo 3) send downstream

        self.worker_done(sender())

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

    @regular()
    def metrics(self) -> float:
        logging.getLogger('metrics').warning('Workers %d', len(self.workers))
        logging.getLogger('metrics').warning('Pending %d', len(self.jobs_pending))
        logging.getLogger('metrics').warning('Jobs %d', len(self.jobs))
        logging.getLogger('metrics').warning('Assigned %d', len(self.workers_jobs))

        return self.conf.metrics

    @signal()
    def exit(self):
        return True
