import logging
import os
from _signal import SIGTERM, SIGKILL
from collections import defaultdict

from dataclasses import dataclass
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, Optional

from xrpc.client import ClientConfig, client_transport
from xrpc.dsl import RPCType, rpc, regular, signal, DEFAULT_GROUP
from xrpc.error import TerminationException
from xrpc.impl.broker import Broker, Worker, BrokerConf, MetricCollector, NodeMetric, WorkerMetric, BACKEND
from xrpc.logging import LoggerSetup, LL
from xrpc.popen import wait_all
from xrpc.util import time_now
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main, DEFAULT_LEVEL, server_main_new


@dataclass(eq=True, frozen=True)
class Request:
    val_req: int = 5
    when: Optional[datetime] = None


@dataclass
class Response:
    val_res: int = 6


@dataclass
class ResultsReceiver:
    exit_in: Optional[datetime] = None

    @rpc(RPCType.Durable)
    def finished_a(self, job: Response):
        self.exit_in = time_now() + timedelta(seconds=0.5)
        logging.debug('Finished %s', self.exit_in)

    @rpc(RPCType.Durable)
    def finished_b(self, job: Response):
        logging.debug('Finished %s', self.exit_in)
        raise TerminationException('Finished')

    @signal()
    def exit(self):
        raise TerminationException()

    @regular()
    def reg(self) -> float:
        logging.debug('Still waiting')

        if self.exit_in and time_now() > self.exit_in:
            raise TerminationException('Finished')

        return 1.


class MetricReceiver(MetricCollector):
    def __init__(self) -> None:
        self.metrics_by_type = defaultdict(list)

    @rpc(RPCType.Signalling)
    def metrics(self, metric: NodeMetric):
        ms = self.metrics_by_type[metric.__class__]

        ms.append(metric)

        if len(ms) > 5:
            ms = ms[-5:]
        self.metrics_by_type[metric.__class__] = ms

    @signal()
    def exit(self):
        raise TerminationException()

    @rpc()
    def metric_counts(self) -> Dict[str, int]:
        r = {k.__name__: len([x for x in v if not isinstance(x, WorkerMetric) or x.running_since is not None]) for
             k, v in self.metrics_by_type.items()}
        return {k: v for k, v in r.items() if v > 0}


def worker_function(req: Request) -> Response:
    return Response(req.val_req + 1)


def worker_function_sleeper(req: Request) -> Response:
    sleep(3)
    return Response(req.val_req + 99)


def worker_function_resign(req: Request) -> Response:
    logging.getLogger('worker_function_resign').warning('%s', req)

    if time_now() < req.when:
        sleep(999999)

    return Response(99)


def run_broker(broker_addr, conf, res_addr=None, url_metrics=None):
    rpc = Broker[Request, Response](
        Request,
        Response,
        conf,
        res_addr,
        url_metrics,
    )
    return Broker[Request, Response], rpc


def run_worker(conf, broker_addr, url_metrics=None, worker_fun=None):
    if worker_fun is None:
        worker_fun = worker_function
    rpc = Worker(Request, Response, conf, broker_addr, worker_fun, url_metrics)
    return Worker[Request, Response], rpc


def run_results(addr):
    return ResultsReceiver, ResultsReceiver()


def run_metrics(addr):
    return MetricReceiver, MetricReceiver()


class TestBroker(ProcessHelperCase):
    def _get_ls(self) -> LoggerSetup:
        return LoggerSetup(LL(None, DEFAULT_LEVEL), [
            LL('xrpc.generic', logging.ERROR),
            LL('xrpc.serde', logging.ERROR),
            LL('xrpc.tr.n.r', logging.INFO),
            LL('xrpc.tr.n.r.e', logging.DEBUG),
            LL('xrpc.tr.n.s', logging.INFO),
            LL('xrpc.loop', logging.INFO),
        ], ['stream:///stderr'])

    def test_workflow_a(self):
        conf = BrokerConf()
        broker_addr = 'udp://127.0.0.1:7483'
        res_addr = 'udp://127.0.0.1:7485?finished=finished_a'
        w_addr = 'udp://127.0.0.1'

        brpo = self.ps.popen(server_main, run_broker, broker_addr, conf, res_addr, None)
        wrpo = self.ps.popen(server_main_new, run_worker, {
            DEFAULT_GROUP: w_addr,
            BACKEND: 'unix://#bind'
        }, conf, broker_addr)
        repo = self.ps.popen(server_main, run_results, res_addr)

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
            x = 0
            while x == 0:
                x = br.metrics().workers
                tr.sleep(1.)

        with client_transport(Broker[Request, Response], broker_addr) as br:
            br.assign(Request(1))

        self.ps.wait([repo])

        wrpo.send_signal(SIGTERM)

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], broker_addr) as br:
            x = 1
            while x > 0:
                x = br.metrics().workers
                tr.sleep(1)

        brpo.send_signal(SIGTERM)

    def test_workflow_b(self):
        conf = BrokerConf()
        broker_addr = 'udp://127.0.0.1:7483'
        res_addr = 'udp://127.0.0.1:7485?finished=finished_b'
        w_addr = 'udp://127.0.0.1'

        a = self.ps.popen(server_main, run_broker, broker_addr, conf, res_addr, None)
        b = self.ps.popen(server_main_new, run_worker, {
            DEFAULT_GROUP: w_addr,
            BACKEND: 'unix://#bind'
        }, conf, broker_addr)
        c = self.ps.popen(server_main, run_results, res_addr)

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
            x = 0
            while x == 0:
                x = br.metrics().workers
                tr.sleep(1.)

        with client_transport(Broker[Request, Response], broker_addr) as br:
            br.assign(Request(1))

        self.ps.wait([c])

        b.send_signal(SIGTERM)

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], broker_addr) as br:
            x = 1
            while x > 0:
                x = br.metrics().workers
                tr.sleep(1)

        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, c, max_wait=1), [0, 0, 0])

    def test_kill_child(self):
        conf = BrokerConf(heartbeat=0.5, max_pings=10)
        broker_addr = 'udp://127.0.0.1:54546'
        res_addr = 'udp://127.0.0.1:54547'
        w_addr = 'udp://127.0.0.1:54548'

        a = self.ps.popen(server_main, run_broker, broker_addr, conf, res_addr, None)
        b = self.ps.popen(server_main_new, run_worker, {
            DEFAULT_GROUP: w_addr,
            BACKEND: 'unix://#bind'
        }, conf, broker_addr)
        c = self.ps.popen(run_results, res_addr)

        logging.getLogger(__name__).warning('A')

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
            x = 0
            while x == 0:
                x = br.metrics().workers
                tr.sleep(1)

        logging.getLogger(__name__).warning('B')

        with client_transport(Worker[Request, Response], w_addr) as w:
            # w: Worker
            os.kill(w.pid(), SIGKILL)

        logging.getLogger(__name__).warning('C')

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], broker_addr) as br:
            x = 1
            while x > 0:
                x = br.metrics().workers
                tr.sleep(1)

        logging.getLogger(__name__).warning('D')

        c.send_signal(SIGTERM)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, c, max_wait=1), [0, 0, 0])

    def test_metrics(self):
        try:
            conf = BrokerConf(metrics=0.5)
            broker_addr = 'udp://127.0.0.1:7483'
            metric_addr = 'udp://127.0.0.1:8845'
            w_addr = 'udp://127.0.0.1'
            w_addr_2 = 'udp://127.0.0.1'

            self.step()

            a = self.ps.popen(server_main, run_broker, broker_addr, conf, res_addr=None, url_metrics=metric_addr)
            b = self.ps.popen(server_main_new, run_worker, {
                DEFAULT_GROUP: w_addr,
                BACKEND: 'unix://#bind'
            }, conf, broker_addr,
                              worker_fun=worker_function_sleeper,
                              url_metrics=metric_addr)
            c = self.ps.popen(server_main, run_metrics, metric_addr)

            self.step()

            with client_transport(Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
                br.assign(Request(5))

            self.step()

            x = {}
            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                while len(x) <= 1:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    tr.sleep(0.3)

            self.step()

            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                while len(x) >= 2:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    tr.sleep(0.3)

            self.step()

            b.send_signal(SIGTERM)
            c.send_signal(SIGTERM)
            a.send_signal(SIGTERM)

            self.assertEqual(wait_all(a, b, c, max_wait=4), [0, 0, 0])
        except:
            logging.getLogger(__name__).exception('Now exiting')
            raise

    def test_resign(self):
        # todo test double assignment
        try:
            conf = BrokerConf(metrics=0.5)
            broker_addr = 'udp://127.0.0.1:7483'
            metric_addr = 'udp://127.0.0.1:8845'
            w_addr = 'udp://127.0.0.1:5648'
            w_addr_2 = 'udp://127.0.0.1'

            a = self.ps.popen(server_main, run_broker, broker_addr, conf, res_addr=None, url_metrics=metric_addr)
            b = self.ps.popen(server_main_new, run_worker, {
                DEFAULT_GROUP: w_addr,
                BACKEND: 'unix://#bind'
            }, conf, broker_addr,
                              worker_fun=worker_function_resign,
                              url_metrics=metric_addr)
            c = self.ps.popen(server_main, run_metrics, metric_addr)

            with client_transport(Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
                br.assign(Request(5, when=time_now() + timedelta(seconds=3)))

            x = {}

            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                while len(x) <= 1:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    tr.sleep(0.3)

            with self.ps.timer(20) as tr:
                tr.sleep(3)

            with client_transport(Worker[Request, Response], w_addr, ClientConfig(ignore_horizon=True)) as w:
                w.resign()

            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                while len(x) >= 2:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    tr.sleep(0.3)

            with client_transport(Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
                self.assertEqual(0, br.metrics().jobs)

            b.send_signal(SIGTERM)
            c.send_signal(SIGTERM)
            a.send_signal(SIGTERM)

            self.assertEqual(wait_all(a, b, c, max_wait=5), [0, 0, 0])
        except:
            logging.getLogger(__name__).exception('Now exiting')
            raise

    def test_sending_to_unknown_host(self):
        metric_addr = 'udp://1asdasjdklasjdasd:8845'

        with client_transport(MetricReceiver, metric_addr) as mr:
            mr.metrics(WorkerMetric(None, 'zex', 'udp://localhost'))
