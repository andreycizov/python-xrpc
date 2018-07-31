import logging
import os
import shutil
from _signal import SIGTERM, SIGKILL
from datetime import datetime, timedelta
from tempfile import mkdtemp
from time import sleep
from typing import Dict, Optional

from collections import defaultdict
from dataclasses import dataclass

from xrpc.client import ClientConfig, client_transport
from xrpc.dsl import RPCType, rpc, regular, signal, DEFAULT_GROUP
from xrpc.error import TerminationException
from xrpc.impl.broker import Broker, Worker, ClusterConf, MetricCollector, NodeMetric, WorkerMetric, BACKEND, \
    WorkerConf, BrokerMetric
from xrpc.logging import LoggerSetup, LL
from xrpc.net import RPCKey
from xrpc.popen import wait_all
from xrpc.runtime import sender, service
from xrpc.trace import trc
from xrpc.util import time_now
from xrpc_tests.mp.abstract import ProcessHelperCase, server_main, DEFAULT_LEVEL, server_main_new


@dataclass(eq=True, frozen=True)
class Request:
    val_req: int = 5
    where: Optional[str] = None


@dataclass
class Response:
    val_res: int = 6


@dataclass
class ResultsReceiver:
    exit_in: Optional[datetime] = None

    def _ack(self, jid: RPCKey):
        s = service(Broker[Request, Response], sender(), group=DEFAULT_GROUP)
        s: Broker[Request, Response]
        s.flush_ack(jid)

    @rpc(RPCType.Durable)
    def finished_a(self, jid: RPCKey, job: Response):
        self.exit_in = time_now() + timedelta(seconds=0.5)
        trc('0').warning('Finished %s', self.exit_in)
        self._ack(jid)

    @rpc(RPCType.Durable)
    def finished_b(self, jid: RPCKey, job: Response):
        trc('1').warning('Finished %s', self.exit_in)
        self._ack(jid)
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
        r = {k.__name__: len([x for x in v if not isinstance(x, WorkerMetric) or x.jobs > 0]) for
             k, v in self.metrics_by_type.items()}
        return {k: v for k, v in r.items() if v > 0}


def worker_function(req: Request) -> Response:
    return Response(req.val_req + 1)


def worker_function_sleeper(req: Request) -> Response:
    sleep(3)
    return Response(req.val_req + 99)


def worker_function_resign(req: Request) -> Response:
    logging.getLogger('worker_function_resign').warning('%s', req)

    if not os.path.exists(req.where):
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

    def test_worker_startup(self):
        conf = ClusterConf()

        ub = 'udp://127.0.0.1:5678'
        uw = 'udp://127.0.0.1:5789'
        par_conf = WorkerConf()
        pidw = self.ps.popen(
            server_main_new,
            lambda: (
                Worker[Request, Response],
                Worker(Request, Response, conf, ub, worker_function, None, par_conf=par_conf)
            ),
            {
                DEFAULT_GROUP: uw,
                BACKEND: 'unix://#bind'
            },
        )

        with self.ps.timer(5.) as tr, client_transport(
                Worker[Request, Response], uw, ClientConfig(ignore_horizon=True)) as br:
            x: Optional[WorkerMetric] = None
            while x is None or x.workers_free < par_conf.processes * par_conf.threads:
                x = br.metrics()
                tr.sleep(0.3)

        pidw.send_signal(SIGTERM)

        self.assertEqual(wait_all(pidw, max_wait=2), [0])

    def test_worker_participation(self):
        conf = ClusterConf()

        ub_front = 'udp://127.0.0.1:5678'
        ub_back = 'udp://127.0.0.1:5679'
        uw1 = 'udp://127.0.0.1'
        uw2 = 'udp://127.0.0.1'

        par_conf = WorkerConf(1, 13)

        pidws = []

        for uw in [uw1, uw2]:
            pidw = self.ps.popen(
                server_main_new,
                lambda: (
                    Worker[Request, Response],
                    Worker(Request, Response, conf, ub_back, worker_function, None, par_conf=par_conf)
                ),
                {
                    DEFAULT_GROUP: uw,
                    BACKEND: 'unix://#bind'
                },
            )
            pidws.append(pidw)

        pidb = self.ps.popen(
            server_main_new,
            lambda: (
                Broker[Request, Response],
                Broker(Request, Response, conf=conf)
            ),
            {
                DEFAULT_GROUP: ub_front,
                BACKEND: ub_back,
            }
        )

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
            x: BrokerMetric = None
            while x is None or x.workers < 2:
                x = br.metrics()
                tr.sleep(0.05)

                trc('0').debug(x)

        for pidw in pidws:
            pidw.send_signal(SIGTERM)

        pidb.send_signal(SIGTERM)

        self.assertEqual(wait_all(*pidws + [pidb]), [0, 0, 0])

    def test_workflow_a(self):
        conf = ClusterConf()
        ub_front = 'udp://127.0.0.1:54546'
        ub_back = 'udp://127.0.0.1:54540'
        res_addr = 'udp://127.0.0.1:7485?finished=finished_a'
        w_addr = 'udp://127.0.0.1'

        brpo = self.ps.popen(
            server_main_new,
            lambda: (
                Broker[Request, Response],
                Broker(Request, Response, conf=conf, url_results=res_addr)
            ),
            {
                DEFAULT_GROUP: ub_front,
                BACKEND: ub_back,
            }
        )
        wrpo = self.ps.popen(
            server_main_new,
            lambda: (
                Worker[Request, Response],
                Worker(Request, Response, conf, ub_back, worker_function)
            ),
            {
                DEFAULT_GROUP: w_addr,
                BACKEND: 'unix://#bind'
            }
        )
        repo = self.ps.popen(
            server_main_new,
            lambda: (ResultsReceiver, ResultsReceiver()),
            {
                DEFAULT_GROUP: res_addr,
            }
        )

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
            x = 0
            while x == 0:
                x = br.metrics().workers
                tr.sleep(1.)

        with client_transport(Broker[Request, Response], ub_front) as br:
            br: Broker[Request, Request]
            br.assign(RPCKey.new(), Request(1))

        self.ps.wait([repo])

        wrpo.send_signal(SIGTERM)

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], ub_front) as br:
            x = 1
            while x > 0:
                x = br.metrics().workers
                tr.sleep(1)

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], ub_front) as br:
            ms = br.metrics()
            self.assertEqual(0, ms.jobs)
            self.assertEqual(0, ms.jobs_pending)
            self.assertEqual(0, ms.assigned)

        brpo.send_signal(SIGTERM)

    def test_workflow_b(self):
        conf = ClusterConf()
        ub_front = 'udp://127.0.0.1:54546'
        ub_back = 'udp://127.0.0.1:54540'
        res_addr = 'udp://127.0.0.1:7485?finished=finished_b'
        w_addr = 'udp://127.0.0.1'

        a = self.ps.popen(
            server_main_new,
            lambda: (
                Broker[Request, Response],
                Broker(Request, Response, conf=conf, url_results=res_addr)
            ),
            {
                DEFAULT_GROUP: ub_front,
                BACKEND: ub_back,
            }
        )
        b = self.ps.popen(
            server_main_new,
            lambda: (
                Worker[Request, Response],
                Worker(Request, Response, conf, ub_back, worker_function)
            ),
            {
                DEFAULT_GROUP: w_addr,
                BACKEND: 'unix://#bind'
            }
        )
        c = self.ps.popen(
            server_main_new,
            lambda: (ResultsReceiver, ResultsReceiver()),
            {
                DEFAULT_GROUP: res_addr,
            }
        )

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
            x = 0
            while x == 0:
                x = br.metrics().workers
                tr.sleep(1.)

        with client_transport(Broker[Request, Response], ub_front) as br:
            br.assign(RPCKey.new(), Request(1))

        self.ps.wait([c])

        b.send_signal(SIGTERM)

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], ub_front) as br:
            x = 1
            while x > 0:
                x = br.metrics().workers
                tr.sleep(1)

        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, c, max_wait=1), [0, 0, 0])

    def test_kill_worker(self):
        conf = ClusterConf(heartbeat=0.5, max_pings=5)
        ub_front = 'udp://127.0.0.1:54546'
        ub_back = 'udp://127.0.0.1:54540'
        res_addr = 'udp://127.0.0.1:54547'
        w_addr = 'udp://127.0.0.1:54548'

        a = self.ps.popen(
            server_main_new,
            lambda: (
                Broker[Request, Response],
                Broker(Request, Response, conf=conf, url_results=res_addr)
            ),
            {
                DEFAULT_GROUP: ub_front,
                BACKEND: ub_back,
            }
        )

        b = self.ps.popen(
            server_main_new,
            lambda: (
                Worker[Request, Response],
                Worker(
                    Request, Response,
                    conf, ub_back, worker_function, url_metrics=None, par_conf=WorkerConf(1, 2))
            ),
            {
                DEFAULT_GROUP: w_addr,
                BACKEND: 'unix://#bind'
            },
        )
        c = self.ps.popen(
            server_main_new,
            lambda: (ResultsReceiver, ResultsReceiver()),
            {
                DEFAULT_GROUP: res_addr,
            }
        )

        logging.getLogger(__name__).warning('A')

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
            x = 0
            while x == 0:
                x = br.metrics().workers
                trc('1').error('%s', x)
                tr.sleep(1)

        logging.getLogger(__name__).warning('B')

        b.send_signal(SIGKILL)

        logging.getLogger(__name__).warning('C')

        with self.ps.timer(5.) as tr, client_transport(Broker[Request, Response], ub_front) as br:
            br: Broker[Request, Response]

            x = 1
            while x > 0:
                x = br.metrics().workers
                trc('2').error('%s', x)
                tr.sleep(1)

            self.assertEqual(
                BrokerMetric(
                    0,
                    0,
                    0,
                    0,
                ),
                br.metrics()
            )

        logging.getLogger(__name__).warning('D')

        c.send_signal(SIGTERM)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, c, max_wait=1), [0, 0])

    def test_metrics(self):
        try:
            conf = ClusterConf(metrics=0.5)
            ub_front = 'udp://127.0.0.1:7483'
            ub_back = 'udp://127.0.0.1:7484'
            metric_addr = 'udp://127.0.0.1:8845'
            w_addr = 'udp://127.0.0.1'
            w_addr_2 = 'udp://127.0.0.1'

            self.step()

            a = self.ps.popen(
                server_main_new,
                lambda: (
                    Broker[Request, Response],
                    Broker(Request, Response, conf=conf, url_metrics=metric_addr)
                ),
                {
                    DEFAULT_GROUP: ub_front,
                    BACKEND: ub_back,
                }
            )
            b = self.ps.popen(
                server_main_new,
                run_worker,
                {
                    DEFAULT_GROUP: w_addr,
                    BACKEND: 'unix://#bind'
                },
                conf,
                ub_back,
                worker_fun=worker_function_sleeper,
                url_metrics=metric_addr
            )
            c = self.ps.popen(server_main, run_metrics, metric_addr)

            self.step()

            with client_transport(Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
                br.assign(RPCKey.new(), Request(5))

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
        dtemp = mkdtemp()
        try:
            conf = ClusterConf(metrics=0.5)
            ub_front = 'udp://127.0.0.1:7483'
            ub_back = 'udp://127.0.0.1:7484'
            metric_addr = 'udp://127.0.0.1:8845'
            w_addr = 'udp://127.0.0.1:5648'
            w_addr_2 = 'udp://127.0.0.1'

            a = self.ps.popen(
                server_main_new,
                lambda: (
                    Broker[Request, Response],
                    Broker(Request, Response, conf=conf, url_metrics=metric_addr)
                ),
                {
                    DEFAULT_GROUP: ub_front,
                    BACKEND: ub_back,
                }
            )
            b = self.ps.popen(
                server_main_new,
                run_worker,
                {
                    DEFAULT_GROUP: w_addr,
                    BACKEND: 'unix://#bind'
                },
                conf,
                ub_back,
                worker_fun=worker_function_resign,
                url_metrics=metric_addr
            )
            c = self.ps.popen(server_main, run_metrics, metric_addr)

            key = RPCKey.new()

            self.step()

            ftemp = os.path.join(dtemp, 'toucher')

            with client_transport(Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
                br: Broker[Request, Response]

                br.assign(key, Request(5, where=ftemp))

            x = {}

            self.step()

            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                while len(x) <= 1:
                    x = mr.metric_counts()
                    trc('1').debug(x)
                    tr.sleep(0.3)

            self.step()

            with self.ps.timer(20) as tr:
                tr.sleep(0.35)

            self.step()

            with client_transport(Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
                br: Broker[Request, Response]

                br.resign(key)

            self.step()

            with open(ftemp, 'w+') as _:
                pass

            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                while len(x) >= 2:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    tr.sleep(0.3)

            with client_transport(Broker[Request, Response], ub_front, ClientConfig(ignore_horizon=True)) as br:
                self.assertEqual(0, br.metrics().jobs)

            b.send_signal(SIGTERM)
            c.send_signal(SIGTERM)
            a.send_signal(SIGTERM)

            self.assertEqual(wait_all(a, b, c, max_wait=5), [0, 0, 0])
        except:
            logging.getLogger(__name__).exception('Now exiting')
            raise
        finally:
            shutil.rmtree(dtemp)

    def test_sending_to_unknown_host(self):
        metric_addr = 'udp://1asdasjdklasjdasd:8845'

        with client_transport(MetricReceiver, metric_addr) as mr:
            mr.metrics(WorkerMetric(None, 0, 0, 0, 0, ''))
