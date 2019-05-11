import logging
import os
from _signal import SIGTERM, SIGKILL
from datetime import datetime, timedelta
from time import sleep
from typing import Dict, Optional, Tuple

from collections import defaultdict
from dataclasses import dataclass

from xrpc.client import ClientConfig, client_transport
from xrpc.dsl import RPCType, rpc, regular, signal, DEFAULT_GROUP
from xrpc.error import TerminationException
from xrpc.impl.broker import Broker, Worker, ClusterConf, MetricCollector, NodeMetric, WorkerMetric, BACKEND, \
    WorkerConf, BrokerMetric, BrokerResult, BrokerConf
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
    def finished_a(self, jid: RPCKey, jres: Response):
        self.exit_in = time_now() + timedelta(seconds=0.5)
        trc('0').warning('Finished %s', self.exit_in)
        self._ack(jid)

    @rpc(RPCType.Durable)
    def finished_b(self, jid: RPCKey, jres: Response):
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


@dataclass
class BPResultsReceiver(BrokerResult[Response]):
    def __init__(self):
        self.enabled = False
        self.hit_count = 0
        self.results: Dict[RPCKey, Response] = {}

    @rpc(RPCType.Repliable)
    def enable(self):
        self.enabled = True

    @rpc(RPCType.Repliable)
    def count(self) -> Tuple[int, int]:
        return len(self.results), self.hit_count

    @rpc(RPCType.Signalling)
    def finished(self, jid: RPCKey, jres: Response):
        if self.enabled:
            self._finished_ack(jid)
            self.results[jid] = jres

        self.hit_count += 1

    @signal()
    def exit(self):
        raise TerminationException()


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
    if not os.path.exists(req.where):
        sleep(999999)

    return Response(99)


def worker_function_raise(req: Request) -> Response:
    if not os.path.exists(req.where):
        raise ValueError()

    return Response(99)


def worker_function_retry(req: Request) -> Response:
    while not os.path.exists(req.where):
        sleep(0.03)
    return Response(req.val_req + 8)


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

    def _worker(self, w_addr, *args, **kwargs):
        return self.ps.popen(
            server_main_new,
            lambda: (
                Worker[Request, Response],
                Worker(
                    Request, Response,
                    *args, **kwargs
                )
            ),
            {
                DEFAULT_GROUP: w_addr,
                BACKEND: 'unix://#bind'
            },
        )

    def _broker(self, ub_front, ub_back, **kwargs):
        return self.ps.popen(
            server_main_new,
            lambda: (
                Broker[Request, Response],
                Broker(Request, Response, **kwargs)
            ),
            {
                DEFAULT_GROUP: ub_front,
                BACKEND: ub_back,
            }
        )

    def _test_worker_startup(self, par_conf: WorkerConf):
        conf = ClusterConf()

        ub = 'udp://127.0.0.1:5678'
        uw = 'udp://127.0.0.1:5789'

        pidw = self._worker(
            uw, conf, ub, worker_function, None, par_conf=par_conf
        )

        with self.ps.timer(5.) as tr, client_transport(
                Worker[Request, Response], uw, ClientConfig(horz=False)) as br:
            x: Optional[WorkerMetric] = None
            slept = 0.
            while True:
                x = br.metrics()

                if x is None or x.workers_free < par_conf.processes * par_conf.threads:
                    slept = tr.sleep(0.3)
                else:
                    trc('slept1').error('%s', slept)
                    break

        pidw.send_signal(SIGTERM)

        self.assertEqual(wait_all(pidw, max_wait=2), [0])

    def test_worker_startup__default(self):
        self._test_worker_startup(WorkerConf())

    def test_worker_startup__1_32(self):
        self._test_worker_startup(WorkerConf(1, 32))

    def test_worker_startup__1_64(self):
        self._test_worker_startup(WorkerConf(1, 64))

    def test_worker_participation(self):
        conf = ClusterConf()

        ub_front = 'udp://127.0.0.1:5678'
        ub_back = 'udp://127.0.0.1:5679'
        uw1 = 'udp://127.0.0.1'
        uw2 = 'udp://127.0.0.1'

        par_conf = WorkerConf(1, 13)

        pidws = []

        for uw in [uw1, uw2]:
            pidw = self._worker(
                uw,
                conf=conf,
                url_broker=ub_back,
                fn=worker_function,
                par_conf=par_conf,
            )
            pidws.append(pidw)

        pidb = self._broker(
            ub_front,
            ub_back,
            conf=conf,
        )

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
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

        brpo = self._broker(
            ub_front,
            ub_back,
            conf=conf,
            url_results=res_addr
        )
        wrpo = self._worker(
            w_addr,
            conf=conf,
            url_broker=ub_back,
            fn=worker_function
        )
        repo = self.ps.popen(
            server_main_new,
            lambda: (ResultsReceiver, ResultsReceiver()),
            {
                DEFAULT_GROUP: res_addr,
            }
        )

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
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

        a = self._broker(
            ub_front,
            ub_back,
            conf=conf,
            url_results=res_addr
        )
        b = self._worker(
            w_addr, conf=conf, url_broker=ub_back, fn=worker_function
        )
        c = self.ps.popen(
            server_main_new,
            lambda: (ResultsReceiver, ResultsReceiver()),
            {
                DEFAULT_GROUP: res_addr,
            }
        )

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
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

        a = self._broker(ub_front, ub_back, conf=conf, url_results=res_addr)
        b = self._worker(w_addr, conf, ub_back, fn=worker_function, url_metrics=None, par_conf=WorkerConf(1, 2))
        c = self.ps.popen(
            server_main_new,
            lambda: (ResultsReceiver, ResultsReceiver()),
            {
                DEFAULT_GROUP: res_addr,
            }
        )

        logging.getLogger(__name__).warning('A')

        with self.ps.timer(5.) as tr, client_transport(
                Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
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

            a = self._broker(ub_front, ub_back, conf=conf, url_metrics=metric_addr)
            b = self._worker(
                w_addr,
                conf,
                ub_back,
                fn=worker_function_sleeper,
                url_metrics=metric_addr
            )
            c = self.ps.popen(server_main, run_metrics, metric_addr)

            self.step()

            with client_transport(Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
                br.assign(RPCKey.new(), Request(5))

            self.step()

            x = {}
            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(horz=False, timeout_total=3)) as mr:
                while len(x) <= 1:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    tr.sleep(0.3)

            self.step()

            with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                           ClientConfig(horz=False, timeout_total=3)) as mr:
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
        dtemp = self.dtemp

        conf = ClusterConf(metrics=0.5)
        ub_front = 'udp://127.0.0.1:7483'
        ub_back = 'udp://127.0.0.1:7484'
        metric_addr = 'udp://127.0.0.1:8845'
        w_addr = 'udp://127.0.0.1:5648'
        w_addr_2 = 'udp://127.0.0.1'

        a = self._broker(
            ub_front,
            ub_back,
            conf=conf,
        url_metrics=metric_addr)

        b = self._worker(
            w_addr,
            conf,
            ub_back,
            fn=worker_function_resign,
            url_metrics=metric_addr
        )

        c = self.ps.popen(server_main, run_metrics, metric_addr)

        key = RPCKey.new()

        self.step()

        ftemp = os.path.join(dtemp, 'toucher')

        with client_transport(Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
            br: Broker[Request, Response]

            br.assign(key, Request(5, where=ftemp))

        x = {}

        self.step()

        with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                       ClientConfig(horz=False, timeout_total=3)) as mr:
            while len(x) <= 1:
                x = mr.metric_counts()
                trc('1').debug(x)
                tr.sleep(0.3)

        self.step()

        with self.ps.timer(20) as tr:
            tr.sleep(0.35)

        self.step()

        with client_transport(Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
            br: Broker[Request, Response]

            br.resign(key)

        self.step()

        with open(ftemp, 'w+') as _:
            pass

        with self.ps.timer(20) as tr, client_transport(MetricReceiver, metric_addr,
                                                       ClientConfig(horz=False, timeout_total=3)) as mr:
            while len(x) >= 2:
                x = mr.metric_counts()
                logging.getLogger(__name__).debug(x)
                tr.sleep(0.3)

        with client_transport(Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
            self.assertEqual(0, br.metrics().jobs)

        b.send_signal(SIGTERM)
        c.send_signal(SIGTERM)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, c, max_wait=5), [0, 0, 0])

    def test_worker_patchup(self):
        dtemp = self.dtemp

        conf = ClusterConf(
            heartbeat=0.5,
            metrics=0.5,
        )

        par_conf = WorkerConf()

        ub_front = 'udp://127.0.0.1:7483'
        ub_back = 'udp://127.0.0.1:7484'
        w_addr = 'udp://127.0.0.1:5648'
        w_addr_2 = 'udp://127.0.0.1'

        a = self._broker(
            ub_front,
            ub_back,
            conf=conf,
        )

        b = self._worker(
            w_addr,
            conf,
            ub_back,
            fn=worker_function_raise,
        )

        key = RPCKey.new()

        self.step('assign')

        ftemp = os.path.join(dtemp, 'toucher')

        with client_transport(Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
            br: Broker[Request, Response]

            br.assign(key, Request(5, where=ftemp))

        self.step('wait_until_assigned')

        def get_metrics():
            with client_transport(Broker[Request, Response], ub_front,
                                                           ClientConfig(horz=False, timeout_total=3)) as mr:
                mr: Broker[Request, Response]
                r = mr.metrics()
                trc().error('%s', r)
                return r

        x = get_metrics()

        with self.ps.timer(20) as tr:
            while x.jobs < 1:
                x = get_metrics()
                tr.sleep(0.01)

        self.step('job_assigned')

        with self.ps.timer(20) as tr:
            tr.sleep(0.35)

        self.step('create_ftemp')

        with open(ftemp, 'w+') as _:
            pass

        self.step('wait_done')

        with self.ps.timer(20) as tr:
            while x.jobs > 0 and x.capacity < par_conf.total:
                x = get_metrics()
                tr.sleep(0.01)

        with client_transport(Broker[Request, Response], ub_front, ClientConfig(horz=False)) as br:
            self.assertEqual(0, br.metrics().jobs)

        self.step('wait_term')

        b.send_signal(SIGTERM)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, max_wait=5), [0, 0])

    def test_backpressure(self):
        conf = ClusterConf(heartbeat=0.5, max_pings=5)
        backlog = 4

        br_conf = BrokerConf(
            backlog=backlog,
            flush_backlog=0,
        )

        wr_conf = WorkerConf(1, backlog)

        jobs_total = 10
        jobs_cancel = 2

        ub_front = 'udp://127.0.0.1:54546'
        ub_back = 'udp://127.0.0.1:54540'
        ur_front = 'udp://127.0.0.1:54547'
        uw_front = 'udp://127.0.0.1:54548'

        a = self._broker(ub_front, ub_back, conf=conf, url_results=ur_front, par_conf=br_conf)
        b = self._worker(uw_front, conf=conf, url_broker=ub_back, fn=worker_function_retry, url_metrics=None,
                         par_conf=wr_conf)
        c = self.ps.popen(
            server_main_new,
            lambda: (BPResultsReceiver, BPResultsReceiver()),
            {
                DEFAULT_GROUP: ur_front
            }
        )

        tots = 0
        failed = 0

        self.step('assign')

        flock = os.path.join(self.dtemp, 'lock')

        ok_keys = []

        with client_transport(Broker[Request, Response], dest=ub_front, horz=False) as br:
            br: Broker[Request, Response]
            with self.ps.timer(2) as tr:
                for i in range(jobs_total):
                    key = RPCKey.new()
                    res = br.assign(key, Request(i * 1000, where=flock))
                    tots += 1
                    if not res:
                        failed += 1
                    else:
                        ok_keys.append(key)
                    tr.sleep(0.01)

        jobs_to_fail = tots - br_conf.backlog
        jobs_to_ok = br_conf.backlog

        self.assertEqual(jobs_to_fail, failed)

        self.step('check_cancel')

        assert jobs_cancel < jobs_to_ok

        with client_transport(Broker[Request, Response], dest=ub_front, horz=False) as br:
            br: Broker[Request, Response]
            with self.ps.timer(2) as tr:
                for _, key in zip(range(jobs_cancel), ok_keys):
                    self.assertEqual(True, br.cancel(key))

        jobs_to_ok -= jobs_cancel

        self.step('check_backpressure')

        with open(flock, 'w+'):
            pass

        self.step('job_barrier')

        x = 0

        with client_transport(Broker[Request, Response], dest=ub_front, horz=False) as br:
            br: Broker[Request, Response]
            with self.ps.timer(2) as tr:
                x = 0
                while x < jobs_to_ok:
                    x = br.metrics().flushing
                    tr.sleep(0.01)

        self.assertEqual(jobs_to_ok, x)

        self.step('res_check')

        hc_now = None
        hc_after = None

        with client_transport(BPResultsReceiver, dest=ur_front, horz=False) as rr:
            rr: BPResultsReceiver
            _, hc_now = rr.count()

            self.assertLess(0, hc_now)

            self.step('res_barrier')

            rr.enable()

            r = 0
            with self.ps.timer(2) as tr:
                while r < jobs_to_ok:
                    r, hc_after = rr.count()
                tr.sleep(0.01)

            self.assertEqual(jobs_to_ok, r)

            self.assertLess(hc_after, hc_now + 5)

        b.send_signal(SIGTERM)
        c.send_signal(SIGTERM)
        a.send_signal(SIGTERM)

        self.assertEqual(wait_all(a, b, c, max_wait=5), [0, 0, 0])

    def test_sending_to_unknown_host(self):
        metric_addr = 'udp://1asdasjdklasjdasd:8845'

        with client_transport(MetricReceiver, metric_addr) as mr:
            mr.metrics(WorkerMetric(None, 0, 0, 0, 0, ''))

# todo Test Backpressure
# todo Test Cancellations
# todo Test Random kills of the workers/subworkers
# todo Test Tandem/massive
# todo Test Broker switch w or w/out state preservation
