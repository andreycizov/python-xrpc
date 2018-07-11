import logging
import os
import unittest
from _signal import SIGTERM, SIGKILL
from collections import defaultdict, deque
from time import sleep
from typing import Dict

from dataclasses import dataclass

from xrpc.client import ClientConfig
from xrpc.dsl import RPCType, rpc, regular, signal
from xrpc.error import TerminationException
from xrpc.impl.broker import Broker, Worker, BrokerConf, BrokerResult, MetricCollector, NodeMetric, WorkerMetric
from xrpc.logging import logging_setup, LoggerSetup, LL, logging_config
from xrpc.popen import popen, PopenStack, cov
from xrpc.server import run_server
from xrpc_tests.examples.test_generic import build_ts
from xrpc_tests.examples.test_pong import wait_items


@dataclass(eq=True, frozen=True)
class Request:
    val: int = 5


@dataclass
class Response:
    val: int = 6


class ResultsReceiver(BrokerResult[Response]):
    @rpc(RPCType.Durable)
    def finished(self, job: Response):
        logging.debug('Finished')
        raise TerminationException('Finished')

    @signal()
    def exit(self):
        return True

    @regular()
    def reg(self) -> float:
        logging.debug('Still waiting')
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
        return True

    @rpc()
    def metric_counts(self) -> Dict[str, int]:
        r = {k.__name__: len([x for x in v if not isinstance(x, WorkerMetric) or x.running_since is not None]) for
             k, v in self.metrics_by_type.items()}
        return {k: v for k, v in r.items() if v > 0}


def run_broker(lc, conf, broker_addr, res_addr=None, url_metrics=None):
    with logging_setup(lc), cov():
        try:
            rpc = Broker[Request, Response](
                Request,
                Response,
                conf,
                res_addr,
                url_metrics,
            )
            run_server(Broker[Request, Response], rpc, [broker_addr])
        except KeyboardInterrupt:
            return
        except:
            logging.exception('')
            raise


def worker_function(req: Request) -> Response:
    return Response(req.val + 1)


def worker_function_sleeper(req: Request) -> Response:
    sleep(3)
    return Response(req.val + 99)


def run_worker(lc, conf, addr, broker_addr, url_metrics=None, worker_fun=None):
    with logging_setup(lc), cov():
        try:
            if worker_fun is None:
                worker_fun = worker_function
            rpc = Worker(Request, Response, conf, broker_addr, worker_fun, url_metrics)

            run_server(Worker[Request, Response], rpc, [addr])
        except KeyboardInterrupt:
            return
        except:
            logging.exception('')
            raise


def run_results(lc, addr):
    with logging_setup(lc), cov():
        try:
            rpc = ResultsReceiver()

            run_server(ResultsReceiver, rpc, [addr])
        except KeyboardInterrupt:
            return
        except:
            logging.exception('')
            raise


def run_metrics(lc, addr):
    with logging_setup(lc), cov():
        try:
            rpc = MetricReceiver()

            run_server(MetricReceiver, rpc, [addr])
        except KeyboardInterrupt:
            return
        except:
            logging.exception('')
            raise


def build_logging():
    return logging_setup(LoggerSetup(LL(None, logging.DEBUG), [], ['stream:///stderr']))


class TestBroker(unittest.TestCase):
    def test_workflow(self):
        conf = BrokerConf()
        broker_addr = 'udp://127.0.0.1:7483'
        res_addr = 'udp://127.0.0.1:7485'
        w_addr = 'udp://127.0.0.1'

        with build_logging(), PopenStack(timeout=10.) as s:
            a = popen(run_broker, logging_config(), conf, broker_addr, res_addr, None)
            s.add(a)
            b = popen(run_worker, logging_config(), conf, w_addr, broker_addr)
            s.add(b)
            c = popen(run_results, logging_config(), res_addr)
            s.add(c)

            with build_ts(Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
                x = 0
                while x == 0:
                    x, = br.stats()
                    sleep(1)

            with build_ts(Broker[Request, Response], broker_addr) as br:
                br.assign(Request(1))

            wait_items([c])

            b.send_signal(SIGTERM)

            with build_ts(Broker[Request, Response], broker_addr) as br:
                x = 1
                while x > 0:
                    x, = br.stats()
                    sleep(1)

            a.send_signal(SIGTERM)

    def test_kill_child(self):
        conf = BrokerConf(heartbeat=0.5, max_pings=10)
        broker_addr = 'udp://127.0.0.1:54546'
        res_addr = 'udp://127.0.0.1:54547'
        w_addr = 'udp://127.0.0.1:54548'

        with build_logging(), PopenStack(timeout=10.) as s:
            a = popen(run_broker, logging_config(), conf, broker_addr, res_addr)
            s.add(a)
            # todo: add a worker that is killed mid-execution
            b = popen(run_worker, logging_config(), conf, w_addr, broker_addr)
            s.add(b)
            c = popen(run_results, logging_config(), res_addr)
            s.add(c)

            with build_ts(Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
                x = 0
                while x == 0:
                    x, = br.stats()
                    sleep(1)

            with build_ts(Worker[Request, Response], w_addr) as w:

                # w: Worker
                os.kill(w.pid(), SIGKILL)

            with build_ts(Broker[Request, Response], broker_addr) as br:
                x = 1
                while x > 0:
                    x, = br.stats()
                    sleep(1)
            c.send_signal(SIGTERM)
            a.send_signal(SIGTERM)

    def test_metrics(self):
        conf = BrokerConf(metrics=0.5)
        broker_addr = 'udp://127.0.0.1:7483'
        metric_addr = 'udp://127.0.0.1:8845'
        w_addr = 'udp://127.0.0.1'
        w_addr_2 = 'udp://127.0.0.1'

        with build_logging(), PopenStack(timeout=10.) as s:
            a = popen(run_broker, logging_config(), conf, broker_addr, res_addr=None, url_metrics=metric_addr)
            s.add(a)
            b = popen(run_worker, logging_config(), conf, w_addr, broker_addr,
                      worker_fun=worker_function_sleeper,
                      url_metrics=metric_addr)
            s.add(b)

            c = popen(run_metrics, logging_config(), metric_addr)

            s.add(c)

            with build_ts(Broker[Request, Response], broker_addr, ClientConfig(ignore_horizon=True)) as br:
                br.assign(Request(5))

            x = {}
            while len(x) <= 1:
                with build_ts(MetricReceiver, metric_addr, ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    sleep(0.3)

            while len(x) >= 2:
                with build_ts(MetricReceiver, metric_addr, ClientConfig(ignore_horizon=True, timeout_total=3)) as mr:
                    x = mr.metric_counts()
                    logging.getLogger(__name__).debug(x)
                    sleep(0.3)

            b.send_signal(SIGTERM)
            c.send_signal(SIGTERM)
            a.send_signal(SIGTERM)

    def test_sending_to_unknown_host(self):
        metric_addr = 'udp://1asdasjdklasjdasd:8845'

        with build_ts(MetricReceiver, metric_addr) as mr:
            mr.metrics(WorkerMetric(None))
