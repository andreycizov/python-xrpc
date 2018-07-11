import logging
import os
import unittest
from _signal import SIGTERM, SIGKILL
from time import sleep

from dataclasses import dataclass

from xrpc.client import ClientConfig
from xrpc.dsl import RPCType, rpc, regular, signal
from xrpc.error import TerminationException
from xrpc.impl.broker import Broker, Worker, BrokerConf, BrokerResult
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


def run_broker(lc, conf, broker_addr, res_addr):
    with logging_setup(lc), cov():
        try:
            rpc = Broker[Request, Response](
                Request,
                Response,
                conf,
                res_addr
            )
            run_server(Broker[Request, Response], rpc, [broker_addr])
        except KeyboardInterrupt:
            return
        except:
            logging.exception('')
            raise


def worker_function(req: Request) -> Response:
    return Response(req.val + 1)


def run_worker(lc, conf, addr, broker_addr,):
    with logging_setup(lc), cov():
        try:
            rpc = Worker(Request, Response, conf, broker_addr, worker_function)

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


def build_logging():
    return logging_setup(LoggerSetup(LL(None, logging.DEBUG), [], ['stream:///stderr']))


class TestBroker(unittest.TestCase):
    def test_udp(self):
        conf = BrokerConf()
        broker_addr = 'udp://127.0.0.1:7483'
        res_addr = 'udp://127.0.0.1:7485'
        w_addr = 'udp://127.0.0.1'

        with build_logging(), PopenStack(timeout=10.) as s:
            a = popen(run_broker, logging_config(), conf, broker_addr, res_addr)
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

                #w: Worker
                os.kill(w.pid(), SIGKILL)

            with build_ts(Broker[Request, Response], broker_addr) as br:
                x = 1
                while x > 0:
                    x, = br.stats()
                    sleep(1)
            c.send_signal(SIGTERM)
            a.send_signal(SIGTERM)

