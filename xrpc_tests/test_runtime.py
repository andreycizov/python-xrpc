import shutil
from tempfile import mkdtemp
from time import sleep

from xrpc.client import client_transport, ClientConfig
from xrpc.dsl import regular, rpc, RPCType, DEFAULT_GROUP
from xrpc.error import TerminationException
from xrpc.popen import wait_all
from xrpc.runtime import reset
from xrpc_tests.mp.abstract import server_main_new, ProcessHelperCase


class ResetActor:
    @rpc(RPCType.Repliable)
    def stop(self) -> bool:
        reset(self.stopper, 0)
        return True

    @rpc(exc=True)
    def exc(self, _: BaseException) -> bool:
        return True

    @regular(initial=None)
    def stopper(self):
        raise TerminationException()


class ResetStrActor:
    @rpc(RPCType.Repliable)
    def stop(self) -> bool:
        reset('stopper', 0)
        return True

    @rpc(exc=True)
    def exc(self, _: BaseException) -> bool:
        return True

    @regular(initial=None)
    def stopper(self):
        raise TerminationException()


class TestRuntime(ProcessHelperCase):
    def test_reset(self):
        td = mkdtemp()

        try:
            ua = f'unix://{td}/sock.sock'
            a = self.ps.popen(server_main_new, lambda: (ResetActor, ResetActor()), {DEFAULT_GROUP: ua + '#bind'})

            with self.ps.timer(10.) as tr:
                while True:
                    tr.sleep(0.5)

                    try:
                        with client_transport(ResetActor, ua, ClientConfig(horz=False), origin=ua) as acli:
                            self.assertEqual(True, acli.stop())
                            break
                    except FileNotFoundError:
                        pass

            self.assertEqual(wait_all(a), [0])
        finally:
            shutil.rmtree(td)

    def test_reset_str(self):
        td = mkdtemp()

        try:
            ua = f'unix://{td}/sock.sock'
            a = self.ps.popen(server_main_new, lambda: (ResetStrActor, ResetStrActor()), {DEFAULT_GROUP: ua + '#bind'})

            with self.ps.timer(10.) as tr:
                while True:
                    tr.sleep(0.5)

                    try:
                        with client_transport(ResetStrActor, ua, ClientConfig(horz=False), origin=ua) as acli:
                            self.assertEqual(True, acli.stop())
                            break
                    except FileNotFoundError:
                        pass

            self.assertEqual(wait_all(a), [0])
        finally:
            shutil.rmtree(td)
