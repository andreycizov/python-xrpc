from xrpc.actor import actor_create, SignalRunner
from xrpc.dsl import rpc, DEFAULT_GROUP, regular
from xrpc.loop import EventLoop, EventLoopEmpty
from xrpc.runtime import service
from xrpc_tests.mp.abstract import ProcessHelperCase


class TestActor(ProcessHelperCase):
    def test_exc_raised(self):
        class A1:
            @rpc()
            def mopo(self):
                raise ValueError()

        class A2:
            @regular()
            def reg(self) -> float:
                s = service(A1, url_a)
                s.mopo()
                return 2.

        act1 = A1()
        act2 = A2()
        el = EventLoop()
        sr = SignalRunner(el)

        url_a = 'udp://127.0.0.1:23456'

        actor_create(el, sr, A1, act1, [url_a])
        actor_create(el, sr, A2, act1, {
            DEFAULT_GROUP: 'udp://127.0.0.1'
        })

        try:
            el.loop()
        except ValueError:
            self.assertEqual('1', '1')
        except EventLoopEmpty:
            self.assertEqual('1', '0')
