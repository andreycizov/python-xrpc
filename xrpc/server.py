import contextlib
import logging
import signal
from collections import deque
from datetime import timedelta, datetime
from functools import partial
from typing import List, Dict, Optional, Callable, Tuple, Any, Deque, NamedTuple

from xrpc.util import signal_context, time_now
from xrpc.dict import RPCLogDict, ObjectDict
from xrpc.error import HorizonPassedError, InvalidFingerprintError, InternalError, TerminationException
from xrpc.net import RPCPacket, RPCReply, RPCPacketType
from xrpc.dsl import RPCType
from xrpc.runtime import ExecutionContext
from xrpc.service import ServiceDefn
from xrpc.transform import get_regular, get_rpc, get_signal, get_startup, get_socketio
from xrpc.transport import Transport, RPCTransportStack, RPCPacketRaw, select_helper


def check_callable_args(fn: Callable, args, kwargs):
    # we only check their existence, not their types
    pass


PrevSignals = Dict[Tuple[str, int], Callable]
ExecutionContextCallable = Callable[[], ExecutionContext]


class SpecialException(Exception):
    pass


def signal_handler_wrapper(code: int, frame, key: str, conf, fn, fn_ec: ExecutionContextCallable,
                           prev_signals: PrevSignals, running_instance=None):
    should_pass = fn_ec().exec(fn, running_instance)
    logging.getLogger('sig.wrapper').debug('Code=%s Frame=%s ShouldPass=%s', code, frame, should_pass)
    if should_pass:
        prev_signals[(key, code)](code, frame)


def signal_handler_default(code: int, frame, state: ObjectDict, prev_signals: PrevSignals):
    logging.getLogger('sig.default').warning('Code=%s Frame=%s', code, frame)

    if code in prev_signals:
        fn = prev_signals[code]
        if callable(fn):
            logging.getLogger('sig.default.prev').warning('Run previous handler %s', fn)
            fn(code, frame)

    state.is_running = False
    raise SpecialException()


@contextlib.contextmanager
def special_handler():
    try:
        yield
    except (KeyboardInterrupt, SpecialException):
        raise KeyboardInterrupt from None


def run_server(cls, running_instance, bind_urls: List[str], horizon_each=60.):
    # todo: enable mapping between RPC groups and the bind urls
    # todo: - the issue with this is the back-comm. ? which bound url sends the packets

    regulars = get_regular(cls)
    rpcs = get_rpc(cls)
    signals = get_signal(cls)
    startups = get_startup(cls)
    socketios = get_socketio(cls)

    # we can't yet build an RPC from the class without pushing the envelope.

    service_defn = ServiceDefn.from_obj(cls)

    waiting_for_regulars: Dict[str, datetime] = {
        k: time_now() + timedelta(seconds=x.conf.initial) for k, x in regulars.items()
    }

    log_dict = RPCLogDict(time_now())

    state = ObjectDict(is_running=True)

    with contextlib.ExitStack() as stack:
        prev_signals2: PrevSignals = {}

        codes = (signal.SIGTERM, signal.SIGINT)

        prev_hdlrs = stack.enter_context(
            signal_context(
                signals=codes,
                handler=partial(signal_handler_default, state=state, prev_signals=prev_signals2)
            )
        )

        for code, prev_hdlr in zip(codes, prev_hdlrs):
            prev_signals2[code] = prev_hdlr
            logging.getLogger('signal.bind.default').debug('Code=%d', code)


        # build transports

        transports = []
        for url, t in ((url, Transport.from_url(url)) for url in bind_urls):
            stack.enter_context(t)

            logging.getLogger('transport.bind').debug('%s', url)
            transports.append(t)

        assert len(transports), 'No transports specified'

        # build signals

        prev_signals: PrevSignals = {}

        def exec_ctx_fn():
            return ExecutionContext(transport_stack, None)

        for k, (conf, fn) in signals.items():
            x = signal_context(
                signals=conf.codes,
                handler=partial(
                    signal_handler_wrapper, key=k, conf=conf, fn=fn,
                    prev_signals=prev_signals,
                    fn_ec=exec_ctx_fn,
                    running_instance=running_instance
                )
            )
            prev_hdlrs = stack.enter_context(x)
            # x is now prev signal handler

            for code, prev_hdlr in zip(conf.codes, prev_hdlrs):
                prev_signals[(k, code)] = prev_hdlr
                logging.getLogger('signal.bind').debug('Code=%d Name=%s', code, k)

        # build transport stack

        class ExecutionQueueEntry(NamedTuple):
            p: RPCPacket
            fb: Any
            raw_packet: RPCPacketRaw
            args: Any
            kwargs: Any

        execution_queue: Deque[ExecutionQueueEntry] = deque()

        def recv_packet(raw_packet: RPCPacketRaw):
            p = raw_packet.packet

            rp: Optional[RPCPacket] = None

            try:
                if p.key in log_dict:
                    rep = log_dict[p.key]

                    if rpcs[p.name].conf.type != RPCType.Signalling:
                        rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.ok.value, rep)
                else:
                    if p.name not in rpcs:
                        logging.error(f'Could not find name %s %s', p.name, set(rpcs.keys()))
                        raise InvalidFingerprintError()

                    fa, fb = service_defn.rpcs_serde[p.name]

                    try:
                        args, kwargs = service_defn.serde.deserialize(fa, p.payload)
                    except Exception as e:
                        # could not deserialize the payload correctly
                        logging.exception(f'Failed to deserialize packet from {raw_packet.addr}')
                        raise InvalidFingerprintError()

                    # todo we have deserialized the packet

                    execution_queue.append(
                        ExecutionQueueEntry(
                            p,
                            fb, raw_packet, args, kwargs
                        )
                    )

                    if rpcs[p.name].conf.type == RPCType.Durable:
                        rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.ok.value, None)
                        log_dict[p.key] = None
                    elif rpcs[p.name].conf.type == RPCType.Signalling:
                        log_dict[p.key] = None
            except InvalidFingerprintError:
                rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.fingerprint.value, None)
            except HorizonPassedError as e:
                rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.horizon.value,
                               service_defn.serde.serialize(datetime, e.when))

            if rp:
                transport_stack.send(RPCPacketRaw(raw_packet.addr, rp))

        transport_stack = RPCTransportStack(transports)

        transport_stack.push(lambda x: x.packet.type == RPCPacketType.Req, recv_packet)

        # build startups

        for k, (conf, fn) in startups.items():
            pass

        # build socketios

        socketio_states = {}

        for k, (conf, fn) in socketios.items():
            socketio_states[k] = fn(running_instance)

        # end of builds

        stack.enter_context(special_handler())

        while state.is_running:
            step_time = time_now()

            max_poll_regulars = {k: (x - step_time).total_seconds() for k, x in waiting_for_regulars.items()}
            max_poll_horizon = ((log_dict.horizon + timedelta(seconds=horizon_each) * 2) - step_time).total_seconds()

            items = list(max_poll_regulars.values()) + [max_poll_horizon]

            max_poll = min(items)

            fds_transport = [x.fd for x in transport_stack.transports]
            fds_socketios = [v for _, v in sorted(socketio_states.items())]

            flags = select_helper(fds_transport + fds_socketios, max_wait=max(0., max_poll))

            flags_transport = flags[:len(fds_transport)]
            flags_socketios = flags[len(fds_transport):]

            should_receive = any(flags_transport)
            should_regular = any(x <= 0 or regulars[k].conf.tick for k, x in max_poll_regulars.items())
            should_horizon = max_poll_horizon <= 0
            should_socketio = any(flags_socketios)

            assert len(transport_stack.stack) == 1, transport_stack.stack

            if should_receive:
                transport_stack.recv()

                # todo process the queue

                while len(execution_queue):
                    eq = execution_queue.popleft()

                    p = eq.p
                    fb = eq.fb
                    raw_packet = eq.raw_packet
                    args = eq.args
                    kwargs = eq.kwargs

                    rp: Optional[RPCPacket] = None

                    try:
                        has_returned = False

                        def replier(ret: Any):
                            assert rpcs[p.name].conf.type == RPCType.Repliable, p.name

                            nonlocal has_returned
                            assert not has_returned

                            ret_payload = service_defn.serde.serialize(fb, ret)

                            log_dict[p.key] = ret_payload

                            rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.ok.value, ret_payload)

                            transport_stack.send(RPCPacketRaw(raw_packet.addr, rp))
                            has_returned = True

                        ctx = ExecutionContext(transport_stack, raw_packet.addr, p.key, ret=replier)
                        rpc_fn = rpcs[p.name].fn
                        try:
                            ret = ctx.exec(rpc_fn, running_instance, *args, **kwargs)
                        except TerminationException:
                            raise
                        except Exception as e:
                            logging.getLogger(__name__).exception(
                                'While receiving the payload [%s][%s][%s]', p.name, args, kwargs)
                            raise InternalError(str(e))

                        if rpcs[p.name].conf.type == RPCType.Repliable:
                            if not has_returned:
                                replier(ret)
                            elif has_returned and ret is None:
                                pass
                            elif has_returned and ret is not None:
                                raise ValueError()
                            else:
                                raise NotImplementedError()
                    except InternalError as e:
                        rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.internal.value, e.reason)
                        raise
                    except TerminationException as e:
                        state.is_running = False
                        raise SpecialException()
                        continue
                    finally:
                        if rp and rpcs[p.name].conf.type == RPCType.Repliable:
                            transport_stack.send(RPCPacketRaw(raw_packet.addr, rp))

            if should_regular:
                for name, regular, callable in [(k, *regulars[k]) for k, v in max_poll_regulars.items() if v <= 0.]:
                    try:
                        x: float = ExecutionContext(transport_stack).exec(callable, running_instance)
                    except TerminationException:
                        state.is_running = False
                        raise SpecialException()
                    else:
                        waiting_for_regulars[name] = time_now() + timedelta(seconds=x)

            if should_horizon:
                log_dict.set_horizon(step_time - timedelta(seconds=horizon_each))

            if should_socketio:
                for is_ready, key in zip(flags_socketios, [k for k, _ in sorted(socketio_states.items())]):
                    if is_ready:
                        callable = socketios[key][1]

                        r = ExecutionContext(transport_stack).exec(callable, running_instance)

                        assert r is not None, key

                        socketio_states[key] = r
