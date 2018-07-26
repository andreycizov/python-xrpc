import contextlib
import logging

from dataclasses import dataclass
from datetime import timedelta, datetime
from functools import partial
from itertools import count
from typing import List, Dict, Optional, Callable, Any, ContextManager

from xrpc.dict import RPCLogDict
from xrpc.dsl import RPCType, DEFAULT_GROUP
from xrpc.error import HorizonPassedError, InvalidFingerprintError, InternalError, TerminationException
from xrpc.loop import EventLoop, ELTransportRef, ELPktEntry, ELWaitEntry, ELPollEntry, EventLoopEmpty, _log_called_from
from xrpc.net import RPCPacket, RPCReply, RPCPacketType
from xrpc.runtime import ExecutionContext
from xrpc.serde.abstract import SerdeStruct
from xrpc.service import SocketIOEntrySet, RegularEntrySet, RPCEntrySet, SignalEntrySet
from xrpc.trace import log_tr_act
from xrpc.transport import Transport, RPCPacketRaw, Transports
from xrpc.util import signal_context, time_now


class Terminating:
    def terminate(self):
        pass


class TerminatingHandler:
    actor: 'Actor'

    @contextlib.contextmanager
    def exc_ctx(self, reg_def):
        try:
            yield
        #except EventLoopEmpty:
        #    raise
        except TerminationException:
            self.actor.terminate('te1_' + repr(reg_def))
        except BaseException:
            _log_called_from(logging.getLogger(f'{__name__}.{self.__class__.__name__}'), 'While %s', reg_def)
            self.actor.terminate('be' + repr(reg_def))

    def exception_handler(self, exc: BaseException):
        if isinstance(exc, TerminationException):
            self.actor.terminate('te2_' + repr(exc))

        return False


class RegularRunner(Terminating, TerminatingHandler):
    def __init__(
            self,
            actor: 'Actor',
            el: EventLoop,
            regulars: RegularEntrySet,
    ):
        # if we integrate max_waits into the RPCTransportStack

        self.actor = actor

        self.wait = el.wait_add(ELWaitEntry(self.max_wait, self.timeout, self.exception_handler))

        self.regulars = regulars

        self.states_regulars: Dict[str, Optional[datetime]] = {
            k: time_now() + timedelta(seconds=x.conf.initial) for k, x in self.regulars.items()
        }

        # if we make sure that max_wait is called for every execution of select_
        # then we know if sleeping is available

    def terminate(self):
        self.wait.remove()

    @property
    def max_poll_regulars(self):
        step_time = time_now()

        max_poll_regulars = {k: (x - step_time).total_seconds() for k, x in self.states_regulars.items()
                             if x is not None}

        return max_poll_regulars

    def max_wait(self) -> Optional[float]:
        items = list(self.max_poll_regulars.values())
        return min(items) if len(items) else None

    def timeout(self):
        # a regular does not know about the transports available to it!

        max_poll_regulars = self.max_poll_regulars

        for name, reg_def in ((k, self.regulars[k]) for k, v in max_poll_regulars.items() if v is not None and v <= 0.):
            with self.exc_ctx(reg_def):
                self.states_regulars[name] = None
                x: float = self.actor.ctx().exec('reg', reg_def.fn)
                self.states_regulars[name] = time_now() + timedelta(seconds=x)


class SocketIORunner(Terminating, TerminatingHandler):
    def __init__(
            self,
            actor: 'Actor',
            ts: EventLoop,
            sios: SocketIOEntrySet,
    ):
        self.actor = actor
        # these guys need to be able to provide the context in which we may change our sio mappings
        self.socketios = sios

        self.chans = Transports()

        self.chan = ts = ts.transport_add(self.chans)
        ts.push_raw(ELPollEntry(self.poll, self.exception_handler))

        for k, sio in self.socketios.items():
            with self.exc_ctx(sio):
                ret = self._assert_tran(sio.fn(None))
                self.chans[k] = ret

    def _assert_tran(self, tran):
        assert isinstance(tran, Transport), tran
        return tran

    def terminate(self):
        self.chan.remove()

    def poll(self, polled_flags: Optional[List[bool]] = None):
        kv = self.chans.poll_helper(polled_flags)

        for key, ready_flags in kv.items():
            if not any(ready_flags):
                continue

            sio_def = self.socketios[key]

            with self.exc_ctx(sio_def):
                r = self.actor.ctx().exec('sio', sio_def.fn, ready_flags)

                r = self._assert_tran(r)

                self.chans[key] = r


SignalHdlr = Callable[[int, int], bool]


@dataclass
class SigHdlrEntry:
    act: int
    sigh: SignalHdlr


def signal_handler(sig, frame, self: 'SignalRunner' = None):
    self.signal_handler(sig, frame)


@dataclass
class SignalRunnerRef(Terminating):
    par: 'SignalRunner'
    idx: int

    def terminate(self):
        self.par.remove(self.idx)


class LoggingActor:
    def logger(self, sn=None):
        name = self.__class__.__name__

        if sn:
            name += '.' + sn
        return log_tr_act(name)


class SignalRunner(LoggingActor):
    # remove an actor whenever any of it's signal handlers have returned true
    # need to dynamically manage the state of the current handlers.

    # an actor terminates whenever the call had been made to terminate it
    # a signal runner terminates whenever it's empty
    def __init__(
            self,
    ):
        self.idx_ctr = count()
        self.acts: Dict[int, Actor] = {}
        self.ssighs: Dict[int, List[SigHdlrEntry]] = {}

        self.ces: Dict[int, ContextManager] = {}

    def add(self, act: 'Actor', signals: Dict[int, List[SignalHdlr]]) -> SignalRunnerRef:
        self.logger('act.add').warning('%s %s', act, signals)

        new_idx = next(self.idx_ctr)
        self.acts[new_idx] = act

        for k, sighs in signals.items():
            if k not in self.ssighs:
                self._new_hdlr(k)
            for sigh in sighs:
                self.ssighs[k].append(SigHdlrEntry(new_idx, sigh))

        return SignalRunnerRef(self, new_idx)

    def remove(self, act: int):
        self.logger('act.rm').warning('%s', act)

        del self.acts[act]

        for k in list(self.ssighs.keys()):
            sighs = self.ssighs[k]

            try:
                sigh_idx = [x.act for x in sighs].index(act)
            except ValueError:
                continue
            else:
                sighs = sighs[:sigh_idx] + sighs[sigh_idx + 1:]

            if len(sighs) == 0:
                self._del_hdlr(k)
            else:
                self.ssighs[k] = sighs

    def _new_hdlr(self, code: int):
        self.logger('hdlr.add').debug('%s', code)

        assert code not in self.ssighs, (code, self.ssighs)
        self.ces[code] = signal_context(signals=(code,), handler=partial(signal_handler, self=self))
        self.ces[code].__enter__()
        self.ssighs[code] = []

    def _del_hdlr(self, code: int):
        self.logger('hdlr.rm').debug('%s', code)
        del self.ssighs[code]
        try:
            self.ces[code].__exit__(None, None, None)
        except BaseException:
            self.logger('hdlr.rm').exception('%s', code)
            raise

    def signal_handler(self, sig, frame):
        self.logger('hdlr.x').warning('%s %s', sig, frame)

        for x in list(self.ssighs[sig]):
            act = self.acts[x.act]

            try:
                act.ctx().exec('sig', x.sigh)
            except TerminationException:
                act.terminate('sigte')
            except:
                logging.getLogger(__name__ + '.' + self.__class__.__name__).exception('%s %s', sig, frame)


class RPCGroupRunner(Terminating, TerminatingHandler):
    def __init__(
            self,
            actor: 'Actor',
            group: str,
            chan: ELTransportRef,
            serde: SerdeStruct,
            rpcs: RPCEntrySet,
            horizon_each=60.
    ):
        self.actor = actor
        self.horizon_each = horizon_each

        self.log_dict = RPCLogDict(time_now())

        self.group = group
        self.chan = chan
        self.wait = actor.el.wait_add(ELWaitEntry(self.max_wait, self.timeout))

        self.chan.push(ELPktEntry(
            lambda x: x.packet.type == RPCPacketType.Req,
            self.packet_receive,
            self.exception_handler
        ))

        self.serde = serde

        self.rpcs = rpcs

    def terminate(self):
        self.wait.remove()

    def get_horizon_wait(self) -> float:
        step_time = time_now()

        next_horizon: datetime = self.log_dict.horizon + timedelta(seconds=self.horizon_each) * 2

        max_poll_horizon = (next_horizon - step_time).total_seconds()

        return max_poll_horizon

    def max_wait(self) -> Optional[float]:
        return self.get_horizon_wait()

    def timeout(self):
        self.log_dict.set_horizon(time_now() - timedelta(seconds=self.horizon_each))

    def _get_rpc(self, name):
        if name not in self.rpcs:
            logging.error(f'Could not find name %s %s', name, self.rpcs.keys())
            raise InvalidFingerprintError('name')

        rpc_def = self.rpcs[name]

        return rpc_def

    def packet_reply(self, raw_packet: RPCPacketRaw, p: RPCPacket, ret: Any):
        rpc_def = self._get_rpc(p.name)

        assert rpc_def.conf.type == RPCType.Repliable, p.name

        has_returned = p.key in self.log_dict
        assert not has_returned

        ret_payload = self.serde.serialize(rpc_def.res, ret)

        self.log_dict[p.key] = ret_payload

        rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.ok.value, ret_payload)

        self.chan.send(RPCPacketRaw(raw_packet.addr, rp))

    def packet_handle(self, packet: RPCPacket, raw_packet: RPCPacketRaw, args, kwargs):
        rpc_def = self._get_rpc(packet.name)

        rp: Optional[RPCPacket] = None

        try:
            # some RPCs may return earlier than exiting themselves
            # we make sure the RPC can not return the value twice

            has_returned = packet.key in self.log_dict

            ctx = self.actor.ctx(
                chan_def=self.group,
                origin=raw_packet.addr,
                reply=partial(self.packet_reply, raw_packet, packet)
            )

            try:
                ret = ctx.exec('call', rpc_def.fn, *args, **kwargs)
            except TerminationException:
                raise
            except Exception as e:
                logging.getLogger(__name__).exception(
                    'While receiving the payload [%s][%s][%s]', packet.name, args, kwargs)
                raise InternalError(str(e))

            if rpc_def.conf.type == RPCType.Repliable:
                if not has_returned:
                    self.packet_reply(raw_packet, packet, ret)
                elif has_returned and ret is None:
                    pass
                elif has_returned and ret is not None:
                    raise ValueError()
                else:
                    raise NotImplementedError()
        except InternalError as e:
            rp = RPCPacket(packet.key, RPCPacketType.Rep, RPCReply.internal.value, e.reason)
            _log_called_from(logging.getLogger(__name__), 'While receiving the payload [%s %s %s] %s', packet, args,
                             kwargs, rpc_def)
            self.actor.terminate('ie_' + repr(e))
        except TerminationException as e:
            self.actor.terminate('te4_' + repr(e))
        finally:
            if rp and rpc_def.conf.type == RPCType.Repliable:
                self.chan.send(RPCPacketRaw(raw_packet.addr, rp))

    def packet_receive(self, raw_packet: RPCPacketRaw):
        pkt = raw_packet.packet

        pkt_reply: Optional[RPCPacket] = None

        try:
            rpc_def = self._get_rpc(pkt.name)

            if pkt.key in self.log_dict:
                rep = self.log_dict[pkt.key]

                if rpc_def.conf.type != RPCType.Signalling:
                    pkt_reply = RPCPacket(pkt.key, RPCPacketType.Rep, RPCReply.ok.value, rep)
            else:
                try:
                    args, kwargs = self.serde.deserialize(rpc_def.req, pkt.payload)
                except Exception as e:
                    # could not deserialize the payload correctly
                    logging.exception(f'Failed to deserialize packet from {raw_packet.addr}')
                    raise InvalidFingerprintError('args')

                self.packet_handle(pkt, raw_packet, args, kwargs)

                if rpc_def.conf.type == RPCType.Durable:
                    pkt_reply = RPCPacket(pkt.key, RPCPacketType.Rep, RPCReply.ok.value, None)
                    self.log_dict[pkt.key] = None
                elif rpc_def.conf.type == RPCType.Signalling:
                    self.log_dict[pkt.key] = None
        except InvalidFingerprintError as e:
            pkt_reply = RPCPacket(pkt.key, RPCPacketType.Rep, RPCReply.fingerprint.value,
                                  self.serde.serialize(Optional[str], e.reason))
        except HorizonPassedError as e:
            pkt_reply = RPCPacket(pkt.key, RPCPacketType.Rep, RPCReply.horizon.value,
                                  self.serde.serialize(datetime, e.when))

        if pkt_reply:
            self.chan.send(RPCPacketRaw(raw_packet.addr, pkt_reply))


class Actor(Terminating, LoggingActor):
    def __init__(self, el: EventLoop):
        self.el = el

        self.idx_ctr = count()
        self.terms: Dict[int, Terminating] = {}
        self.chans: Dict[str, int] = {}

    def add_transport(self, group: str, url: str) -> ELTransportRef:
        tran = Transport.from_url(url)

        tref = self.el.transport_add(tran)

        self.chans[group] = tref.idx

        return tref

    def add(self, item: Terminating) -> int:
        new_idx = next(self.idx_ctr)
        self.terms[new_idx] = item
        return new_idx

    def remove(self, idx: int):
        del self.terms[idx]

    def terminate(self, why=None):
        self.logger('term').debug('Terminating %s %s', why, self)

        for k, v in list(self.chans.items()):
            del self.chans[k]

            tran = self.el.transport(v).remove()

            self.logger('term.tran').warning('Closing %s %s', k, v)
            tran.close()

        for k, v in list(self.terms.items()):
            del self.terms[k]
            self.logger('term.act').warning('Terminating %s %s', why, v)
            v.terminate()

    def ctx(self, **kwargs) -> ExecutionContext:
        if 'chan_def' not in kwargs:
            if DEFAULT_GROUP in self.chans:
                kwargs['chan_def'] = DEFAULT_GROUP

        return ExecutionContext(
            actor=self,
            el=self.el,
            chans=self.chans,
            **kwargs
        )


def actor_create(
        el: EventLoop,
        sr: SignalRunner,
        cls, cls_inst, bind_urls: Dict[str, str], horizon_each=60.
) -> 'Actor':
    if isinstance(bind_urls, list):
        assert len(bind_urls) == 1, bind_urls

        bind_urls = {DEFAULT_GROUP: bind_urls[0]}

    rpcs = RPCEntrySet.from_cls(cls).bind(cls_inst)
    regs = RegularEntrySet.from_cls(cls).bind(cls_inst)

    sios = SocketIOEntrySet.from_cls(cls).bind(cls_inst)
    sigs = SignalEntrySet.from_cls(cls).bind(cls_inst)

    tran_grps = sorted(bind_urls.keys())
    rpc_grps = sorted(rpcs.groups())

    assert tran_grps == rpc_grps, (tran_grps, rpc_grps)

    act = Actor(el)

    for grp in rpc_grps:
        srpcs = rpcs.by_group(grp)

        act.add(
            RPCGroupRunner(
                act,
                grp,
                act.add_transport(grp, bind_urls[grp]),
                srpcs.serde,
                srpcs,
                horizon_each=horizon_each
            )
        )
    regr = RegularRunner(act, el, regs)
    sior = SocketIORunner(act, el, sios)

    act.add(regr)
    act.add(sior)

    act.add(sr.add(act, sigs.to_signal_map()))

    return act


def run_server(cls, cls_inst, bind_urls: Dict[str, str], horizon_each=60.):
    el = EventLoop()
    sr = SignalRunner()

    actor_create(el, sr, cls, cls_inst, bind_urls, horizon_each)

    try:
        el.loop()
    except EventLoopEmpty:
        pass
