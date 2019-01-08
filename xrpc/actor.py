import contextlib
import logging
import os
import shutil
from datetime import timedelta, datetime
from functools import partial
from itertools import count
from tempfile import mkdtemp
from typing import List, Dict, Optional, Callable, Any, ContextManager

from dataclasses import dataclass

from xrpc.dict import RPCLogDict
from xrpc.dsl import RPCType, DEFAULT_GROUP
from xrpc.error import HorizonPassedError, InvalidFingerprintError, TerminationException
from xrpc.loop import EventLoop, ELTransportRef, ELPktEntry, ELWaitEntry, ELPollEntry, EventLoopEmpty, ELExcEntry
from xrpc.net import RPCPacket, RPCReply, RPCPacketType
from xrpc.runtime import ExecutionContext
from xrpc.serde.abstract import SerdeStruct
from xrpc.service import SocketIOEntrySet, RegularEntrySet, RPCEntrySet, SignalEntrySet
from xrpc.trace import log_tr_act, trc
from xrpc.transport import Transport, RPCPacketRaw, Transports, Packet
from xrpc.util import signal_context, time_now, _log_called_from, _log_traceback


class Terminating:
    def terminate(self):
        pass


class TerminatingHandler:
    actor: 'Actor'

    @contextlib.contextmanager
    def exc_ctx(self, reg_def):
        try:
            yield
        # except EventLoopEmpty:
        #    raise
        except TerminationException:
            self.actor.terminate('te1_' + repr(reg_def))
        except EventLoopEmpty:
            raise
        except BaseException:
            _log_called_from(logging.getLogger(f'{__name__}.{self.__class__.__name__}'), 'While %s', reg_def)
            self.actor.terminate('be' + repr(reg_def))
            raise

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
            k: time_now() + timedelta(seconds=x.conf.initial) if x.conf.initial is not None else None for k, x in
            self.regulars.items()
        }

        # if we make sure that max_wait is called for every execution of select_
        # then we know if sleeping is available

    def terminate(self):
        self.wait.remove()

    def reset(self, name: str, new_val: Optional[float]):
        assert isinstance(new_val, (float, int, None.__class__)), new_val

        step_time = time_now()

        if new_val is not None:
            new_val = step_time + timedelta(seconds=new_val)

        self.states_regulars[name] = new_val

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

                if x is not None and x < 0:
                    trc().error('%s (%s < 0)', name, x)

                if x is not None:
                    self.states_regulars[name] = time_now() + timedelta(seconds=x)
                else:
                    self.states_regulars[name] = None


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

    def __getattr__(self, item):
        return getattr(self.par, item)

    def terminate(self):
        self.par.remove(self.idx)


class LoggingActor:
    def logger(self, sn=None):
        name = self.__class__.__name__

        if sn:
            name += '.' + sn
        return log_tr_act(name)


class SignalRunner(LoggingActor, TerminatingHandler):
    # remove an actor whenever any of it's signal handlers have returned true
    # need to dynamically manage the state of the current handlers.

    # an actor terminates whenever the call had been made to terminate it
    # a signal runner terminates whenever it's empty
    def __init__(
            self,
            el: EventLoop,
    ):
        self.el = el
        self.idx_ctr = count()
        self.acts: Dict[int, Actor] = {}
        self.ssighs: Dict[int, List[SigHdlrEntry]] = {}

        self.ces: Dict[int, ContextManager] = {}

        self.has_transport = False
        # we use that to wake up our running thread

    def establish_transport(self):

        self.has_transport = True

        self.path_temp = mkdtemp()

        self.path_unix = 'unix://' + os.path.join(self.path_temp, 'sock.sock')
        self.tran = Transport.from_url(self.path_unix + '#bind')
        self.tran_ref = self.el.transport_add(self.tran)
        self.tran_ref.push_raw(ELPollEntry(
            self.handle_packet,
        ))

        self.logger('tran.est').debug('%s', self.path_unix)

    def _fork_transport_patch(self):
        # unsafe

        if self.has_transport:
            self.has_transport = None
            self.path_temp = None
            self.tran_ref.remove()
            self.tran = None

            self.establish_transport()

    def demolish_transport(self):
        self.tran_ref.remove()
        self.tran.close()
        shutil.rmtree(self.path_temp)

        self.has_transport = False

        self.logger('tran.dem').debug('%s', self.path_unix)

    def add(self, act: 'Actor', signals: Dict[int, List[SignalHdlr]]) -> SignalRunnerRef:
        self.logger('act.add').info('%s %s', act, signals)

        new_idx = next(self.idx_ctr)
        self.acts[new_idx] = act

        for k, sighs in signals.items():
            if k not in self.ssighs:
                self._new_hdlr(k)
            for sigh in sighs:
                self.ssighs[k].append(SigHdlrEntry(new_idx, sigh))

        if len(self.ssighs) and not self.has_transport:
            self.establish_transport()

        return SignalRunnerRef(self, new_idx)

    def remove(self, act: int):
        self.logger('act.rm').info('%s %s %s', act, self.ssighs, self.has_transport)

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
        if len(self.ssighs) == 0 and self.has_transport:
            self.demolish_transport()

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

    def handle_packet(self, flags: Optional[List[bool]]):
        while True:
            try:
                for packet in self.tran.read(flags):
                    code = int(packet.data.decode())

                    self.handle_actor(code)
            except ConnectionAbortedError:
                continue
            else:
                return

    def handle_actor(self, sig):
        for sighe in self.ssighs[sig]:
            if sighe.act not in self.acts:
                continue

            act = self.acts[sighe.act]

            try:
                act.ctx().exec('sig', sighe.sigh)
            except TerminationException:
                act.terminate('sigte')
            except:
                logging.getLogger(__name__ + '.' + self.__class__.__name__).exception('%s', sig)

    def signal_handler(self, sig, frame):
        self.logger('hdlr.x').info('%s %s', sig, frame)

        with Transport.from_url(self.path_unix) as t:
            sig = str(sig).encode()
            t.send(Packet(self.path_unix, sig))


@dataclass(eq=True)
class HandlingMarker:
    pass


HANDLING = HandlingMarker()


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

        self.rpcs: RPCEntrySet = {k: v for k, v in rpcs.items() if not v.conf.exc}

        self.rpcs_exc: RPCEntrySet = {k: v for k, v in rpcs.items() if v.conf.exc}

        self.chan.push_exc(ELExcEntry(self.handle_exc))

    def handle_exc(self, exc: BaseException) -> bool:
        ctx = self.actor.ctx()
        for k, v in self.rpcs_exc.items():
            try:
                if ctx.exec('tran_exc', v.fn, exc):
                    return True
            except TerminationException:
                self.actor.terminate('te')
                return True
        else:
            return False

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
            _log_traceback(trc('1'))
            trc('0').error('%s %s %s %s', self.chan.origin, self.group, name, self.rpcs.keys())
            raise InvalidFingerprintError('name')

        rpc_def = self.rpcs[name]

        return rpc_def

    def has_returned(self, key):
        return key in self.log_dict and self.log_dict[key] != HANDLING

    def packet_reply(self, raw_packet: RPCPacketRaw, p: RPCPacket, ret: Any):
        rpc_def = self._get_rpc(p.name)

        assert rpc_def.conf.type == RPCType.Repliable, p.name

        has_returned = self.has_returned(p.key)
        assert not has_returned

        ret_payload = self.serde.serialize(rpc_def.res, ret)

        self.log_dict[p.key] = ret_payload

        rp = RPCPacket(p.key, RPCPacketType.Rep, RPCReply.ok.value, ret_payload)

        self.chan.send(RPCPacketRaw(raw_packet.addr, rp))

    def packet_handle(self, packet: RPCPacket, raw_packet: RPCPacketRaw, args, kwargs):
        rpc_def = self._get_rpc(packet.name)

        # try:
        # some RPCs may return earlier than exiting themselves
        # we make sure the RPC can not return the value twice

        has_returned = self.has_returned(packet.key)

        ctx = self.actor.ctx(
            chan_def=self.group,
            origin=raw_packet.addr,
            reply=partial(self.packet_reply, raw_packet, packet)
        )

        try:
            ret = ctx.exec('call', rpc_def.fn, *args, **kwargs)
        except TerminationException:
            raise

        if rpc_def.conf.type == RPCType.Repliable:
            if not has_returned:
                self.packet_reply(raw_packet, packet, ret)
            elif has_returned and ret is None:
                pass
            elif has_returned and ret is not None:
                raise ValueError(self.log_dict[packet.key])
            else:
                raise NotImplementedError()

    def packet_receive(self, raw_packet: RPCPacketRaw):
        pkt = raw_packet.packet

        def reply_now(a: str, b: Any):
            rp = RPCPacket(pkt.key, RPCPacketType.Rep, a, b)

            self.chan.send(RPCPacketRaw(raw_packet.addr, rp))

        try:
            rpc_def = self._get_rpc(pkt.name)
        except InvalidFingerprintError as e:
            reply_now(RPCReply.fingerprint.value, self.serde.serialize(Optional[str], e.reason))
            return

        try:
            if pkt.key in self.log_dict:
                rep = self.log_dict[pkt.key]

                if rep == HANDLING:
                    # todo short-circuit packets that we have received (maybe acknowledge them even)
                    return

                if rpc_def.conf.type != RPCType.Signalling:
                    reply_now(RPCReply.ok.value, rep)

                return
        except HorizonPassedError as e:
            reply_now(RPCReply.horizon.value, self.serde.serialize(datetime, e.when))
            return

        try:
            args, kwargs = self.serde.deserialize(rpc_def.req, pkt.payload)
        except Exception as e:
            # could not deserialize the payload correctly
            logging.exception(f'Failed to deserialize packet from {raw_packet.addr}')
            reply_now(RPCReply.fingerprint.value, self.serde.serialize(Optional[str], 'args'))
            return

        self.log_dict[pkt.key] = HANDLING

        if rpc_def.conf.type == RPCType.Durable:
            reply_now(RPCReply.ok.value, None)
            self.log_dict[pkt.key] = None
        elif rpc_def.conf.type == RPCType.Signalling:
            self.log_dict[pkt.key] = None

        try:
            self.packet_handle(pkt, raw_packet, args, kwargs)
        except Exception as e:
            # reply_now(RPCReply.internal.value, str(e))
            _log_called_from(logging.getLogger(__name__), 'While receiving the payload [%s %s %s] %s', pkt, args,
                             kwargs, rpc_def)
            self.actor.terminate('ie_' + repr(e))
            raise
        except TerminationException as e:
            if rpc_def.conf.type == RPCType.Repliable:
                reply_now(RPCReply.ok.value, self.serde.serialize(rpc_def.res, None))
            self.actor.terminate('te4_' + repr(e))


class Actor(Terminating, LoggingActor):
    def __init__(self, el: EventLoop, name: Optional[str] = None):
        self.el = el

        self.name = name

        self.idx_ctr = count()
        self.terms: Dict[int, Terminating] = {}
        self.names_terms: Dict[str, int] = {}
        self.terms_names: Dict[int, str] = {}
        self.chans: Dict[str, int] = {}

    def add_transport(self, group: str, url: str) -> ELTransportRef:
        tran = Transport.from_url(url)

        tref = self.el.transport_add(tran)

        self.chans[group] = tref.idx

        return tref

    def get(self, name) -> Terminating:
        return self.terms[self.names_terms[name]]

    def add(self, item: Terminating, name: Optional[str] = None) -> int:
        new_idx = next(self.idx_ctr)
        self.terms[new_idx] = item

        if name is not None:
            assert name not in self.names_terms, (self.names_terms, name)
            self.names_terms[name] = new_idx
            self.terms_names[new_idx] = name

        return new_idx

    def remove(self, idx: int):
        del self.terms[idx]

        if idx in self.terms_names:
            name = self.terms_names[idx]
            del self.terms_names[idx]
            del self.names_terms[name]

    def terminate(self, why=None):
        self.logger('term').info('Terminating %s %s %s', self.name, why, self)

        for k, v in list(self.chans.items()):
            del self.chans[k]

            tran = self.el.transport(v).remove()

            self.logger('term.tran').info('Closing %s %s %s', self.name, k, v)
            tran.close()

        for k, v in list(self.terms.items()):
            del self.terms[k]
            self.logger('term.act').info('Terminating %s %s %s', self.name, why, v)
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
        cls, cls_inst, bind_urls: Dict[str, str],
        horizon_each=60.,
        name: Optional[str] = None
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

    rpc_requested = set(rpc_grps) - set(tran_grps)

    assert len(rpc_requested) == 0, rpc_requested

    act = Actor(el, name=name)

    for tran_grp in tran_grps:
        trc('tran').debug('%s %s', tran_grp, bind_urls[tran_grp])

        tran_ref = act.add_transport(tran_grp, bind_urls[tran_grp])

        if tran_grp not in rpc_grps:
            continue

        srpcs = rpcs.by_group(tran_grp)

        act.add(
            RPCGroupRunner(
                act,
                tran_grp,
                tran_ref,
                srpcs.serde,
                srpcs,
                horizon_each=horizon_each
            )
        )
    regr = RegularRunner(act, el, regs)
    sior = SocketIORunner(act, el, sios)

    act.add(regr, 'regular')
    act.add(sior)

    act.add(sr.add(act, sigs.to_signal_map()), 'signal')

    return act


def run_server(cls, cls_inst, bind_urls: Dict[str, str], horizon_each=60., actor_name=None):
    el = EventLoop()
    sr = SignalRunner(el)

    if actor_name is None:
        if hasattr(cls, '__name__'):
            actor_name = cls.__name__
        else:
            actor_name = str(cls)

    act = actor_create(el, sr, cls, cls_inst, bind_urls, horizon_each, name=actor_name)

    try:
        el.loop()
    except EventLoopEmpty:
        pass
    except:
        act.terminate('exit')
        raise
