import logging
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from typing import Optional, NamedTuple, Any, Type, ContextManager, TypeVar
from urllib.parse import urlparse, parse_qs, ParseResult, urlunparse

from dataclasses import dataclass, field

from xrpc.dsl import RPCType
from xrpc.error import TimeoutError, InvalidFingerprintError, HorizonPassedError, InternalError
from xrpc.loop import EventLoop, ELTransportRef, ELPktEntry, raise_exc_handler
from xrpc.net import RPCKey, RPCPacket, RPCReply, RPCPacketType
from xrpc.service import ServiceDefn
from xrpc.transport import Origin, RPCPacketRaw, Transport
from xrpc.util import time_now


class ClientConfig(NamedTuple):
    timeout_resend: float = 0.033
    timeout_total: Optional[float] = 3.5
    horz: bool = True
    """If ```HorizonPassedError``` is passed, restart the action"""


def dest_overrides(url):
    r: ParseResult = urlparse(url)
    qs = parse_qs(r.query) if r.query else {}
    r = r._replace(query=None)
    r: str = urlunparse(r)

    return {k: v[0] for k, v in qs.items()}, r


class ServiceWrapper:
    def __init__(self, defn: ServiceDefn, conf: ClientConfig, tran: ELTransportRef, dest: Origin):
        self.tran = tran
        self.overrides, self.dest = dest_overrides(dest)
        self.defn = defn
        self.conf = conf

        self._assert()

    def _assert(self):
        missing = []
        for x in self.overrides.keys():
            if x not in self.defn:
                missing.append(x)
        if len(missing):
            assert False, missing

    def __getattr__(self, item):
        f1 = item in self.defn
        alias = self.overrides.get(item)

        if f1 or alias:
            return CallWrapper(self, item, alias)
        else:
            logging.getLogger(__name__).error('%s', self.defn)
            raise AttributeError(item)


class RequestStopper(BaseException):
    def __init__(self, return_: Any, *args: object, **kwargs: object) -> None:
        self.return_ = return_
        super().__init__(return_, *args, **kwargs)


@dataclass
class RequestWrapper:
    par: ServiceWrapper
    name: str
    alias: Optional[str]
    key: RPCKey = field(default_factory=RPCKey)

    def __post_init__(self):
        self.call = self.par.defn[self.name]

    def __call__(self, *args, **kwargs):
        payload = self.par.defn.serde.serialize(self.call.req, [args, kwargs])

        packet = RPCPacket(self.key, RPCPacketType.Req, self.alias or self.name, payload)

        # the only difference between a client and a server is NONE.
        # the only issue would be the routing of the required packets to the required instances

        if self.call.conf.type == RPCType.Signalling:
            self.par.tran.send(RPCPacketRaw(self.par.dest, packet))
        elif self.call.conf.type in [RPCType.Repliable, RPCType.Durable]:
            time_started = time_now()

            self.par.tran.push(ELPktEntry(
                lambda x: x.packet.key == self.key and x.packet.type == RPCPacketType.Rep,
                self.process_packet,
                partial(raise_exc_handler, should_log=False),
            ))

            try:
                while True:
                    time_step = time_now()

                    dur_passed = time_step - time_started

                    if self.par.conf.timeout_total is not None and dur_passed.total_seconds() > self.par.conf.timeout_total:
                        raise TimeoutError()

                    self.par.tran.send(RPCPacketRaw(self.par.dest, packet))

                    self.par.tran.loop(max(0., self.par.conf.timeout_resend))
            except RequestStopper as e:
                return e.return_
            finally:
                # todo: if someone causes a TerminationException,

                # todo: if a server causes an EventLoopEmpty while doing this
                # todo: then the current transport is going to be destroyed before we reach this line

                if self.par.tran.is_alive():
                    self.par.tran.pop()
        else:
            raise NotImplementedError(self.call.conf.type)

    def process_packet(self, x: RPCPacketRaw):
        packet = x.packet

        assert packet.key == self.key, (packet, self.key)

        if packet.name == RPCReply.ok.value:
            return_ = self.par.defn.serde.deserialize(self.call.res, packet.payload)
            raise RequestStopper(return_)
        elif packet.name == RPCReply.fingerprint.value:
            raise InvalidFingerprintError(self.par.defn.serde.deserialize(Optional[str], packet.payload))
        elif packet.name == RPCReply.horizon.value:
            raise HorizonPassedError(self.par.defn.serde.deserialize(datetime, packet.payload))
        elif packet.name == RPCReply.internal.value:
            raise InternalError(packet.payload)
        else:
            raise NotImplementedError(packet.name)


@dataclass()
class CallWrapper:
    type: ServiceWrapper
    name: str
    alias: Optional[str]

    def __call__(self, *args, **kwargs):
        while True:
            try:
                return RequestWrapper(self.type, self.name, self.alias)(*args, **kwargs)
            except HorizonPassedError:
                if not self.type.conf.horz:
                    continue
                raise


def build_wrapper(pt: ServiceDefn, tran: ELTransportRef, dest: Origin, conf: ClientConfig = ClientConfig()):
    return ServiceWrapper(pt, conf, tran, dest)


class ClientTransportCircuitBreaker(BaseException):
    def __init__(self, pkt):
        self.pkt = pkt
        super().__init__(pkt)


T = TypeVar('T')


@contextmanager
def client_transport(
        rpc: Type[T],
        dest: str = 'udp://127.0.0.1:7483',
        conf: Optional[ClientConfig] = None,
        origin: str = 'udp://127.0.0.1',
        **kwargs
) -> ContextManager[T]:
    t = Transport.from_url(origin)

    if conf is None:
        conf = ClientConfig(**kwargs)

    def client_cb(p):
        raise ClientTransportCircuitBreaker(p)

    with t:
        ts = EventLoop()
        tref = ts.transport_add(t)
        # we may receive packets which are replies to things that are too late
        tref.push(ELPktEntry(lambda p: p.packet.type != RPCPacketType.Rep, client_cb, raise_exc_handler))
        pt = ServiceDefn.from_cls(rpc)
        r: T = build_wrapper(pt, tref, dest, conf=conf)

        yield r
