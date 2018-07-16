import logging
from urllib.parse import urlparse, parse_qs, ParseResult, urlunparse

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, NamedTuple

from xrpc.net import RPCKey, RPCPacket, RPCReply, RPCPacketType, time_unpack
from xrpc.dsl import RPCType
from xrpc.service import ServiceDefn
from xrpc.transport import Origin, RPCTransportStack, RPCPacketRaw, select_helper
from xrpc.error import TimeoutError, InvalidFingerprintError, HorizonPassedError, InternalError

from xrpc.util import time_now


class ClientConfig(NamedTuple):
    timeout_resend: float = 0.033
    timeout_total: Optional[float] = None
    ignore_horizon: bool = False
    """If ```HorizonPassedError``` is passed, restart the action"""


def dest_overrides(url):
    r: ParseResult = urlparse(url)
    qs = parse_qs(r.query) if r.query else {}
    r = r._replace(query=None)
    r = urlunparse(r)

    return {k: v[0] for k, v in qs.items()}, r


class ServiceWrapper:
    def __init__(self, defn: ServiceDefn, conf: ClientConfig, transport: RPCTransportStack, dest: Origin):
        self.transport = transport
        self.overrides, self.dest = dest_overrides(dest)
        self.defn = defn
        self.conf = conf

    def _assert(self):
        missing = []
        for x in self.overrides.keys():
            if x not in self.defn.rpcs:
                missing.append(x)
        if len(missing):
            assert False, missing

    def __getattr__(self, item):
        f1 = item in self.defn.rpcs
        alias = self.overrides.get(item)

        if f1 or alias:
            return CallWrapper(self, item, alias)
        else:
            logging.getLogger(__name__).error('%s', self.defn.rpcs)
            raise AttributeError(item)


@dataclass
class RequestWrapper:
    type: ServiceWrapper
    name: str
    alias: Optional[str]
    key: RPCKey = field(default_factory=RPCKey)

    def __call__(self, *args, **kwargs):
        c = self.type.defn.rpcs[self.name]
        i, o = self.type.defn.rpcs_serde[self.name]
        payload = self.type.defn.serde.serialize(i, [args, kwargs])

        packet = RPCPacket(self.key, RPCPacketType.Req, self.alias or self.name, payload)

        # the only difference between a client and a server is NONE.
        # the only issue would be the routing of the required packets to the required instances

        if c.conf.type == RPCType.Signalling:
            self.type.transport.send(RPCPacketRaw(self.type.dest, packet))
        elif c.conf.type in [RPCType.Repliable, RPCType.Durable]:
            time_started = time_now()

            stop = False
            ret = None

            def process_packet(x: RPCPacketRaw):
                packet = x.packet

                assert packet.key == self.key, (packet, self.key)

                nonlocal stop
                nonlocal ret
                stop = True

                if packet.name == RPCReply.ok.value:
                    ret = self.type.defn.serde.deserialize(o, packet.payload)
                elif packet.name == RPCReply.fingerprint.value:
                    raise InvalidFingerprintError(self.type.defn.serde.deserialize(Optional[str], packet.payload))
                elif packet.name == RPCReply.horizon.value:
                    raise HorizonPassedError(self.type.defn.serde.deserialize(datetime, packet.payload))
                elif packet.name == RPCReply.internal.value:
                    raise InternalError(packet.payload)
                else:
                    raise NotImplementedError(packet.name)

            self.type.transport.push(
                lambda x: x.packet.key == self.key and x.packet.type == RPCPacketType.Rep,
                process_packet
            )

            try:
                while not stop:
                    time_step = time_now()

                    dur_passed = time_step - time_started

                    if self.type.conf.timeout_total is not None and dur_passed.total_seconds() > self.type.conf.timeout_total:
                        raise TimeoutError()

                    self.type.transport.send(RPCPacketRaw(self.type.dest, packet))

                    # todo transform transport sends to
                    # todo an ability to buffer the transport outputs

                    flags = select_helper([x.fd for x in self.type.transport.transports],
                                          max_wait=max(0., self.type.conf.timeout_resend))

                    self.type.transport.recv(flags)
                return ret

            finally:
                # todo: we can not be sure that all of the packets here have been read ? or can we?
                self.type.transport.pop()
        else:
            raise NotImplementedError(c.conf.type)


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
                if self.type.conf.ignore_horizon:
                    continue
                raise


def build_wrapper(pt: ServiceDefn, transport: RPCTransportStack, dest: Origin, conf: ClientConfig = ClientConfig()):
    return ServiceWrapper(pt, conf, transport, dest)
