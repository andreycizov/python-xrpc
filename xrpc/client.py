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


class ServiceWrapper:
    def __init__(self, defn: ServiceDefn, conf: ClientConfig, transport: RPCTransportStack, dest: Origin):
        self.transport = transport
        self.dest = dest
        self.defn = defn
        self.conf = conf

    def __getattr__(self, item, key: Optional[RPCKey] = None):
        if item in self.defn.rpcs:
            if key is None:
                key = RPCKey.new()

            return CallWrapper(self, item, key)
        else:
            raise AttributeError(item)


class CallWrapper:
    def __init__(self, type: ServiceWrapper, name: str, key: RPCKey):
        self.type = type
        self.name = name
        self.key = key

    def __call__(self, *args, **kwargs):
        c = self.type.defn.rpcs[self.name]
        i, o = self.type.defn.rpcs_serde[self.name]
        payload = self.type.defn.serde.serialize(i, [args, kwargs])

        packet = RPCPacket(self.key, RPCPacketType.Req, self.name, payload)

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
                    raise InvalidFingerprintError()
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

                    flags = select_helper([x.fd for x in self.type.transport.transports], max_wait=max(0., self.type.conf.timeout_resend))

                    self.type.transport.recv(flags)
                return ret

            finally:
                # todo: we can not be sure that all of the packets here have been read ? or can we?
                self.type.transport.pop()
        else:
            raise NotImplementedError(c.conf.type)


def build_wrapper(pt: ServiceDefn, transport: RPCTransportStack, dest: Origin, conf: ClientConfig=ClientConfig()):
    return ServiceWrapper(pt, conf, transport, dest)
