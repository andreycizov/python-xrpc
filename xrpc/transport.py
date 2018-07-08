import logging
import select
import socket
import struct
from typing import Iterable, Dict, NamedTuple, Type, List, Callable, Optional
from urllib.parse import ParseResult, urlparse, parse_qs

from xrpc.net import RPCPacket

Origin = str

PACKET_PACKER = struct.Struct('!I')


class Packet(NamedTuple):
    addr: Origin
    data: bytes

    def pack(self) -> bytes:
        body = self.data

        len_bts = PACKET_PACKER.pack(len(body))

        return len_bts + body

    @classmethod
    def unpack(cls, addr: Origin, buffer: bytes):
        buffer = memoryview(buffer)

        if len(buffer) < 4:
            raise ValueError('Could not parse size')
        size, = PACKET_PACKER.unpack(buffer[:4])
        if len(buffer) < 4 + size:
            raise ValueError('Size is not enough')

        y = Packet(addr, buffer[4:size + 4].tobytes())

        return y


class Transport:
    def __init__(self, url):
        self.url: ParseResult = urlparse(url)
        self._fd = None

    @property
    def fd(self):
        assert self._fd, '_fd must be set'
        return self._fd

    @fd.setter
    def fd(self, value):
        self._fd = value

    def send(self, packet: Packet):
        raise NotImplementedError()

    def read(self) -> Iterable[Packet]:
        raise NotImplementedError()

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @classmethod
    def from_url(cls, url) -> 'Transport':
        parsed: ParseResult = urlparse(url)

        if parsed.scheme in TRANSPORT_MAP:
            return TRANSPORT_MAP[parsed.scheme](url)
        else:
            raise ValueError(f'Could not find transport for scheme `{parsed.scheme}`')


def recvfrom_helper(fd, buffer_size=2 ** 16, logger_name='net.trace.raw'):
    try:
        while True:

            buffer, addr = fd.recvfrom(buffer_size)

            if len(buffer) == 0:
                break

            try:
                yield Packet.unpack(addr, buffer)
            except ValueError:
                logging.getLogger(f'{logger_name}.e').error('[%d] %s %s', len(buffer), addr, buffer)
                break
    except BlockingIOError:
        return


class UDPTransport(Transport):
    # we need this on both sides, really
    # Sender and Receiver
    def __init__(self, url, buffer=2 ** 16):
        super().__init__(url)

        assert isinstance(buffer, int), buffer

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        sock.bind((self.url.hostname if self.url.hostname else '0.0.0.0', self.url.port if self.url.port else 0))
        sock.settimeout(0)

        self.fd: socket = sock
        self.buffer = buffer

    def send(self, packet: Packet):
        addr = packet.addr

        if isinstance(addr, str):
            pr: ParseResult = urlparse(addr)
            addr = (pr.hostname, int(pr.port))
            assert pr.scheme == 'udp', pr

        logging.getLogger('net.trace.raw.o').debug('[%d] %s %s', len(packet.data), addr, packet.data)

        return self.fd.sendto(packet.pack(), addr)

    def read(self) -> Iterable[Packet]:
        for x in recvfrom_helper(self.fd, self.buffer):
            logging.getLogger('net.trace.raw.i').debug('[%d] %s %s', len(x.data), x.addr,
                                                       x.data)
            yield x

    def close(self):
        self.fd.close()


class UnixTransport(UDPTransport):
    def __init__(self, url):
        Transport.__init__(self, url)

        parsed: ParseResult = urlparse(url)

        should_bind = False

        if parsed.query:
            q = parse_qs(parsed.query, keep_blank_values=True)

            if 'bind' in q:
                should_bind = True

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

        if should_bind:
            sock.bind(parsed.path)
            sock.listen(1)
            sock.settimeout(0)
        else:
            sock.connect(parsed.path)

        self.fd: socket = sock


TRANSPORT_MAP: Dict[str, Type[Transport]] = {
    'udp': UDPTransport,
    'unix': UnixTransport,
}


class RPCPacketRaw(NamedTuple):
    addr: Origin
    packet: RPCPacket


RPCTransportStackMatcher = Callable[[RPCPacketRaw], bool]
RPCTransportStackProcessor = Callable[[RPCPacketRaw], None]


class RPCTransportStackEntry(NamedTuple):
    matcher: RPCTransportStackMatcher
    processor: RPCTransportStackProcessor


class RPCTransportStack:
    def __init__(self, transports: List[Transport]):
        self.transports = transports
        self.stack: List[RPCTransportStackEntry] = []

    def wait(self):
        return [x.fd for x in self.transports]

    def recv(self, polled_flags: Optional[List[bool]] = None):
        if polled_flags is None:
            polled_flags = [True for _ in self.transports]

        if not polled_flags:
            return []

        for has_data, transport in zip(polled_flags, self.transports):
            if not has_data:
                continue

            for raw_packet in transport.read():
                packet = RPCPacket.unpack(raw_packet.data)

                logging.getLogger('net.trace.pkt.i').debug('%s %s', raw_packet.addr, packet)

                for idx in range(len(self.stack) - 1, -1, -1):
                    pass

                raw_rpc_packet = RPCPacketRaw(raw_packet.addr, packet)

                for st in self.stack[::-1]:
                    if st.matcher(raw_rpc_packet):
                        st.processor(raw_rpc_packet)
                        break
                else:
                    logging.getLogger('net.trace.pkt.u').debug('%s %s', raw_packet.addr, packet)

    def send(self, packet: RPCPacketRaw):
        # todo: think about how this would work with RPC groups assigned to different ports

        logging.getLogger('net.trace.pkt.o').debug('%s %s', packet.addr, packet.packet)

        raw_packet = Packet(packet.addr, packet.packet.pack())

        self.transports[0].send(raw_packet)

    def push(self, fn: RPCTransportStackMatcher, processor: RPCTransportStackProcessor):
        self.stack.append(RPCTransportStackEntry(fn, processor))

    def pop(self):
        x = self.stack.pop()


def select_helper(fds, max_wait: float) -> Optional[List[bool]]:
    rd, _, er = select.select(fds, [], fds, max_wait)

    r = set(rd + er)

    return [fd in r for fd in fds]
