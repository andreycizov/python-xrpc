import logging
import os
import select
import struct
import tempfile
from urllib.parse import ParseResult, urlparse, urlunparse

import shutil
import socket
from dataclasses import dataclass
from typing import Iterable, Dict, NamedTuple, Type, List, Optional, TypeVar, Generic, Tuple

from xrpc.net import json_pack, json_unpack, RPCPacket
from xrpc.serde.abstract import SerdeStruct
from xrpc.trace import log_tr_net_raw_out, log_tr_net_raw_err, log_tr_net_raw_in, log_tr_net_meta_out, \
    log_tr_net_meta_in, log_tr_net_obj_out, log_tr_net_obj_in, log_tr_net_sel_err, log_tr_net_sel_in, trc
from xrpc.util import _log_called_from

Origin = str

PACKET_PACKER = struct.Struct('!I')


def _norm_addr(addr: Origin):
    pr: ParseResult = urlparse(addr)

    pr = pr._replace(params=None, query=None, fragment=None)

    return urlunparse(pr)


class RPCPacketRaw(NamedTuple):
    addr: Origin
    packet: RPCPacket


@dataclass(eq=True, frozen=True)
class Packet:
    addr: Origin
    data: bytes

    def __repr__(self):
        txt = repr(self.data[:12])

        if len(self.data) > 12:
            txt += '~'

        return f'{self.__class__.__name__}({self.addr}. {txt})'

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

        assert len(buffer) == size + 4, (len(buffer), size + 4)

        return y


class Transport:
    def __init__(self, url):
        self.url: ParseResult = urlparse(url)
        self._fd = None

    def __len__(self):
        return len(self.fds)

    @property
    def fd(self) -> socket.socket:
        assert self._fd, '_fd must be set'
        return self._fd

    @fd.setter
    def fd(self, value):
        self._fd = value

    @property
    def origin(self) -> Origin:
        raise NotImplementedError()

    @property
    def fds(self) -> List[socket.socket]:
        return [self.fd]

    def send(self, packet: Packet):
        raise NotImplementedError()

    def read(self, polled_flags: Optional[List[bool]] = None) -> Iterable[Packet]:
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

        trc().debug('%s %s', url, parsed)

        if parsed.scheme in TRANSPORT_MAP:
            return TRANSPORT_MAP[parsed.scheme](url)
        else:
            raise ValueError(f'Could not find transport for scheme `{parsed.scheme}`')


class FauxTransport(Transport):
    def __init__(self, *fds: socket.socket):
        self._fds = list(fds)

    @property
    def fds(self) -> List[socket.socket]:
        return self._fds


class Transports(Transport):
    def __init__(self, **kwargs: Transport):
        self.tss: Dict[str, Transport] = kwargs

    def _mapping(self):
        iter_obj = enumerate((ti, fd) for _, (ti, v) in enumerate(sorted(self.tss.items())) for fd in v.fds)
        iter_obj = ((tti, ti, fd) for tti, (ti, fd) in iter_obj)

        return iter_obj

    def poll_helper(self, poll_flags: Optional[List[bool]] = None) -> Dict[str, List[bool]]:
        r = {}

        for tti, ti, fd in self._mapping():
            if ti not in r:
                r[ti] = []

            if poll_flags is None:
                r[ti].append(True)
            else:
                r[ti].append(poll_flags[tti])
        return r

    @property
    def fds(self) -> List[socket.socket]:
        return [x for _, _, x in self._mapping()]

    def __setitem__(self, key, value):
        self.tss[key] = value

    def __getitem__(self, item):
        return self.tss[item]


def recvfrom_helper(fd, buffer_size=2 ** 16, logger_name='net.trace.raw'):
    try:
        while True:
            try:
                buffer, addr = fd.recvfrom(buffer_size)
            except OSError as e:
                if e.errno == 9:
                    raise ConnectionAbortedError(str(e))
                else:
                    raise

            if len(buffer) == 0:
                raise ConnectionAbortedError('Zero bytes received')

            try:
                yield Packet.unpack(addr, buffer)
            except ValueError:
                logging.getLogger(f'{logger_name}.e').error('[%d] %s %s', len(buffer), addr, buffer)
                break
    except BlockingIOError:
        return


class UDPTransport(Transport):
    # transport needs to be able to discern the clients it announces itself

    # we need this on both sides, really
    # Sender and Receiver
    def __init__(self, url, buffer=2 ** 16):
        super().__init__(url)

        assert isinstance(buffer, int), buffer

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP

        try:
            sock.bind((self.url.hostname if self.url.hostname else '0.0.0.0', self.url.port if self.url.port else 0))
        except OSError as e:
            raise OSError(f'{url}')
        sock.settimeout(0)

        self.url = url

        self.fd: socket = sock
        self.buffer = buffer

    def __len__(self):
        return 1

    @property
    def origin(self) -> Origin:
        addr, port = self.fd.getsockname()
        return f'udp://{addr}:{port}'

    def send(self, packet: Packet):
        addr = packet.addr

        if isinstance(addr, str):
            pr: ParseResult = urlparse(addr)
            assert pr.scheme == 'udp' and pr.port is not None, pr

            addr = (pr.hostname, int(pr.port))
        else:
            raise NotImplementedError(repr(addr))

        log_tr_net_raw_out.debug('[%d] %s %s', len(packet.data), addr, packet.data)

        try:
            return self.fd.sendto(packet.pack(), addr)
        except OSError as e:
            if str(e).endswith('Invalid argument'):
                log_tr_net_raw_err.error('%s %s %s', self.url, addr, e)
                return None
        except socket.gaierror as e:
            if str(e).endswith('Try again'):
                log_tr_net_raw_err.debug('%s %s', addr, e)
                return None
            elif str(e).endswith('Name or service not known'):
                log_tr_net_raw_err.debug('%s %s', addr, e)
                return None
            else:
                raise ConnectionError(str(e))

    def read(self, polled_flags: Optional[List[bool]] = None) -> Iterable[Packet]:
        for x in recvfrom_helper(self.fd, self.buffer):
            log_tr_net_raw_in.debug('[%d] %s %s', len(x.data), x.addr,
                                                       x.data)

            addr, port = x.addr

            yield Packet(f'udp://{addr}:{port}', x.data)

    def close(self):
        log_tr_net_meta_out.debug('[3] %s Close %s', self, self.fd)
        self.fd.close()


def _insert_ordered(insert_to, insert_val, insert_item, ord_col, cmp_fn=lambda x: x):
    """

    :param insert_to: ``len(insert_to) = len(ord_col) - 1``
    :param insert_val: value to insert into ``insert_to``
    :param insert_item: value inserted into ``ord_col``
    :param ord_col: collection of ``insert_item`` ordered by cmp_fn
    :param cmp_fn:
    :return:
    """
    idx = ord_col.index(cmp_fn(insert_item))

    return insert_to[:idx] + [insert_val] + insert_to[idx:]


class UnixTransport(UDPTransport):
    def __init__(self, url, buffer_size=2 ** 24):
        # todo implement multi-server client transport by connecting when sending
        # todo how do we handle gc'ing of the conns ?

        Transport.__init__(self, url)

        self.buffer_size = buffer_size

        parsed: ParseResult = urlparse(url)

        should_bind = parsed.fragment == 'bind'

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        sock.setblocking(False)
        sock.settimeout(0)

        self._fd_bind: Optional[socket] = None
        self._client_ctr = 0
        self._fd_clients: Dict[Origin, socket] = {}

        self._temp_dir = None

        path = parsed.path

        if not path:
            self._temp_dir = tempfile.mkdtemp()

            path = os.path.join(self._temp_dir, 'unix.sock')

        self._path = path

        if should_bind:
            self._fd_bind = sock
            log_tr_net_meta_in.debug('Bind `%s`', self._path)
            sock.bind(self._path)
            sock.listen(1024)

        else:
            self._fd_clients[_norm_addr(url)] = sock
            sock.connect(parsed.path)

            log_tr_net_meta_out.debug('Connect `%s`', parsed.path)

    def __len__(self):
        return len(self._fd_clients) if self._fd_bind else 1

    @property
    def _next_client_id(self):
        r = self._client_ctr
        self._client_ctr += 1
        return r

    @property
    def _fd_clients_ord(self):
        return [(x, y) for x, y in sorted(self._fd_clients.items())]

    @property
    def fds(self) -> List[socket.socket]:
        bind_fd = []
        if self._fd_bind:
            bind_fd = [self._fd_bind]
        r = bind_fd + [x for _, x in self._fd_clients_ord]
        return r

    @property
    def origin(self) -> Origin:
        return f'unix://{self._path}'

    def _clean_client_id(self, addr, reason=None):
        log_tr_net_meta_in.debug('Close %s %s', addr, reason)

        if addr in self._fd_clients:
            cli = self._fd_clients[addr]
            del self._fd_clients[addr]
            cli.close()
        else:
            raise ValueError(f'{addr} {reason}')

    def send(self, packet: Packet):
        addr = packet.addr

        # todo: KeyError if the address had been subsequently removed

        addr = _norm_addr(addr)

        if addr not in self._fd_clients:
            log_tr_net_raw_err.error('Drop %s %s %s', self, addr, packet)
            for x in self._fd_clients.keys():
                log_tr_net_raw_err.getChild('dump').error('%s', x)
            _log_called_from(log_tr_net_raw_err.getChild('tb'))
            return

        sock = self._fd_clients[addr]

        log_tr_net_raw_out.debug('[%d] %s %s', len(packet.data), addr, packet.data)

        try:
            r = sock.send(packet.pack())
            trc('return').debug('%s', r)
            return r
        except BrokenPipeError as e:
            self._clean_client_id(addr, str(e))
            log_tr_net_raw_err.error('[%d] %s %s', len(packet.data), addr, packet.data)
            # todo should we raise or should we ignore the error ?
            # todo upstream possibly needs to know when a client was disconnected
            #raise ConnectionAbortedError(addr, str(e))

    def read(self, polled_flags: Optional[List[bool]] = None) -> Iterable[Packet]:
        if polled_flags is None:
            polled_flags = [True for _ in self._fd_clients_ord]

            if self._fd_bind:
                polled_flags = [True] + polled_flags

        if self._fd_bind:
            polled_bind, *polled_flags = polled_flags
        else:
            polled_bind = False

        if polled_bind:
            while True:
                try:
                    c_sock, _ = self._fd_bind.accept()
                    c_sock.setblocking(False)
                    c_sock.settimeout(0)

                    addr = self._next_client_id

                    c_addr_url = _norm_addr(f'unix://{addr}{self._path}')

                    self._fd_clients[c_addr_url] = c_sock

                    polled_flags = _insert_ordered(polled_flags, True, c_addr_url, [x for x, _ in self._fd_clients_ord])

                    log_tr_net_meta_in.debug('Accept %s %s', c_addr_url, addr)
                except BlockingIOError:
                    break

        flags_clients = list(zip(polled_flags, self._fd_clients_ord))

        for is_ready, (addr, fd) in flags_clients:
            if not is_ready:
                continue

            try:
                for x in recvfrom_helper(fd, self.buffer_size):
                    log_tr_net_raw_in.debug('[%d] %s %s', len(x.data), addr,
                                                               x.data)

                    yield Packet(addr, x.data)
            except ConnectionAbortedError as e:
                if addr not in self._fd_clients:
                    log_tr_net_raw_err.error('%s %s', addr, fd)
                    continue

                self._clean_client_id(addr, str(e))
                # todo should we raise or should we ignore the error ?
                # todo upstream possibly needs to know when a client was disconnected
                raise ConnectionAbortedError(addr, str(e))

    def close(self):
        if self._temp_dir:
            shutil.rmtree(self._temp_dir)
            self._temp_dir = None

        for _, fd in self._fd_clients_ord:
            log_tr_net_meta_in.debug('[1] Close %s', fd)
            fd.close()

        self._fd_clients = {}

        if self._fd_bind:
            log_tr_net_meta_in.debug('[2] Close %s', self._fd_bind)
            self._fd_bind.close()
            self._fd_bind = None


TRANSPORT_MAP: Dict[str, Type[Transport]] = {
    'udp': UDPTransport,
    'unix': UnixTransport,
}

# EVTerminator = Callable[[Any], None]
# EVException = Callable[[Exception], bool]


# @dataclass
# class EVErrEntry:
#    term: EVTerminator
#    exc: EVException


TReq = TypeVar('TReq')
TRep = TypeVar('TRep')


class TransportSerde(Generic[TReq, TRep]):
    def __init__(self, t: Transport, serde: SerdeStruct, cls_req: Type[TReq], cls_rep: Type[TRep]):
        self.transport = t
        self.serde = serde
        self.cls_req = cls_req
        self.cls_rep = cls_rep

    @property
    def fds(self) -> List[socket.socket]:
        return self.transport.fds

    def send(self, to: Origin, obj: TReq):
        pkt_body = json_pack(self.serde.serialize(self.cls_req, obj))

        log_tr_net_obj_out.debug('[%s] %s', self.cls_req, obj)

        pkt = Packet(to, pkt_body)

        self.transport.send(pkt)

    def read(self, polled_flags: Optional[List[bool]] = None) -> Iterable[Tuple[Origin, TRep]]:
        for x in self.transport.read(polled_flags):
            obj = self.serde.deserialize(self.cls_rep, json_unpack(x.data))
            log_tr_net_obj_in.debug('[%s] %s', self.cls_rep, obj)
            yield x.addr, obj


def select_helper(fds, max_wait: Optional[float] = None) -> Optional[List[bool]]:
    log_tr_net_sel_in.debug('%s %s', max_wait, fds)

    try:
        rd, _, er = select.select(fds, [], fds, max_wait)
    except OSError as e:
        log_tr_net_sel_err.debug('%s %s %s', e, max_wait, fds)

        if e.errno == 9:
            raise ConnectionAbortedError(str(e))
        else:
            raise
    except KeyboardInterrupt:
        raise
    except:
        raise ValueError(f'{fds} {max_wait}')

    r = set(rd + er)

    return [fd in r for fd in fds]
