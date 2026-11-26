"""
A small pure-asyncio client for reading data blocks (DB) from Siemens S7 PLCs over
ISO-on-TCP (RFC 1006, TCP port 102).

Supported: TCP connect, COTP connect, S7 PDU-size negotiation, Read-Var on standard DB
areas with `(db, byte_address, size)` items, up to `MaxVars` items per request, request
serialization on one connection, timeouts, and explicit errors for PLC return codes.

Not supported: writes, symbolic addressing, other protocol areas (only DB), automatic
reconnect, or parallel requests on one connection (the connection is serial by protocol).

```py
from asyncs7 import S7Client

async with S7Client('192.168.0.1', rack=0, slot=1) as s7:
    blocks = await s7((1, 0, 10), (1, 20, 4))   # [bytearray, bytearray], in request order
    data = await s7.read_one(1, 0, 10)          # just the bytes of DB1[0:10]
```

Or without a context manager: ``s7 = await S7('192.168.0.1', 102, 0, 1)``; then ``s7.close()``.

The S7 connection is serial: requests on one client wait on a lock, so several tasks may
share one client, but throughput stays one request at a time. Use one client per stream when
you need parallel connections.

Requires Python 3.11+; a wheel was built and imported from clean 3.11 and 3.14 installs.
"""

from asyncio import Lock, IncompleteReadError, run, timeout, open_connection
from ctypes import BigEndianStructure, c_ubyte, c_ushort, sizeof, memmove, addressof, Array
from logging import debug
from math import log2
from typing import Annotated, get_origin, get_args, Iterator

MaxVars = 20
__version__ = '0.3.1'


def fields(cls):
    '''Build the ctypes layout from the Annotated annotations declared on a Struct subclass.

    The annotations are copied onto the class as ``_declared_``: since Python 3.14 a ctypes
    structure instance no longer exposes ``__annotations__`` (it must come from the type).
    '''
    cls._pack_ = 1
    cls._layout_ = 'ms'  # explicit now that the implicit _pack_=1 layout is deprecated
    # PEP 649 (3.14+) keeps annotations out of cls.__dict__ until first access; reading the
    # attribute triggers evaluation and caches it, while 3.11 stores the dict directly.
    ann = cls.__dict__.get('__annotations__') or getattr(cls, '__annotations__', {})
    cls._declared_ = dict(ann)
    cls._fields_ = [(i, get_args(j)[0] if get_origin(j) == Annotated else j)
                    for i, j in cls._declared_.items()]
    return cls


def Str(_) -> str:
    return '[' + ','.join(map(str, _)) + ']' if isinstance(_, Array) else str(_)


class Struct(BigEndianStructure):
    '''Big-endian, byte-packed wire structure: default values come from the annotations.'''

    __slots__ = ()
    _layout_ = 'ms'  # byte-packed MSVC layout; fields() sets it per class too

    def __init__(self):
        for i, _ in type(self)._declared_.items():
            if get_origin(_) == Annotated:
                setattr(self, i, get_args(_)[1])
            else:
                getattr(self, i).__init__()

    def __iter__(self) -> Iterator:
        return (getattr(self, i) for i, _ in self._fields_)

    def __str__(self):
        return type(self).__name__ + '(' + ','.join(map(Str, self)) + ')'

    def __bytes__(self) -> bytes:
        return bytes(memoryview(self))


@fields
class Tpkt(Struct):
    '''TPKT header, ISO on TCP, RFC 1006 (4 bytes): version, reserved, total frame length.'''
    __slots__ = ()
    Version: Annotated[c_ubyte, 3]
    Reserved: c_ubyte
    Lenght: c_ushort  # spelling kept from the original protocol tables


@fields
class CotpParams(Struct):
    __slots__ = ()
    PduSizeCode: Annotated[c_ubyte, 0xC0]
    PduSizeLen: Annotated[c_ubyte, 1]
    PduSizeVal: Annotated[c_ubyte, int(log2(1024))]
    TSAP: Annotated[c_ushort * 4, (0xC102, 0x0100, 0xC202, 0x0100)]


@fields
class CotpCo(Struct):
    '''COTP connection request/confirm header.'''
    __slots__ = ()
    HLength: Annotated[c_ubyte, 6 + 11]
    PDUType: Annotated[c_ubyte, 0xe0]
    DstRef: c_ushort
    SrcRef: c_ushort
    CO_R: c_ubyte
    Params: CotpParams


@fields
class CotpDt(Struct):
    '''COTP data-transfer header.'''
    __slots__ = ()
    HLength: Annotated[c_ubyte, 2]
    PDUType: Annotated[c_ubyte, 0xf0]
    EoT_Num: Annotated[c_ubyte, 0x80]


def connect_frame(rack: int = 0, slot: int = 0) -> bytes:
    '''COTP connection request for the given rack/slot, with a valid TPKT length.'''

    @fields
    class PDU(Struct):
        TPKT: Tpkt
        COTP: CotpCo

    pdu = PDU()
    pdu.COTP.Params.TSAP[3] = 0x0100 | (rack << 5) | slot
    pdu.TPKT.Lenght = sizeof(PDU)
    return bytes(pdu)


@fields
class S7ReqHeader(Struct):
    __slots__ = ()
    P: Annotated[c_ubyte, 0x32]  # always 0x32
    PDUType: Annotated[c_ubyte, 1]
    AE_XB: c_ushort
    Sequence: c_ushort
    ParLen: c_ushort
    DataLen: c_ushort


@fields
class ReqFunNegotiateParams(Struct):
    '''S7 PDU-size negotiation request.'''
    __slots__ = ()
    FunNegotiate: Annotated[c_ubyte, 0xF0]
    Unknown: c_ubyte
    ParallelJobs_1: Annotated[c_ushort, 0x0100]
    ParallelJobs_2: Annotated[c_ushort, 0x0100]
    PDULength: Annotated[c_ushort, 480]

    def _size(self) -> int:
        return 8


@fields
class ReqFunReadItem(Struct):
    __slots__ = ()
    ItemHead: Annotated[c_ubyte * 3, (0x12, 0x0A, 0x10)]
    TransportSize: Annotated[c_ubyte, 0x02]  # 0x02 = byte
    Length: c_ushort
    DBNumber: c_ushort
    Area: Annotated[c_ubyte, 0x84]  # 0x84 = data block
    Address: c_ubyte * 3


ITEM_SIZE = sizeof(ReqFunReadItem)  # 12 bytes


def _sized(pdu: Struct, frame_size: int, par_len: int, seq: int, data_len: int = 0) -> bytes:
    '''Set the TPKT length and S7 header counters, then return the frame without array padding.'''
    pdu.TPKT.Lenght = frame_size
    pdu.Header.Sequence = seq
    pdu.Header.ParLen = par_len
    pdu.Header.DataLen = data_len
    return bytes(pdu)[:frame_size]


HEAD_SIZE = sizeof(Tpkt) + sizeof(CotpDt) + sizeof(S7ReqHeader)  # 4 + 3 + 10 = 17


def negotiate_frame(sequence: int = 0) -> bytes:
    '''S7 negotiate-PDU request (asks the PLC for its PDU length).'''

    @fields
    class PDU(Struct):
        TPKT: Tpkt
        COTP: CotpDt
        Header: S7ReqHeader
        Params: ReqFunNegotiateParams

    pdu = PDU()
    return _sized(pdu, sizeof(PDU), sizeof(ReqFunNegotiateParams), sequence)


def read_frame(items, sequence: int = 0) -> bytes:
    '''S7 Read-Var request for ``(db, adr, sz)`` items (at most ``MaxVars`` per frame).'''

    @fields
    class PDU(Struct):
        TPKT: Tpkt
        COTP: CotpDt
        Header: S7ReqHeader
        FunRead: Annotated[c_ubyte, 0x04]
        ItemsCount: c_ubyte
        Items: ReqFunReadItem * MaxVars

    pdu = PDU()
    pdu.ItemsCount = len(items)
    for (db, adr, sz), itm in zip(items, pdu.Items):
        # Array elements never run ReqFunReadItem.__init__, so spell out its constants here.
        for k, b in enumerate((0x12, 0x0A, 0x10)):
            itm.ItemHead[k] = b
        itm.TransportSize, itm.Area = 0x02, 0x84
        itm.DBNumber = db
        # S7 addresses are in bits: a byte offset `adr` is bit address `adr * 8`, bit 0.
        for k, b in enumerate((adr * 8).to_bytes(3, 'big')):
            itm.Address[k] = b
        itm.Length = sz
    return _sized(pdu, HEAD_SIZE + 2 + len(items) * ITEM_SIZE, 2 + len(items) * ITEM_SIZE, sequence)


@fields
class S7ResHeader(Struct):
    __slots__ = ()
    P: c_ubyte
    PDUType: c_ubyte
    AB_EX: c_ushort
    Sequence: c_ushort
    ParLen: c_ushort
    DataLen: c_ushort
    Error: c_ushort


@fields
class ResFunNegotiateParams(Struct):
    __slots__ = ()
    FunNegotiate: c_ubyte
    Unknown: c_ubyte
    ParallelJobs_1: c_ushort
    ParallelJobs_2: c_ushort
    PDULength: c_ushort


@fields
class ResFunReadParams(Struct):
    __slots__ = ()
    FunRead: c_ubyte
    ItemCount: c_ubyte


@fields
class S7ResPDU(Struct):
    '''Fixed head of an answer frame: TPKT, COTP and the 12-byte S7 response header.'''
    __slots__ = ()
    TPKT: Tpkt
    COTP: CotpDt
    Header: S7ResHeader


RES_HEAD_SIZE = sizeof(S7ResPDU)  # 19 bytes


class S7Error(ConnectionError):
    '''The PLC answered, but the answer is an error (return code, malformed or unexpected frame).'''


class S7TimeoutError(TimeoutError):
    '''No complete answer arrived before the timeout; the connection has been closed.'''


def item_bytes(transport_size: int, data_length: int) -> int:
    '''Payload length of a read item: transport size 0x04 (BIT) counts bits, others count bytes.'''
    return data_length // 8 if transport_size == 0x04 else data_length


def parse_read_items(data: bytes, item_count: int) -> list[tuple[int, int, int, bytes]]:
    '''Split a read-response data section into (return_code, transport_size, data_length, payload).'''
    items = []
    pos = 0
    for _ in range(item_count):
        if pos + 4 > len(data):
            raise S7Error(f'read response truncated after {pos} bytes: item head incomplete')
        rc, ts = data[pos], data[pos + 1]
        dl = int.from_bytes(data[pos + 2:pos + 4], 'big')
        pos += 4
        n = item_bytes(ts, dl)
        items.append((rc, ts, dl, data[pos:pos + n]))
        pos += n
    return items


class S7Client:
    '''One S7 connection: `async with S7Client(ip, rack=..., slot=...)` or `await client.connect()`.'''

    def __init__(self, ip: str = '127.0.0.1', port: int = 102, rack: int = 0, slot: int = 0,
                 timeout: float = 5.0):
        self.ip, self.port, self.rack, self.slot = ip, port, rack, slot
        self.timeout = timeout
        self.reader = self.writer = None
        self.pdu_length = 480  # PLC's answer to the negotiate request; also the frame ceiling
        self.sequence = 0
        self._lock = Lock()

    @property
    def connected(self) -> bool:
        return self.writer is not None and not self.writer.is_closing()

    async def __aenter__(self) -> 'S7Client':
        return await self.connect()

    async def __aexit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
        self.reader = self.writer = None

    def _next_sequence(self) -> int:
        self.sequence = (self.sequence + 1) & 0xFFFF
        return self.sequence

    def _send(self, frame: bytes) -> None:
        debug('<<< %s', frame.hex())
        self.writer.write(frame)

    async def _recv(self) -> bytes:
        '''Read exactly one TPKT frame from the socket.'''
        try:
            async with timeout(self.timeout):
                head = await self.reader.readexactly(sizeof(Tpkt))
                length = int.from_bytes(head[2:4], 'big')
                if not sizeof(Tpkt) <= length <= 0xFFFF:
                    raise S7Error(f'bad TPKT length {length}')
                return head + await self.reader.readexactly(length - sizeof(Tpkt))
        except (TimeoutError, IncompleteReadError) as e:
            self.close()
            raise S7TimeoutError(
                f'no complete answer from {self.ip}:{self.port} within {self.timeout}s') from e

    async def connect(self) -> 'S7Client':
        '''TCP connect, COTP connect and S7 PDU-size negotiation.'''
        self.close()
        self.reader, self.writer = await open_connection(self.ip, self.port)
        try:
            self._send(connect_frame(self.rack, self.slot))
            await self._recv()
            self._send(negotiate_frame(self._next_sequence()))
            body = await self._recv()
            pdu = S7ResPDU.__new__(S7ResPDU)
            memmove(addressof(pdu), body, min(len(body), sizeof(pdu)))
            if pdu.Header.P != 0x32:
                raise S7Error(f'negotiation answer is not an S7 frame (id 0x{pdu.Header.P:02X})')
            params = ResFunNegotiateParams.__new__(ResFunNegotiateParams)
            memmove(addressof(params), body[RES_HEAD_SIZE:], sizeof(params))
            self.pdu_length = params.PDULength or 480
            debug('pdu %d', self.pdu_length)
        except BaseException:
            self.close()
            raise
        return self

    async def read(self, *db_adr_sz: tuple[int, int, int]) -> list[bytearray]:
        '''Read (db, adr, sz) items; one bytearray per item, in request order.'''
        items = tuple(db_adr_sz)
        if not items:
            raise ValueError('give at least one (db, adr, sz) triple')
        if len(items) > MaxVars:
            raise ValueError(f'at most {MaxVars} items per request, got {len(items)}')
        for db, adr, sz in items:
            if not (0 <= db <= 0xFFFF and 0 <= adr <= 0xFFFFFF and 1 <= sz <= 0xFFFF):
                raise ValueError(f'invalid (db={db}, adr={adr}, sz={sz})')
        if not self.connected:
            raise S7Error('connection is closed')
        frame = read_frame(items, self._next_sequence())
        if len(frame) > self.pdu_length:
            raise ValueError(f'request of {len(frame)} bytes exceeds the negotiated '
                             f'PDU size {self.pdu_length}; split it into fewer/smaller items')
        # The S7 connection is serial: one request in flight, other callers wait here.
        async with self._lock:
            self._send(frame)
            body = await self._recv()
        res = S7ResPDU.__new__(S7ResPDU)
        memmove(addressof(res), body, min(len(body), sizeof(res)))
        h = res.Header
        if h.P != 0x32:
            raise S7Error(f'not an S7 frame (id 0x{h.P:02X})')
        if h.PDUType not in (2, 3):
            raise S7Error(f'unexpected PDU type 0x{h.PDUType:02X}')
        params = ResFunReadParams.__new__(ResFunReadParams)
        memmove(addressof(params), body[RES_HEAD_SIZE:], sizeof(params))
        if params.FunRead != 0x04:
            raise S7Error(f'answer is not a read response (function 0x{params.FunRead:02X})')
        if params.ItemCount != len(items):
            raise S7Error(f'asked for {len(items)} items, PLC answered {params.ItemCount}')
        parsed = parse_read_items(
            body[RES_HEAD_SIZE + h.ParLen:RES_HEAD_SIZE + h.ParLen + h.DataLen], params.ItemCount)
        results = []
        for (db, adr, sz), (rc, ts, dl, payload) in zip(items, parsed):
            if rc != 0xFF:
                raise S7Error(f'read DB{db}@{adr}:{sz} failed with return code 0x{rc:02X}')
            if len(payload) != sz:
                raise S7Error(f'read DB{db}@{adr}:{sz}: expected {sz} bytes, got {len(payload)} '
                              f'(transport size 0x{ts:02X}, length {dl})')
            results.append(bytearray(payload))
        return results

    __call__ = read

    async def read_one(self, db: int, adr: int = 0, sz: int = 1) -> bytearray:
        '''Read a single range and return its bytes directly.'''
        return (await self.read((db, adr, sz)))[0]


async def S7(*args, **kwargs) -> 'S7Client':
    '''Shortcut: `s7 = await S7(ip, port, rack, slot)` returns a connected client.'''
    return await S7Client(*args, **kwargs).connect()


def main(argv=None) -> int:
    '''Demo entry point: read blocks from a PLC (or the synthetic server) and print them.'''
    import argparse
    import asyncio as aio

    parser = argparse.ArgumentParser(
        prog='asyncs7', description='Read DB blocks from an S7 PLC. Demo by default.')
    parser.add_argument('host', nargs='?', default='127.0.0.1')
    parser.add_argument('port', nargs='?', type=int, default=1102)
    parser.add_argument('--rack', type=int, default=0)
    parser.add_argument('--slot', type=int, default=0)
    parser.add_argument('--timeout', type=float, default=5.0)
    parser.add_argument('--db', type=int, default=1, help='data block to read (default 1)')
    parser.add_argument('--adr', type=int, default=0, help='byte offset (default 0)')
    parser.add_argument('--size', type=int, default=16, help='bytes to read (default 16)')
    parser.add_argument('--bench', type=float, metavar='SECONDS',
                        help='measure reads per second for this many seconds instead')
    args = parser.parse_args(argv)

    async def run() -> int:
        client = S7Client(args.host, args.port, args.rack, args.slot, args.timeout)
        try:
            async with client:
                print(f'connected to {args.host}:{args.port}, PDU length {client.pdu_length}')
                if args.bench:
                    count, deadline = 0, aio.get_running_loop().time() + args.bench
                    while aio.get_running_loop().time() < deadline:
                        await client.read_one(args.db, args.adr, args.size)
                        count += 1
                    print(f'{count / args.bench:.0f} reads/s ({args.size} bytes from DB{args.db})')
                    return 0
                data = await client.read_one(args.db, args.adr, args.size)
                print(f'DB{args.db}[{args.adr}:{args.adr + len(data)}] = {data.hex()}')
                return 0
        # S7Error and S7TimeoutError both subclass OSError (ConnectionError/TimeoutError),
        # so they must come first or a PLC error would be reported as "cannot connect".
        except S7TimeoutError as e:
            print(f'timeout: {e}')
            return 1
        except S7Error as e:
            print(f'S7 error: {e}')
            return 1
        except OSError as e:
            print(f'cannot connect to {args.host}:{args.port}: {e}')
            print('for a local demo, start the synthetic server first: python demo_s7.py')
            return 2

    try:
        return aio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
