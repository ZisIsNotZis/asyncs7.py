"""
Usage:
```py
from asyncs7 import S7
s7 = await s7(ip, port, rack, slot)
dat: dict[Annotated[int, 'db'], Annotated[bytearray, 'start from 0']] = await s7(db, adr, sz) # DBRead
```

Note that s7 connection is serial. Therefore please don't try to make another request on the same connection while one request is processing.
"""

from asyncio import open_connection, Task, sleep, new_event_loop
from ctypes import BigEndianStructure, c_ubyte, c_ushort, sizeof, BigEndianUnion, memmove, addressof, Array
from itertools import groupby
from logging import debug
from math import log2
from typing import Annotated, get_origin, get_args, Iterator, Callable, Awaitable

try:
    from uvloop import new_event_loop
except ImportError:
    pass
MaxVars = 20
IsoPayloadSize = 4096


def fields[T](_: T) -> T:
    _._pack_ = 1
    _._fields_ = [(i, get_args(j)[0] if get_origin(j) == Annotated else j) for i, j in _.__annotations__.items()]
    return _


def Str(_) -> str:
    return f'[{','.join(map(str, _))}]' if isinstance(_, Array) else str(_)


def Size(_) -> int:
    return getattr(_, '_size', lambda: sizeof(_))()


class Struct(BigEndianStructure):
    __slots__ = ()

    def __init__(self):
        for i, _ in self.__annotations__.items():
            if get_origin(_) == Annotated:
                setattr(self, i, get_args(_)[1])
            else:
                getattr(self, i).__init__()

    def __iter__(self) -> Iterator:
        return (getattr(self, i) for i, _ in self._fields_)

    def __str__(self):
        return f'{type(self).__name__}({','.join(map(Str, self))})'

    def _size(self) -> int:
        return sum(map(Size, self))


class Union(BigEndianUnion):
    __slots__ = '_i',

    def __init__(self):
        self._i = ''
        getattr(self, next(iter(self.__annotations__))).__init__()

    def __iter__(self) -> Iterator:
        return (getattr(self, i) for i, _ in self._fields_)

    def __setattr__(self, key, value):
        self.key = key
        super().__setattr__(key, value)

    def __str__(self):
        return str(getattr(self, self._i)) if self._i else '|'.join(map(Str, self))

    def size(self) -> int:
        assert self._i
        return getattr(self, self._i)._size()


@fields
class Tpkt(Struct):
    '''TPKT Header - ISO on TCP - RFC 1006 (4 bytes), Packet length : min 7 max 65535'''
    __slots__ = ()
    Version: Annotated[c_ubyte, 3]  # Always 3 for RFC 1006
    Reserved: c_ubyte  # 0
    Lenght: c_ushort  # packet lenght (entire frame, payload and TPDU included)


@fields
class CotpParams(Struct):
    __slots__ = ()
    PduSizeCode: Annotated[c_ubyte, 0xC0]
    PduSizeLen: Annotated[c_ubyte, 1]
    PduSizeVal: Annotated[c_ubyte, int(log2(1024))]  # IsoPDUSize
    TSAP: Annotated[c_ushort * 4, (0xC102,  # code that identifies source TSAP, source TSAP Len
                                   0x0100,  # SrcTSap
                                   0xC202,  # code that identifies dest TSAP, dest TSAP Len
                                   0x0100  # DstTSap, (ConnectionType<<8)+(Rack*0x20)+Slot
                                   )]


@fields
class CotpCo(Struct):
    '''COTP Header for CONNECTION REQUEST/CONFIRM - DISCONNECT REQUEST/CONFIRM'''
    __slots__ = ()
    HLength: Annotated[c_ubyte, 6 + 11]  # Header length : initialized to 6 (length without params - 1)
    PDUType: Annotated[c_ubyte, 0xe0]  # 0xE0 Connection request, 0xD0 Connection confirm, 0x80 Disconnect request, 0xDC Disconnect confirm
    DstRef: c_ushort  # Destination reference, Always 0x0000
    SrcRef: c_ushort  # Source reference, Always 0x0000
    CO_R: c_ubyte  # If the telegram is used for Connection request/Confirm, the meaning of this field is CLASS+OPTION, Class, Always 4 (0100) but is ignored in input (RFC States this), Option, Always 0, also this in ignored.
    Params: CotpParams  # Parameter data : depending on the protocol implementation. ISO 8073 define several type of parameters, but RFC 1006 recognizes only. TSAP related parameters and PDU size.  See RFC 0983 for more details.


@fields
class IsoControlPDU(Struct):
    __slots__ = ()
    TPKT: Annotated[Tpkt, Tpkt(sizeof(CotpCo))]
    COTP: CotpCo  # COPT Header for CONNECTION stuffs

    def __init__(self, rack=0, slot=0):
        super().__init__()
        self.COTP.Params.TSAP[3] = 0x0100 | (rack << 5) | slot


@fields
class CotpDt(Struct):
    '''COTP Header for DATA EXCHANGE'''
    __slots__ = ()
    HLength: Annotated[c_ubyte, 2]  # Header length, 3 for this header
    PDUType: Annotated[c_ubyte, 0xf0]  # 0xF0 for this header
    EoT_Num: Annotated[c_ubyte, 0x80]  # EOT (bit 7) + PDU Number (bits 0..6), EOT 1 -> End of Trasmission Packet (This packet is complete, PDU Number, Always 0


@fields
class ReqFunNegotiateParams(Struct):
    '''FUNCTION NEGOTIATE'''
    __slots__ = ()
    FunNegotiate: Annotated[c_ubyte, 0xF0]  # pduNegotiate, Negotiate PDU length
    Unknown: c_ubyte
    ParallelJobs_1: Annotated[c_ushort, 0x0100]
    ParallelJobs_2: Annotated[c_ushort, 0x0100]
    PDULength: Annotated[c_ushort, 480]  # Our request, FPDULength will contain the CPU answer


@fields
class S7ReqHeader(Struct):
    '''Incoming header, it will be mapped onto IsoPDU payload'''
    __slots__ = ()
    P: Annotated[c_ubyte, 0x32]  # Telegram ID, always 32, Always $32
    PDUType: Annotated[c_ubyte, 1]  # Header type 1 or 7, $01
    AE_XB: c_ushort  # AB currently unknown, maybe it can be used for long numbers, Always $0000
    Sequence: c_ushort  # Message ID. This can be used to make sure a received answer, AutoInc
    ParLen: c_ushort  # Length of parameters which follow this header
    DataLen: c_ushort  # Length of data which follow the parameters


@fields
class ReqFunReadItem(Struct):
    __slots__ = ()
    ItemHead: Annotated[c_ubyte * 3, (0x12, 0x0A, 0x10)]
    TransportSize: Annotated[c_ubyte, 0x02]
    Length: c_ushort
    DBNumber: c_ushort
    Area: Annotated[c_ubyte, 0x84]
    Address: c_ubyte * 3


@fields
class ReqFunReadParams(Struct):
    __slots__ = ()
    FunRead: Annotated[c_ubyte, 0x04]  # pduFuncRead
    ItemsCount: c_ubyte
    Items: ReqFunReadItem * MaxVars

    def __init__(self, *dbAdrSz: tuple[int, int, int]):
        super().__init__()
        self.ItemsCount = len(dbAdrSz)
        for i, ((db, adr, sz), itm) in enumerate(zip(dbAdrSz, self.Item)):
            itm.DBNumber = db
            itm.Address = adr.to_bytes(3, 'big')
            itm.Length = sz

    def _size(self) -> int:
        return 2 + self.ItemsCount * sizeof(ReqFunReadItem)


@fields
class S7ReqUnion(Union):
    __slots__ = ()
    Negotiate: ReqFunNegotiateParams
    Read: ReqFunReadParams


@fields
class S7ReqPDU(Struct):
    __slots__ = ('_i',)
    TPKT: Tpkt
    COTP: CotpDt  # COPT Header for DATA EXCHANGEctypes._endian.c_ushort_be_Array_4
    Header: S7ReqHeader
    Payload: S7ReqUnion

    def __init__(self, *dbAdrSz: tuple[int, int, int]):
        super().__init__()
        self._i = 0
        if dbAdrSz:
            self.Payload.Read = ReqFunReadParams(*dbAdrSz)
        else:
            self.Payload.Negotiate = ReqFunNegotiateParams()
        self.Header.Sequence = type(self)._i = (self._i + 1) & 0xFFFF
        self.Header.ParLen = self.Payload.size()
        self.TPKT = self._size()


@fields
class S7ResHeader(Struct):
    '''Outcoming 12 bytes header , response for Request type 1'''
    __slots__ = ()
    P: c_ubyte  # Telegram ID, always 32
    PDUType: c_ubyte  # Header type 2 or 3
    AB_EX: c_ushort  # AB currently unknown, maybe it can be used for long numbers.
    Sequence: c_ushort  # Message ID. This can be used to make sure a received answer
    ParLen: c_ushort  # Length of parameters which follow this header
    DataLen: c_ushort  # Length of data which follow the parameters
    Error: c_ushort  # Error code


@fields
class ResFunNegotiateParams(Struct):
    '''FUNCTION NEGOTIATE'''
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
class ResFunReadItem(Struct):
    __slots__ = ()
    ReturnCode: c_ubyte
    TransportSize: c_ubyte
    DataLength: c_ushort
    Data: c_ubyte * (IsoPayloadSize - 17)


@fields
class S7ResRead(Struct):
    __slots__ = ()
    Params: ResFunReadParams
    Data: ResFunReadItem * MaxVars


@fields
class S7ResUnion(Union):
    __slots__ = ()
    Negotiate: ResFunNegotiateParams
    Read: S7ResRead


@fields
class S7ResPDU(Struct):
    __slots__ = ()
    TPKT: Tpkt
    COTP: CotpDt  # COPT Header for DATA EXCHANGE
    Header: S7ResHeader
    Payload: S7ResUnion


async def S7(ip='127.0.0.1', port=102, rack=0, slot=0) -> Callable[[tuple[int, int, int]], Awaitable[dict[int, bytearray]]]:
    reader, writer = await open_connection(ip, port)

    def write(a):
        debug('<<< %s', a)
        writer.write(bytearray(a)[:a.TPKT.Lenght])

    async def read[T](reader, a: T) -> T:
        memmove(addressof(a.TPKT), await reader.readexactly(sizeof(Tpkt)), sizeof(Tpkt))
        memmove(addressof(a.TPKT) + sizeof(Tpkt), await reader.readexactly(a.TPKT.Lenght - sizeof(Tpkt)), a.TPKT.Lenght - sizeof(Tpkt))
        debug('>>> %s', a)
        return a

    write(IsoControlPDU(rack, slot))
    await read(reader, IsoControlPDU.__new__(IsoControlPDU))
    write(S7ReqPDU())
    debug('pdu %d', (await read(reader, S7ResPDU.__new__(S7ResPDU))).Payload.Negotiate.PDULength)

    async def _(*dbAdrSz: tuple[int, int, int]) -> dict[int, bytearray]:
        write(S7ReqPDU(*dbAdrSz))
        res = await read(reader, S7ResPDU.__new__(S7ResPDU))
        buf = {db: bytearray(max(_[2] for _ in _)) for db, _ in groupby(dbAdrSz, lambda _: _[0])}
        for (db, i, j), itm in zip(dbAdrSz, res.Payload.Read.Data[:res.Payload.Read.Params.ItemCount]):
            itm: ResFunReadItem
            assert itm.ReturnCode == 0xFF
            buf[db][i:j] = itm.Data[:itm.DataLength]
        return buf

    return _


async def bench(*ipPortRackSlotDbAdrSz):
    _ = [0] * len(ipPortRackSlotDbAdrSz)
    for i, (ip, port, rack, slot, db, adr, sz) in enumerate(ipPortRackSlotDbAdrSz):
        @lambda _: Task(_())
        async def _(i=i, ip=ip, port=port, rack=rack, slot=slot, db=db, adr=adr, sz=sz):
            s7 = await S7(ip, port, rack, slot)
            while True:
                await s7((db, adr, sz))
                _[i] += 1
    while True:
        await sleep(1)
        print(_)
        _[:] = (0,) * len(ipPortRackSlotDbAdrSz)


if __name__ == '__main__':
    new_event_loop().run_until_complete(bench(('127.0.0.1', 1102, 0, 0, 0, 0, 100)))
