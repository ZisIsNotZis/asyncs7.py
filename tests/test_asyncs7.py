"""End-to-end tests against the synthetic in-process S7 server (demo_s7.FakePLC) on loopback.

No real PLC, device or plant network is involved: the server answers ISO-on-TCP (RFC 1006)
and S7 Read-Var using in-memory data blocks and decodes requests with the client's own struct.

Run:  pytest -q
"""

import asyncio
import sys

import pytest

import asyncs7
from asyncs7 import S7, S7Client, S7Error, S7TimeoutError, MaxVars, item_bytes, parse_read_items
from demo_s7 import FakePLC


@pytest.fixture
async def plc():
    server = await FakePLC().start()
    yield server
    await server.stop()


@pytest.mark.asyncio
async def test_read_single_db(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        assert await s7((1, 0, 16)) == [bytearray(plc.db(1)[0:16])]


@pytest.mark.asyncio
async def test_read_middle_offset(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        assert await s7((7, 100, 32)) == [bytearray(plc.db(7)[100:132])]


@pytest.mark.asyncio
async def test_read_one_returns_bytes_directly(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        assert await s7.read_one(6, 3, 5) == bytearray(plc.db(6)[3:8])


@pytest.mark.asyncio
async def test_read_two_items_in_one_db(plc):
    """Two items of one DB come back as two buffers in request order."""
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        data = await s7((2, 0, 4), (2, 8, 4))
    assert data == [bytearray(plc.db(2)[0:4]), bytearray(plc.db(2)[8:12])]
    assert plc.requests == [(2, 0, 4), (2, 8, 4)]


@pytest.mark.asyncio
async def test_read_two_dbs(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        data = await s7((1, 0, 8), (3, 4, 8))
    assert data == [bytearray(plc.db(1)[0:8]), bytearray(plc.db(3)[4:12])]


@pytest.mark.asyncio
async def test_max_vars_in_one_request(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        data = await s7(*(((1, i, 1) for i in range(MaxVars))))
    assert data == [bytearray([plc.db(1)[i]]) for i in range(MaxVars)]


@pytest.mark.asyncio
async def test_s7_shortcut_still_works(plc):
    s7 = await S7('127.0.0.1', plc.port, timeout=2)
    try:
        assert await s7((5, 2, 6)) == [bytearray(plc.db(5)[2:8])]
    finally:
        s7.close()


@pytest.mark.asyncio
async def test_bit_transport_size(plc):
    """TransportSize 0x04 counts bits; the client must divide by 8."""
    plc.bits = lambda db, adr, sz: sz
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        data = await s7((4, 10, 12))
    assert data == [bytearray(plc.db(4)[10:22])]
    assert item_bytes(0x04, 96) == 12 and item_bytes(0x02, 12) == 12


@pytest.mark.asyncio
async def test_plc_return_code_becomes_error(plc):
    plc.fault = lambda db, adr, sz: 0x05          # "Address out of range"
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        with pytest.raises(S7Error, match='return code 0x05'):
            await s7((9, 0, 4))


@pytest.mark.asyncio
async def test_short_payload_is_an_error(plc):
    """A PLC that answers fewer bytes than requested must not pass silently."""
    plc.bits = lambda db, adr, sz: 1
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        with pytest.raises(S7Error, match='expected 8 bytes, got 1'):
            await s7((4, 0, 8))


@pytest.mark.asyncio
async def test_missing_range_is_an_error(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        with pytest.raises(S7Error, match='return code 0x0A'):
            await s7((1, 65536, 4))


@pytest.mark.asyncio
async def test_concurrent_reads_are_serialized_and_correct(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=5) as s7:
        results = await asyncio.gather(*(s7((i, 0, 8)) for i in range(1, 9)))
    for i, result in enumerate(results, start=1):
        assert result == [bytearray(plc.db(i)[0:8])]
    assert plc.max_in_flight == 1, 'one connection must never have two requests in flight'


@pytest.mark.asyncio
async def test_timeout_is_reported_and_closes(plc):
    plc.delay = 0.3
    s7 = S7Client('127.0.0.1', plc.port, timeout=0.05)
    await s7.connect()
    with pytest.raises(S7TimeoutError):
        await s7((1, 0, 4))
    assert not s7.connected


@pytest.mark.asyncio
async def test_use_after_close_raises(plc):
    s7 = S7Client('127.0.0.1', plc.port, timeout=2)
    await s7.connect()
    s7.close()
    with pytest.raises(S7Error, match='closed'):
        await s7((1, 0, 4))


@pytest.mark.asyncio
async def test_connection_refused_is_not_masked():
    with pytest.raises(OSError):
        await S7Client('127.0.0.1', 1, timeout=0.5).connect()


@pytest.mark.asyncio
async def test_validation_rejects_bad_arguments(plc):
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        with pytest.raises(ValueError, match='at least one'):
            await s7()
        for bad in [(1, 0, 0), (1, -1, 4), (1, 0, 0x10000), (0x10000, 0, 1)]:
            with pytest.raises(ValueError, match='invalid'):
                await s7(bad)
        with pytest.raises(ValueError, match=f'at most {MaxVars}'):
            await s7(*(((1, 0, 1),) * (MaxVars + 1)))


def test_frame_layout_matches_s7_wire_format():
    frame = asyncs7.read_frame(((1, 0, 10), (3, 5, 4)))
    assert int.from_bytes(frame[2:4], 'big') == len(frame) == 43
    assert frame[4:7] == b'\x02\xf0\x80'
    header = asyncs7.S7ReqHeader.from_buffer_copy(frame[7:17])
    assert header.P == 0x32 and header.PDUType == 1 and header.ParLen == 26 and header.DataLen == 0
    assert frame[17] == 0x04 and frame[18] == 2
    first = asyncs7.ReqFunReadItem.from_buffer_copy(frame[19:31])
    assert first.ItemHead[0] == 0x12 and first.Area == 0x84 and first.TransportSize == 0x02
    # Addresses are bit addresses: byte 0 -> 0x000000, byte 5 -> 0x000028.
    assert (first.DBNumber, int.from_bytes(first.Address, 'big'), first.Length) == (1, 0, 10)
    second = asyncs7.ReqFunReadItem.from_buffer_copy(frame[31:43])
    assert (second.DBNumber, int.from_bytes(second.Address, 'big'), second.Length) == (3, 40, 4)
    assert len(asyncs7.negotiate_frame()) == 25
    assert len(asyncs7.connect_frame(0, 1)) == 22


def test_parse_read_items_handles_mixed_transport_sizes():
    data = (bytes([0xFF, 0x04]) + (64).to_bytes(2, 'big') + b'\x01\x02\x03\x04\x05\x06\x07\x08'
            + bytes([0xFF, 0x02]) + (3).to_bytes(2, 'big') + b'abc')
    items = parse_read_items(data, 2)
    assert items[0][:3] == (0xFF, 0x04, 64) and items[0][3] == b'\x01\x02\x03\x04\x05\x06\x07\x08'
    assert items[1][:3] == (0xFF, 0x02, 3) and items[1][3] == b'abc'


def test_parse_read_items_reports_truncation():
    with pytest.raises(S7Error, match='truncated'):
        parse_read_items(b'\xff\x04\x00', 1)


def test_struct_defaults_survive_without_instance_annotations():
    """Regression: on 3.14 a ctypes instance has no __annotations__, which emptied every frame.

    The old code read ``self.__annotations__`` in ``Struct.__init__``, so on 3.14 no default
    was applied and read_frame() failed with "no attribute 'Items'". Assert the defaults
    actually land, not just that sizes match.
    """
    for cls in (asyncs7.Tpkt, asyncs7.CotpCo, asyncs7.ReqFunReadItem, asyncs7.S7ResPDU):
        assert cls._declared_, f'{cls.__name__} lost its declarations'
        assert len(cls._fields_) == len(cls._declared_)
        assert cls._layout_ == 'ms'  # packed layout is explicit, not the deprecated implicit one

    item = asyncs7.ReqFunReadItem()
    assert bytes(item.ItemHead) == b'\x12\x0a\x10'
    assert (item.TransportSize, item.Area) == (0x02, 0x84)

    tpkt = asyncs7.Tpkt()
    assert (tpkt.Version, tpkt.Reserved) == (3, 0)

    pdu = asyncs7.negotiate_frame()
    assert int.from_bytes(pdu[2:4], 'big') == len(pdu) == 25
    assert asyncs7.read_frame(((1, 0, 10), (3, 5, 4)))[2:4] == b'\x00\x2b'  # 43, not padded


async def _run_cli(*args):
    '''Run the CLI in a child process without blocking this loop (the fake PLC must serve).'''
    proc = await asyncio.create_subprocess_exec(
        sys.executable, '-m', 'asyncs7', *map(str, args),
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    return proc.returncode, (stdout + stderr).decode()


async def test_cli_exit_codes_distinguish_plc_error_from_no_server(plc):
    '''The CLI must not blame the network when the PLC itself answers with an error.

    S7Error/S7TimeoutError subclass OSError, so an except-OSError branch placed first would
    swallow them and print the "start the demo server" hint.
    '''
    ok = await _run_cli('127.0.0.1', plc.port, '--db', 1, '--size', 4)
    assert ok[0] == 0, ok
    assert 'DB1[0:4]' in ok[1]

    plc.fault = lambda db, adr, sz: 0x05          # PLC answers, but with an error code
    bad = await _run_cli('127.0.0.1', plc.port, '--db', 1, '--size', 4)
    assert bad[0] == 1, bad
    assert 'S7 error' in bad[1] and 'return code 0x05' in bad[1]
    assert 'start the synthetic server' not in bad[1], 'a PLC error is not a missing server'

    refused = await _run_cli('127.0.0.1', 1)
    assert refused[0] == 2, refused
    assert 'cannot connect' in refused[1]


async def test_read_one_defaults_to_address_zero_size_one(plc):
    """`read_one(db)` is documented as DBn[0:1], the common 'is this DB there?' probe."""
    async with S7Client('127.0.0.1', plc.port, timeout=2) as s7:
        assert await s7.read_one(3) == bytearray(plc.db(3)[0:1])


async def test_two_clients_read_in_parallel(plc):
    """The documented way to get concurrency: one client per stream, gathered."""
    async with S7Client('127.0.0.1', plc.port, timeout=2) as a, \
            S7Client('127.0.0.1', plc.port, timeout=2) as b:
        first, second = await asyncio.gather(a.read_one(1, 0, 16), b.read_one(2, 0, 16))
    assert first == bytearray(plc.db(1)[0:16]) and second == bytearray(plc.db(2)[0:16])
