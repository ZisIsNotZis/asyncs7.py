"""
A synthetic S7 server for local demos and tests. Nothing here talks to real hardware.

It speaks just enough ISO-on-TCP (RFC 1006) and S7 Read-Var to stand in for a PLC with
in-memory data blocks whose bytes are a deterministic pattern: DB N, byte i is
``(i * 7 + N) & 0xFF``.

```bash
python demo_s7.py --port 1102        # then: python asyncs7.py 1102
```

The test suite imports ``FakePLC`` from here, so the wire format proven by the tests is the
same one the demo exercises.
"""

import argparse
import asyncio
import contextlib

import asyncs7

CONFIRM = (b'\x03\x00\x00\x16\x0d\xd0\x00\x01\x00\x00\x00'
           b'\xc0\x01\x0a\xc1\x02\x01\x00\xc2\x02\x01\x02')


def s7_header(pdu_type: int, par_len: int, data_len: int, error: int = 0) -> bytes:
    return (bytes([0x32, pdu_type, 0, 0, 0, 0])
            + par_len.to_bytes(2, 'big') + data_len.to_bytes(2, 'big') + error.to_bytes(2, 'big'))


def frame(body: bytes) -> bytes:
    '''Wrap a COTP-DT + S7 body in a TPKT header carrying the correct total length.'''
    return bytes([3, 0]) + (4 + len(body)).to_bytes(2, 'big') + body


def answer(params: bytes, data: bytes) -> bytes:
    '''A complete S7 answer frame: TPKT + COTP-DT + S7 header + params + data.'''
    return frame(b'\x02\xf0\x80' + s7_header(3, len(params), len(data)) + params + data)


class FakePLC:
    '''Minimal S7 server: COTP connect, PDU negotiation, Read-Var over in-memory DBs.'''

    def __init__(self, db_size: int = 65536, pdu_length: int = 480, delay: float = 0.0):
        self.db_size = db_size
        self.pdu_length = pdu_length
        self.delay = delay
        self.dbs = {}                    # db number -> bytearray
        self.requests = []               # (db, adr, sz) triplets decoded from real frames
        self.in_flight = 0
        self.max_in_flight = 0
        self.fault = None                # optional (db, adr, sz) -> return code override
        self.bits = None                 # optional (db, adr, sz) -> bytes sent, announced in bits
        self.server = None
        self.port = None

    def db(self, number: int) -> bytearray:
        if number not in self.dbs:
            self.dbs[number] = bytearray((i * 7 + number) & 0xFF for i in range(self.db_size))
        return self.dbs[number]

    async def start(self, host: str = '127.0.0.1', port: int = 0):
        self.server = await asyncio.start_server(self._handle, host, port)
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        self.server.close()
        with contextlib.suppress(Exception):
            await self.server.wait_closed()

    async def serve_forever(self, host: str = '127.0.0.1', port: int = 1102):
        self.server = await asyncio.start_server(self._handle, host, port)
        self.port = self.server.sockets[0].getsockname()[1]
        async with self.server:
            await self.server.serve_forever()

    async def _read_frame(self, reader) -> bytes:
        head = await reader.readexactly(4)
        length = int.from_bytes(head[2:4], 'big')
        return head + await reader.readexactly(length - 4)

    async def _handle(self, reader, writer):
        try:
            await self._read_frame(reader)                       # COTP connection request
            writer.write(CONFIRM)
            await writer.drain()
            await self._read_frame(reader)                       # S7 PDU negotiation
            writer.write(answer(bytes([0xF0, 0x00, 0x01, 0x00, 0x01, 0x00])
                                + self.pdu_length.to_bytes(2, 'big'), b''))
            await writer.drain()
            while True:
                request = await self._read_frame(reader)
                self.in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self.in_flight)
                try:
                    if self.delay:
                        await asyncio.sleep(self.delay)
                    writer.write(self._read_answer(request))
                    await writer.drain()
                finally:
                    self.in_flight -= 1
        except (asyncio.IncompleteReadError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            writer.close()

    def decode_items(self, request: bytes):
        '''Decode Read-Var items with the client's own struct, like a PLC parser would.'''
        base = 4 + 3 + 10 + 2
        items = []
        for i in range(request[base - 1]):
            item = asyncs7.ReqFunReadItem.from_buffer_copy(
                request[base + i * 12:base + i * 12 + 12])
            assert item.ItemHead[0] == 0x12 and item.Area == 0x84 and item.TransportSize == 0x02
            bits = int.from_bytes(item.Address, 'big')
            assert bits % 8 == 0, 'byte reads must be byte-aligned in the bit address'
            items.append((item.DBNumber, bits // 8, item.Length))
        return items

    def _read_answer(self, request: bytes) -> bytes:
        items = self.decode_items(request)
        self.requests.extend(items)
        data = bytearray()
        for db, adr, sz in items:
            rc = 0xFF if self.fault is None else self.fault(db, adr, sz)
            if rc != 0xFF:
                data += bytes([rc, 0x00, 0x00, 0x00])
                continue
            chunk = self.db(db)[adr:adr + sz]
            if len(chunk) != sz:
                data += bytes([0x0A, 0x00, 0x00, 0x00])          # object does not exist
                continue
            if self.bits is not None and self.bits(db, adr, sz):
                n = self.bits(db, adr, sz)                        # send n bytes, announce bits
                data += bytes([0xFF, 0x04]) + (n * 8).to_bytes(2, 'big') + bytes(chunk[:n])
            else:
                data += bytes([0xFF, 0x04]) + (sz * 8).to_bytes(2, 'big') + bytes(chunk)
        return answer(bytes([0x04, len(items)]), bytes(data))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog='demo_s7.py',
        description='Synthetic S7 server on loopback. No real PLC, device or plant network is used.')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=1102, help='port to serve on (default 1102)')
    parser.add_argument('--db-size', type=int, default=65536, help='bytes per data block')
    args = parser.parse_args(argv)

    print(f'synthetic S7 server on {args.host}:{args.port} '
          f'(DB 1..n, {args.db_size} bytes, Ctrl-C to stop)', flush=True)
    try:
        asyncio.run(FakePLC(db_size=args.db_size).serve_forever(args.host, args.port))
    except KeyboardInterrupt:
        return 0
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
