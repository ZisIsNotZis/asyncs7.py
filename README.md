# asyncs7

**Status: completed for the documented read-only scope (0.3.1).** See [verification and limits](COMPLETION.md).

A small, pure-Python `asyncio` client that reads data blocks (DB) from Siemens S7 PLCs
over ISO-on-TCP (RFC 1006, TCP port 102). No third-party dependencies; Python 3.11+.

The port is deliberately narrow: **read-only, DB area, byte-addressed ranges.** See
[Supported / not supported](#supported--not-supported) before you point it at hardware.

## Install

```bash
pip install .            # from a checkout
pip install dist/asyncs7-0.3.1-py3-none-any.whl   # or from the built wheel
```

That gives you the `asyncs7` module, the `asyncs7` command, and the `demo_s7` module
used for the local loopback demo below.

## Quick start

```python
import asyncio
from asyncs7 import S7Client

async def main():
    # ip, port=102 (or 1102 for the demo server), rack=0, slot=1 for S7-1200/1500
    async with S7Client('192.168.0.1', 102, rack=0, slot=1) as s7:
        data = await s7.read_one(1, 0, 10)          # DB1 bytes 0..9 as a bytearray
        print(len(data), data.hex())

asyncio.run(main())
```

`await S7(ip, port, rack, slot)` is a shortcut that constructs and connects in one line;
call `s7.close()` yourself when you use it.

### Reading several ranges in one request

```python
async with S7Client(ip, 102, rack=0, slot=1) as s7:
    a, b = await s7.read((1, 0, 16), (3, 100, 4))     # one Read-Var frame, up to 20 items
    # b is a bytearray of DB3[100:104]
```

Results come back as a `list` of `bytearray`, **in the order of the request**, not keyed by
DB number. `s7(...)` is an alias for `s7.read(...)`:

```python
blocks = await s7((1, 0, 16), (1, 20, 4))            # [bytearray, bytearray]
assert len(blocks) == 2 and len(blocks[0]) == 16
```

Each item is `(db, adr, sz)` — DB number, starting **byte** offset, byte count — with
`0 <= db <= 0xFFFF`, `0 <= adr <= 0xFFFFFF`, `1 <= sz <= 0xFFFF`. The client turns `adr`
into the S7 bit address `adr * 8` on the wire, so every request is byte-aligned.

> Breaking change vs. 0.1.x: `s7(...)` used to return a `dict` keyed by DB number.
> It now returns a list in request order, because two ranges of one DB collide as dict keys.

### Constructor

```python
S7Client(ip='127.0.0.1', port=102, rack=0, slot=0, timeout=5.0)
```

| Parameter | Meaning |
|-----------|---------|
| `ip`, `port` | PLC address; port 102 is the standard ISO-on-TCP port |
| `rack`, `slot` | CPU slot selection, used in the COTP TSAP |
| `timeout` | Seconds for one complete answer (all requests, including connect) |

### Connection and lifecycle

```python
async with S7Client(...) as s7:   # connect: TCP -> COTP -> PDU negotiation
    ...

s7 = await S7Client(...).connect()   # same thing, manual
s7.close()                           # idempotent, never raises
s7.connected                         # False once closed or after a timeout closed it
```

Connecting raises `OSError` (e.g. `ConnectionRefusedError`) if the TCP connect fails, and
`S7Error` if the PLC answers with something that is not a valid handshake.
A timeout or a short read closes the socket and raises `S7TimeoutError`; there is no
automatic reconnect — reconnect explicitly, which is usually what you want after a fault.

### Errors

| Exception | When |
|-----------|------|
| `S7Error(ConnectionError)` | PLC answered, but the answer is wrong: bad return code, truncated frame, item count mismatch, short payload, unexpected function, or use after `close()` |
| `S7TimeoutError(TimeoutError)` | No complete frame within `timeout`; socket has been closed |
| `ValueError` | Bad local arguments: no items, > 20 items, out-of-range `db`/`adr`/`sz`, or a frame larger than the negotiated PDU |
| `OSError` | TCP-level failure, including a refused/absent listener |

Nothing is retried or swallowed: if the PLC says "address out of range" the call raises
`S7Error` instead of returning short data.

## Concurrency

**One connection is serial.** The client holds a lock per request, so many tasks can share
one client safely — they queue — but throughput stays one round trip at a time
(`MaxVars = 20` items per trip, up to the negotiated PDU length). For parallel streams open
several clients:

```python
async with S7Client(ip, 102, rack=0, slot=1) as a, S7Client(ip, 102, rack=0, slot=1) as b:
    first, second = await asyncio.gather(a.read_one(1, 0, 16), b.read_one(2, 0, 16))
```

## Supported / not supported

Supported and covered by tests:

- TCP connect, COTP connect for a given rack/slot, S7 PDU-size negotiation (480 default,
  the PLC's answer is honored as the frame ceiling).
- Read-Var on the **DB area only**, byte-aligned ranges, 1–20 items per request.
- Return-code checking, payload-length checking, timeouts, serialized requests,
  clean close, argument validation.

Not supported — the client will refuse or simply not do it:

- Writes, bit/`sz`-in-bits addressing, symbolic names, MK/Merker, inputs/outputs,
  timers, counters, or any area other than DB.
- Multi-frame splitting: one request must fit in the negotiated PDU, otherwise you get a
  `ValueError` telling you to split it yourself.
- Automatic reconnect, retries, keepalive, or connection pooling.
- Authentication, TLS, PROFINET discovery, or routing through a gateway.
- Coverage of every CPU family. Only the S7-1200/1500 style handshake plus DBRead is
  modeled; S7-300/400 behavior is untested and may need different TSAP/PDU handling.

It has **not** been run against a real PLC in this repository. Everything here was verified
against a synthetic server on loopback (`demo_s7.py`), which is also what the tests use, so
the tests prove the frame format the demo serves — not a vendor device. Treat the first
hardware run as a validation step: start with `read_one(1, 0, 16)` on a scratch DB.

## Local demo (no hardware, no network)

Two terminals:

```bash
python demo_s7.py --port 1102          # synthetic S7 server, DB 1.. of 64 KiB
python asyncs7.py 127.0.0.1 1102 --db 1 --size 16
```

The demo server generates a deterministic pattern — DB N, byte i is `(i * 7 + N) & 0xFF` —
so DB1 byte 0 is `0x01` and DB2 byte 5 is `0x25`. You should see:

```
connected to 127.0.0.1:1102, PDU length 480
DB1[0:16] = 01080f161d242b323940474e555c636a
```

Without a server the CLI prints a hint and exits `2`; S7-level errors exit `1`.

```bash
python asyncs7.py 127.0.0.1 1102 --bench 1 --size 100     # loopback throughput
```

Throughput here measures the demo server plus loopback, not PLC latency; it is useful for
catching accidental per-read overhead (handshake, allocation), not for capacity planning.

## Tests

```bash
pip install -e '.[test]' && pytest -q     # 23 passed
```

The suite starts `FakePLC` on an ephemeral loopback port and covers: single and mid-offset
reads, multi-item and multi-DB reads, the 20-item limit, bit-count transport size, PLC
return-code errors, short payloads, missing ranges, request serialization under
`asyncio.gather`, timeouts, use-after-close, refused connections, argument validation,
byte-exact frame layout (TPKT/S7 headers, bit addresses, declared lengths), and the struct
factory keeping its annotated defaults on Python 3.14 (where instances no longer expose
`__annotations__`).

Verified on CPython 3.11.18 and 3.14.4, each from a wheel installed into a clean venv, with
the suite run under `-W error::DeprecationWarning`.

No real PLC, device, or plant network is contacted by install, tests, demo, or the CLI.

## License

AGPL-3.0-only. See [LICENSE](LICENSE).
