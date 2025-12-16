# asyncs7
A lightweight, async Python library for communicating with Siemens S7 PLCs over Ethernet/IP. This library provides an asynchronous interface for reading data blocks (DBRead) from S7 PLCs, with a focus on simplicity and proper asyncio usage.

## Features
- Pure async/await interface (compatible with asyncio)
- Simple DBRead operation for reading data blocks
- Serial connection handling (prevents concurrent request conflicts)
- Minimal dependencies (designed for async Python environments)

## Installation
```bash
pip install .  # Replace with actual installation method if available
```

## Quick Start
### Basic DBRead Example
```python
import asyncio
from asyncs7 import S7
from typing import Annotated

async def read_s7_plc():
    # Initialize S7 connection (IP, Port, Rack, Slot)
    s7 = await S7(ip="192.168.0.1", port=102, rack=0, slot=1)
    
    # Read data block: DB number, starting address (0-based), size (bytes)
    # Returns dict with DB number and bytearray of read data
    data: dict[Annotated[int, 'db'], Annotated[bytearray, 'start from 0']] = await s7(db=1, adr=0, sz=10)
    
    # Process the data
    db_number = list(data.keys())[0]
    raw_data = data[db_number]
    print(f"Read {len(raw_data)} bytes from DB{db_number} starting at address 0")
    print(f"Raw data: {raw_data.hex()}")

if __name__ == "__main__":
    asyncio.run(read_s7_plc())
```

## API Reference
### Class: `S7`
The main class for establishing and managing S7 PLC connections.

#### Constructor
```python
await S7(ip: str, port: int, rack: int, slot: int) -> S7
```
| Parameter | Type | Description |
|-----------|------|-------------|
| `ip` | str | IP address of the S7 PLC |
| `port` | int | Port number (default: 102 for S7 protocol) |
| `rack` | int | PLC rack number (typically 0 for most setups) |
| `slot` | int | PLC slot number (varies by model: e.g., 1 for S7-1200/1500) |

#### Method: `__call__` (DBRead Operation)
```python
await s7(db: int, adr: int, sz: int) -> dict[Annotated[int, 'db'], Annotated[bytearray, 'start from 0']]
```
Performs a DBRead operation to read a contiguous block of data from a specified data block.

| Parameter | Type | Description |
|-----------|------|-------------|
| `db` | int | Data block number (e.g., 1 for DB1) |
| `adr` | int | Starting address (0-based) within the data block |
| `sz` | int | Number of bytes to read |

| Return Value | Description |
|--------------|-------------|
| `dict` | A dictionary with: <br> - Key: DB number (annotated as 'db') <br> - Value: Bytearray of read data (annotated as 'start from 0') |

## Important Notes
### Serial Connection Behavior
The S7 connection is **serial** (single-threaded/non-concurrent). **Do not make multiple concurrent requests on the same connection** while a request is processing. This will cause connection conflicts, timeouts, or corrupted data.

#### Bad Practice (Avoid!)
```python
async def bad_example():
    s7 = await S7("192.168.0.1", 102, 0, 1)
    # Concurrent requests on same connection - WILL CAUSE ISSUES
    task1 = asyncio.create_task(s7(1, 0, 10))
    task2 = asyncio.create_task(s7(2, 0, 5))
    await asyncio.gather(task1, task2)
```

#### Good Practice
```python
async def good_example():
    s7 = await S7("192.168.0.1", 102, 0, 1)
    # Sequential requests (safe)
    data1 = await s7(1, 0, 10)
    data2 = await s7(2, 0, 5)
    
    # Or create separate connections for concurrent requests
    s7_1 = await S7("192.168.0.1", 102, 0, 1)
    s7_2 = await S7("192.168.0.1", 102, 0, 1)
    task1 = asyncio.create_task(s7_1(1, 0, 10))
    task2 = asyncio.create_task(s7_2(2, 0, 5))
    await asyncio.gather(task1, task2)
```

## Error Handling
Always wrap S7 operations in try/except blocks to handle network issues, PLC unavailability, or invalid addresses:
```python
async def safe_read():
    try:
        s7 = await S7("192.168.0.1", 102, 0, 1)
        data = await s7(1, 0, 10)
        return data
    except ConnectionRefusedError:
        print("Error: Could not connect to PLC (check IP/port)")
    except TimeoutError:
        print("Error: Request timed out (PLC may be offline)")
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
    finally:
        # Clean up connection if needed
        pass
```

## Compatibility
- Python 3.8+ (requires async/await support)
- Siemens S7-1200, S7-1500 (tested)
- May work with S7-300/400 (untested - please report compatibility)

## Troubleshooting
1. **Connection Issues**: Verify PLC IP/port, firewall settings, and network connectivity
2. **Invalid Address**: Ensure DB number, starting address, and size are within valid ranges for your PLC
3. **Concurrent Request Errors**: Follow the serial connection guidelines (sequential requests or multiple connections)
4. **Timeout Errors**: Check PLC availability and network latency

## License
AGPL

## Contributing
Contributions are welcome! Please open an issue for bugs/feature requests or submit a pull request with improvements. Ensure all async operations follow the serial connection guidelines and include tests for new functionality.