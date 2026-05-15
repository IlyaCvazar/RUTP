
<p align="center">
  <a href="RU_README.md">
    <img src="https://img.shields.io/badge/Русский-README-blue?style=for-the-badge&logo=readthedocs" alt="Русский">
  </a>
</p>

# RUTP — Reliable UDP Transport Protocol

**RUTP** is a custom reliable transport protocol over UDP, implemented in pure Python 3.7+ with `asyncio`.  
It provides TCP-like reliability, congestion control, flow control, and selective acknowledgements, while remaining lightweight and embeddable.

## Features

- **Reliable delivery** – automatic retransmission with exponential backoff (RFC 6298)
- **Congestion control** – NewReno algorithm (slow start, congestion avoidance, fast retransmit/recovery)
- **Flow control** – receiver window advertisement and **zero‑window probing** (persist timer)
- **Selective acknowledgements (SACK)** – efficient retransmission using SACK blocks
- **Keep‑alive** – periodic probes keep idle connections alive
- **Asynchronous API** – built on `asyncio`
- **Multi‑client server** – `RUTPServer` manages multiple simultaneous connections
- **Pluggable parameters** – RTO, max retransmits, etc., configurable via environment variables

## Installation

Requires Python 3.7 or later. No external dependencies.

```bash
# Clone the repository
git clone https://github.com/IlyaCvazar/RUTP.git
cd RUTP

# Install in development mode
pip install -e .
```

Or directly from the source:

```bash
pip wheel .
pip install rutp-1.0.0-*.whl
```

## Quick Start

### Echo Server (multi‑client)

```python
import asyncio
from rutp import RUTPServer, RUTPConnection

async def handle_client(conn: RUTPConnection):
    queue = asyncio.Queue()
    conn.on_data = lambda data: queue.put_nowait(data)
    try:
        while True:
            data = await queue.get()
            conn.send(data)   # echo back
    except asyncio.CancelledError:
        conn.close()
        raise

async def main():
    loop = asyncio.get_running_loop()
    # Important: wrap async handler in a lambda that creates a task
    server = RUTPServer(loop, on_connection=lambda conn: asyncio.create_task(handle_client(conn)))
    await server.listen(9000)
    print("Server listening on port 9000")
    await asyncio.Event().wait()

asyncio.run(main())
```

### Client

```python
import asyncio
from rutp import RUTPConnection

async def main():
    loop = asyncio.get_running_loop()
    client = RUTPConnection(loop)
    # Wait for handshake completion (you can use asyncio.Event or a simple sleep)
    await client.connect('127.0.0.1', 9000)
    await asyncio.sleep(0.1)   # allow handshake to finish

    received = bytearray()
    done = asyncio.Event()
    client.on_data = lambda d: (received.extend(d), done.set())

    message = "Hello, RUTP!".encode()
    print("Sending:", message)
    client.send(message)

    try:
        await asyncio.wait_for(done.wait(), timeout=5.0)
        print("Response:", bytes(received))
    except asyncio.TimeoutError:
        print("No response")
    finally:
        await client.close()

asyncio.run(main())
```

Run the server in one terminal, then the client in another. You should see the echoed message.

## API

### `RUTPServer`

A server that listens for incoming connections and creates a separate `RUTPConnection` for each client.

**Constructor**
```python
RUTPServer(loop: asyncio.AbstractEventLoop, on_connection: Callable[[RUTPConnection], None])
```
- `loop` – asyncio event loop.
- `on_connection` – called when a new connection is established; receives the new `RUTPConnection` instance.  
  **Note:** If your handler is an `async def` coroutine, you must wrap it in `asyncio.create_task` (e.g. `lambda conn: asyncio.create_task(my_handler(conn))`).

**Methods**
- `await listen(port: int)` – start listening on the given UDP port.

### `RUTPConnection`

Represents a single connection (client‑side or a server‑accepted connection).

**Constructor**
```python
RUTPConnection(loop: asyncio.AbstractEventLoop, on_data: Optional[Callable[[bytes], None]] = None)
```
- `loop` – asyncio event loop.
- `on_data` – optional callback for received data.

**Attributes**
- `on_data` – set or change the data callback at any time.
- `on_close` – optional callback called when the connection is closed.

**Methods**
- `await connect(host: str, port: int)` – initiate a connection to a server.
- `send(data: bytes)` – send data (only in `ESTABLISHED` state).
- `await close()` – gracefully close the connection (FIN handshake).
- `abort()` – force‑close the connection without sending FIN.

**Connection States** (`ConnState` enum):  
`CLOSED`, `SYN_SENT`, `SYN_RECEIVED`, `ESTABLISHED`, `FIN_WAIT_1`, `FIN_WAIT_2`, `CLOSE_WAIT`, `LAST_ACK`, `TIME_WAIT`.

### `Packet`

Wire format representation. Normally you don’t need to instantiate manually.

**Fields:**
- `version` (int)
- `type` (int) – `TYPE_DATA` (0x01), `TYPE_ACK` (0x02), `TYPE_CONTROL` (0x03)
- `flags` (int) – bitmask `FLAG_SYN`, `FLAG_ACK`, `FLAG_FIN`, `FLAG_RST`
- `seq_num` (int)
- `ack_num` (int)
- `window` (int)
- `payload` (bytes)

**Methods:**
- `serialize() -> bytes`
- `Packet.deserialize(data: bytes) -> Optional[Packet]`
- `is_syn_version(version: int) -> bool` (static)
- `make_syn_version(base_version: int = PROTOCOL_VERSION) -> int` (static)

### Constants and Configuration

All constants are in `rutp.constants`.  
User‑tunable parameters are read from environment variables via `rutp.config.get_config()`:

| Environment variable | Default | Description |
|----------------------|---------|-------------|
| `RUTP_RTO`           | 1.0     | Base retransmission timeout (seconds) |
| `RUTP_MIN_RTO`       | 0.2     | Minimum RTO |
| `RUTP_MAX_RTO`       | 60.0    | Maximum RTO |
| `RUTP_MAX_RETRANSMITS` | 10    | Max retransmission attempts per packet |

Example: for more aggressive retransmission, run  
`RUTP_RTO=0.5 RUTP_MAX_RETRANSMITS=5 python server.py`

## Protocol Architecture

### Handshake

Three‑way handshake:
1. Client → Server: `SYN` (seq=x)
2. Server → Client: `SYN+ACK` (seq=y, ack=x+1)
3. Client → Server: `ACK` (ack=y+1) – then both enter `ESTABLISHED`.

SYN packets are marked with the 15th bit of the version field (`VERSION_SYN_BIT`).

### Data Transfer

Data passed to `send()` is split into segments of size `SAFE_PAYLOAD_IPV4` (548 bytes – the minimum guaranteed IPv4 datagram).  
Each segment is assigned a 32‑bit sequence number (cyclic per RFC 1982). The receiver reassembles the stream in order, buffering out‑of‑order packets.

### Congestion Control

Implements **NewReno** (RFC 5681):
- **Slow start**: `cwnd` increases by 1 packet per ACK until `cwnd >= ssthresh`.
- **Congestion avoidance**: `cwnd += 1/cwnd` per ACK.
- **Fast retransmit**: upon receiving 3 duplicate ACKs, `ssthresh = max(cwnd/2, 2)`, `cwnd = ssthresh + 3`, and the lost packet is retransmitted without waiting for timeout.
- **Timeout (loss)**: `ssthresh = max(cwnd/2, 2)`, `cwnd` reset to `INITIAL_CWND` (10).

Retransmissions use exponential RTO backoff (`rto = min(rto*2, max_rto)`).

### Selective Acknowledgements (SACK)

The receiver attaches SACK blocks to every ACK, describing continuous intervals of successfully received but not yet delivered segments. Each block is a pair `(start, end)` packed in big‑endian (`!II`). The sender uses SACK to mark acknowledged packets and avoid unnecessary retransmissions.

### Flow Control and Zero‑Window Probing

Each ACK carries a `window` field – the free space in the receiver’s buffer (up to `MAX_RECV_WINDOW = 65535` bytes).  
The sender must not send data if the window is zero or if the total unacknowledged data plus the next segment would exceed the window.

If the window stays zero for a prolonged time, the sender activates a **persist timer** (zero‑window probing). It periodically sends a probe (a retransmission of the oldest unacknowledged segment or a keep‑alive) to check if the window has reopened. After too many failed probes, the connection is aborted.

### Keep‑alive

Every `KEEPALIVE` seconds (default 15), an idle connection sends an empty ACK to prevent stale NAT bindings.

## Advanced Examples

### File Transfer

**Client:**
```python
with open('myfile.bin', 'rb') as f:
    client.send(f.read())
```

The protocol automatically segments, retransmits lost parts, and reassembles in order at the receiver.

### Handling Handshake Completion Properly

Instead of `asyncio.sleep`, you can wait for the `ESTABLISHED` state:

```python
import asyncio
from rutp import RUTPConnection, ConnState

class MyClient:
    def __init__(self, loop):
        self.conn = RUTPConnection(loop)
        self.connected = asyncio.Event()

    async def connect(self, host, port):
        asyncio.create_task(self._watch_state())
        await self.conn.connect(host, port)
        await self.connected.wait()

    async def _watch_state(self):
        while True:
            if self.conn._state == ConnState.ESTABLISHED:
                self.connected.set()
                break
            await asyncio.sleep(0.05)
```

## Testing

Unit tests are in the `tests/` directory. Install `pytest` and run:

```bash
pip install pytest
pytest tests/
```

Tests cover:
- Sequence number arithmetic (wraparound)
- Packet serialization/deserialization
- Congestion control (NewReno)
- RTO and keep‑alive timers
- Receiver buffering and SACK generation
- Sender window management and persist timer
- Full handshake and data transfer

## License and Attribution

This project is distributed under the **RUTP License**, which **requires explicit attribution**:

- the protocol name: **RUTP (Reliable UDP Transport Protocol)**;
- the creator's name: **Ilya Okolelov** (Илья Околелов).

The attribution must be placed in the documentation, program interface, or another conspicuous place visible to the end user.  
Full license text: [LICENSE.txt](LICENSE.txt)

© 2026 Ilya Okolelov. All rights reserved.

## Development

Project structure:
```
├── src/rutp/        # protocol package
│   ├── __init__.py
│   ├── config.py
│   ├── congestion.py
│   ├── connection.py
│   ├── constants.py
│   ├── flow.py
│   ├── packet.py
│   ├── receiver.py
│   ├── sender.py
│   ├── server.py
│   ├── timers.py
│   └── utils.py
├── tests/           # tests
├── pyproject.toml
├── LICENSE.txt
└── README.md
```

## Questions and Feedback

Open an issue on GitHub.  
Author: Ilya (github: IlyaCvazar)

