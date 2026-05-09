```markdown
# RUTP – Reliable UDP Transport Protocol

**RUTP** is a custom reliable transport protocol over UDP, implemented in pure Python 3.7+ with `asyncio`.  
It provides TCP-like reliability, congestion control, flow control, and selective acknowledgements, while remaining lightweight and embeddable.

## Features

- **Reliable delivery** – automatic retransmission with exponential backoff (RFC 6298)
- **Congestion control** – NewReno algorithm (slow start, congestion avoidance, fast retransmit/recovery)
- **Flow control** – receiver window advertisement and zero‑window probing
- **Selective acknowledgements (SACK)** – up to 1000 reorder buffer blocks for efficient retransmission
- **Keep‑alive** – periodic probes keep idle connections alive
- **Asynchronous API** – built on `asyncio`, supports both client and server
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

### Echo Server

```python
import asyncio
from rutp import RUTPConnection

async def handle_client(conn: RUTPConnection):
    queue = asyncio.Queue()
    conn.on_data = lambda data: queue.put_nowait(data)
    try:
        while True:
            data = await queue.get()
            conn.send(data)   # echo back
    except:
        pass
    finally:
        conn.close()

async def main():
    loop = asyncio.get_event_loop()
    server = RUTPConnection(loop)
    server.on_connection = handle_client
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
    client = RUTPConnection(asyncio.get_event_loop())
    received = bytearray()
    done = asyncio.Event()
    client.on_data = lambda d: (received.extend(d), done.set())

    await client.connect('127.0.0.1', 9000)
    await asyncio.sleep(0.1)   # wait for handshake
    message = "Hello, RUTP!".encode()
    print("Sending:", message)
    client.send(message)

    try:
        await asyncio.wait_for(done.wait(), timeout=5.0)
        print("Response:", bytes(received))
    except asyncio.TimeoutError:
        print("No response")
    await client.close()

asyncio.run(main())
```

## Documentation  
Key classes and methods are summarised below:

- **`RUTPConnection(loop, on_data=None)`** – main connection object. Methods: `listen(port)`, `connect(host, port)`, `send(data)`, `close()`.
- **`Packet`** – serializable packet with fields: version, type, flags, seq_num, ack_num, window, payload.
- **Constants** – protocol parameters in `rutp.constants`.

## License

This project is distributed under a custom license that requires **explicit attribution** – the protocol name **RUTP** and the creator's name must be visibly displayed in any product or service that uses or modifies this software. See [LICENSE.txt](LICENSE.txt) for full details.

© 2026 Ilya [your surname]. All rights reserved.
```
