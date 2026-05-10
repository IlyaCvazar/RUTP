import asyncio
import hashlib
import os
import sys
from rutp import RUTPConnection, ConnState

PASS = 0
FAIL = 0

def check(condition, msg):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✓ {msg}")
    else:
        FAIL += 1
        print(f"  ✗ {msg} FAILED")

async def echo_server_handler(conn: RUTPConnection):
    """Эхо-сервер, ожидающий полного завершения соединения."""
    queue = asyncio.Queue()
    conn.on_data = lambda data: queue.put_nowait(data)
    try:
        while conn._state not in (ConnState.CLOSE_WAIT, ConnState.CLOSED):
            try:
                data = await asyncio.wait_for(queue.get(), timeout=0.1)
                conn.send(data)
                await asyncio.sleep(0.01)
            except asyncio.TimeoutError:
                pass
        # Получен FIN → закрываем серверную сторону и ждём завершения
        if conn._state == ConnState.CLOSE_WAIT:
            await conn.close()                    # отправляем FIN, переходим в LAST_ACK
            # Ждём ACK на наш FIN (переход в CLOSED)
            while conn._state != ConnState.CLOSED:
                await asyncio.sleep(0.05)
    except Exception:
        pass

async def run_server(port_future: asyncio.Future):
    loop = asyncio.get_event_loop()
    server = RUTPConnection(loop)
    server.on_connection = echo_server_handler
    await server.listen(0)
    port = server._transport.get_extra_info('sockname')[1]
    port_future.set_result(port)
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await server.close()

async def test_client(port: int):
    loop = asyncio.get_event_loop()
    client = RUTPConnection(loop)

    # 1. Рукопожатие
    await client.connect('127.0.0.1', port)
    await asyncio.sleep(0.1)
    check(client._state == ConnState.ESTABLISHED,
          "Handshake: client → ESTABLISHED")

    # 2. Маленькое эхо
    received = bytearray()
    done = asyncio.Event()
    client.on_data = lambda d: (received.extend(d), done.set())
    msg_small = b"Hello, RUTP!"
    client.send(msg_small)
    await asyncio.wait_for(done.wait(), timeout=2.0)
    check(received == msg_small,
          f"Small echo: {received} == {msg_small}")
    received.clear()
    done.clear()

    # 3. Средний объём (>1 сегмента)
    msg_medium = os.urandom(1500)
    def medium_collect(d):
        nonlocal received
        received.extend(d)
        if len(received) >= len(msg_medium):
            done.set()
    client.on_data = medium_collect
    client.send(msg_medium)
    try:
        await asyncio.wait_for(done.wait(), timeout=5.0)
        check(received == msg_medium,
              f"Medium echo ({len(msg_medium)} bytes): correct")
    except asyncio.TimeoutError:
        check(False, f"Medium echo: timeout (got {len(received)} of {len(msg_medium)} bytes)")
    received.clear()
    done.clear()

    # 4. Большой файл (100 КБ)
    msg_large = os.urandom(100_000)
    checksum_orig = hashlib.sha256(msg_large).digest()
    all_data = bytearray()
    def large_collect(d):
        nonlocal all_data
        all_data.extend(d)
        if len(all_data) >= len(msg_large):
            done.set()
    client.on_data = large_collect
    client.send(msg_large)
    try:
        await asyncio.wait_for(done.wait(), timeout=20.0)
        checksum_rcvd = hashlib.sha256(all_data).digest()
        check(checksum_orig == checksum_rcvd,
              f"Large file ({len(msg_large)} bytes): SHA‑256 matches")
    except asyncio.TimeoutError:
        check(False, f"Large file: timeout (got {len(all_data)} of {len(msg_large)} bytes)")
    done.clear()

    # 5. Закрытие соединения
    await client.close()                      # клиент отправляет FIN
    await asyncio.sleep(0.5)                  # ждём ответный FIN от сервера и завершение

    # После полного обмена FIN-ACK клиент должен быть в одном из финальных состояний
    check(client._state in (ConnState.FIN_WAIT_2, ConnState.TIME_WAIT, ConnState.CLOSED),
          f"Close: client state = {client._state.name}")

    print(f"\n{'='*40}")
    print(f"Total: {PASS} passed, {FAIL} failed")
    if FAIL:
        sys.exit(1)

async def main():
    loop = asyncio.get_event_loop()
    port_future = loop.create_future()

    server_task = asyncio.create_task(run_server(port_future))
    port = await port_future
    print(f"Test server listening on port {port}")

    await test_client(port)

    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass

if __name__ == "__main__":
    asyncio.run(main())