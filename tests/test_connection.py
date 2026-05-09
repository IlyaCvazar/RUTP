import asyncio
import pytest
from rutp.connection import RUTPConnection, ConnState
from rutp.packet import Packet
from rutp.constants import PROTOCOL_VERSION, VERSION_SYN_BIT, FLAG_SYN, FLAG_RST, TYPE_CONTROL

@pytest.mark.asyncio
async def test_basic_handshake(event_loop):
    server = RUTPConnection(event_loop)
    await server.listen(0)
    port = server._transport.get_extra_info('sockname')[1]

    client = RUTPConnection(event_loop)
    await client.connect('127.0.0.1', port)

    assert client._state == ConnState.ESTABLISHED
    # Сервер тоже должен быть в ESTABLISHED (после обработки handshake)
    assert server._state == ConnState.ESTABLISHED

    await client.close()
    await asyncio.sleep(0.1)
    assert client._state in (ConnState.FIN_WAIT_2, ConnState.TIME_WAIT)
    server.close()

@pytest.mark.asyncio
async def test_data_transfer(event_loop):
    server_data = []
    server = RUTPConnection(event_loop, on_data=lambda d: server_data.append(d))
    await server.listen(0)
    port = server._transport.get_extra_info('sockname')[1]

    client = RUTPConnection(event_loop)
    await client.connect('127.0.0.1', port)
    await asyncio.sleep(0.1)  # дать серверу завершить handshake

    client.send(b'Hello, RUTP!')
    await asyncio.sleep(0.2)
    assert server_data == [b'Hello, RUTP!']

    await client.close()
    server.close()

@pytest.mark.asyncio
async def test_major_version_mismatch(event_loop):
    """Клиент с версией 2.0 не может соединиться с сервером 1.0."""
    server = RUTPConnection(event_loop)
    await server.listen(0)
    port = server._transport.get_extra_info('sockname')[1]

    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    syn = Packet(
        version=0x0200 | VERSION_SYN_BIT,  # MAJOR 2, MINOR 0
        type=TYPE_CONTROL,
        flags=FLAG_SYN,
        seq_num=42,
        window=65535
    )
    sock.sendto(syn.serialize(), ('127.0.0.1', port))
    sock.settimeout(0.5)
    try:
        data, _ = sock.recvfrom(4096)
        rst = Packet.deserialize(data)
        assert rst is not None
        assert rst.flags & FLAG_RST
    except socket.timeout:
        pytest.fail("Server did not respond with RST for mismatched version")
