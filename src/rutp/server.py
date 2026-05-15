"""
Серверная часть RUTP: слушает порт и создаёт отдельные RUTPConnection для каждого клиента.
"""
import asyncio
import logging
from typing import Callable, Dict, Optional, Tuple

from .packet import Packet
from .connection import RUTPConnection, ConnState
from .constants import FLAG_SYN, FLAG_RST, TYPE_CONTROL

logger = logging.getLogger(__name__)


class RUTPServer:
    """
    Сервер RUTP. Слушает UDP-порт, при получении SYN создаёт новое соединение.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop,
                 on_connection: Callable[[RUTPConnection], None]):
        self._loop = loop
        self._on_connection = on_connection
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._connections: Dict[Tuple[str, int], RUTPConnection] = {}

    async def listen(self, port: int) -> None:
        """Начать прослушивание порта."""
        self._transport, _ = await self._loop.create_datagram_endpoint(
            lambda: _ServerProtocol(self), local_addr=('0.0.0.0', port)
        )
        logger.info("RUTP Server listening on port %d", port)

    def _create_connection(self, addr: Tuple[str, int]) -> RUTPConnection:
        """Создать новое соединение для адреса addr."""
        conn = RUTPConnection(self._loop)
        conn.set_transport(self._transport, addr)
        conn.on_close = self._remove_connection
        # Вызываем колбэк приложения
        self._on_connection(conn)
        return conn

    def _remove_connection(self, conn: RUTPConnection) -> None:
        """Удалить соединение из словаря."""
        if conn._remote_addr:
            self._connections.pop(conn._remote_addr, None)


class _ServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, server: RUTPServer):
        self._server = server

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        pkt = Packet.deserialize(data)
        if not pkt:
            return

        # Если это SYN – создаём новое соединение (или используем существующее?)
        if pkt.flags & FLAG_SYN and pkt.type == TYPE_CONTROL:
            # Избегаем повторного создания соединения, если оно уже есть
            if addr not in self._server._connections:
                conn = self._server._create_connection(addr)
                self._server._connections[addr] = conn
            else:
                conn = self._server._connections[addr]
            # Передаём пакет в соединение
            conn.on_packet(pkt)
        else:
            # Не SYN – должно быть существующее соединение
            conn = self._server._connections.get(addr)
            if conn:
                conn.on_packet(pkt)
            else:
                # Неизвестный адрес – отправить RST
                self._send_rst(addr, pkt)

    def _send_rst(self, addr: Tuple[str, int], pkt: Packet) -> None:
        """Отправить RST в ответ на пакет от неизвестного соединения."""
        rst = Packet(
            type=TYPE_CONTROL,
            flags=FLAG_RST,
            seq_num=pkt.ack_num,  # эхо
            ack_num=(pkt.seq_num + 1) % 2**32 if pkt.seq_num else 0
        )
        self._server._transport.sendto(rst.serialize(), addr)
