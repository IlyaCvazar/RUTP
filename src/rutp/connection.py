"""
Реализация конечного автомата соединения RUTP.
Поддерживает рукопожатие, передачу данных, закрытие.
Теперь RUTPConnection представляет одно соединение (клиент или серверная сторона).
Для создания сервера используйте RUTPServer.
"""
import asyncio
import logging
from enum import Enum
from typing import Callable, Optional, Tuple

from .packet import Packet
from .constants import (
    PROTOCOL_VERSION,
    TYPE_DATA,
    TYPE_ACK,
    TYPE_CONTROL,
    FLAG_SYN,
    FLAG_FIN,
    FLAG_RST,
    FLAG_ACK,
)
from .sender import Sender
from .receiver import Receiver
from .timers import KeepAliveTimer

logger = logging.getLogger(__name__)


class ConnState(Enum):
    CLOSED = 1
    SYN_SENT = 2
    SYN_RECEIVED = 3
    ESTABLISHED = 4
    FIN_WAIT_1 = 5
    FIN_WAIT_2 = 6
    CLOSE_WAIT = 7
    LAST_ACK = 8
    TIME_WAIT = 9


class MaxRetransmitsExceeded(Exception):
    pass


class RUTPConnection:
    """
    Одно RUTP‑соединение.

    Для клиента: используйте await connect(host, port)
    Для сервера: экземпляр создаётся автоматически в RUTPServer, передаётся транспорт и адрес.
    """

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 transport: Optional[asyncio.DatagramTransport] = None,
                 remote_addr: Optional[Tuple[str, int]] = None,
                 on_data: Optional[Callable[[bytes], None]] = None):
        self._loop = loop
        self._transport = transport          # может быть None до connect
        self._remote_addr = remote_addr      # для серверных соединений
        self._state = ConnState.CLOSED

        self.on_data = on_data
        self.on_close: Optional[Callable[['RUTPConnection'], None]] = None

        self._sender: Optional[Sender] = None
        self._receiver: Optional[Receiver] = None
        self._keepalive = KeepAliveTimer(loop, callback=self._send_keepalive)

        # Для клиентского режима
        self._client_mode = False

    # ----------------------------- Публичный API (клиент) -----------------------------
    async def connect(self, host: str, port: int) -> None:
        """Клиентское подключение к серверу."""
        self._init_components()
        self._state = ConnState.SYN_SENT
        self._client_mode = True

        self._transport, _ = await self._loop.create_datagram_endpoint(
            lambda: _RUTPClientProtocol(self), remote_addr=(host, port)
        )

        syn = Packet(
            version=Packet.make_syn_version(),
            type=TYPE_CONTROL,
            flags=FLAG_SYN,
            seq_num=self._sender._next_seq,
            window=65535
        )
        self._send_packet(syn)
        self._sender._next_seq = (self._sender._next_seq + 1) % 2**32

    def send(self, data: bytes) -> None:
        """Отправить данные (только в состоянии ESTABLISHED)."""
        if self._state == ConnState.ESTABLISHED and self._sender:
            self._sender.send(data)
        else:
            logger.warning("Cannot send data in state %s", self._state)

    async def close(self) -> None:
        """Корректное закрытие соединения."""
        if self._state == ConnState.ESTABLISHED:
            fin = Packet(
                type=TYPE_CONTROL,
                flags=FLAG_FIN,
                seq_num=self._sender._next_seq if self._sender else 0,
                window=65535
            )
            self._send_packet(fin)
            if self._sender:
                self._sender._next_seq = (self._sender._next_seq + 1) % 2**32
            self._state = ConnState.FIN_WAIT_1
        elif self._state == ConnState.CLOSE_WAIT:
            fin = Packet(
                type=TYPE_CONTROL,
                flags=FLAG_FIN,
                seq_num=self._sender._next_seq if self._sender else 0,
                window=65535
            )
            self._send_packet(fin)
            if self._sender:
                self._sender._next_seq = (self._sender._next_seq + 1) % 2**32
            self._state = ConnState.LAST_ACK
        else:
            self._close_transport()

    def abort(self) -> None:
        """Принудительно закрыть соединение (RST не отправляется)."""
        self._close_transport()

    # ----------------------------- Методы для серверной стороны -----------------------------
    def set_transport(self, transport: asyncio.DatagramTransport, remote_addr: Tuple[str, int]) -> None:
        """Используется сервером для привязки соединения к транспорту и адресу."""
        self._transport = transport
        self._remote_addr = remote_addr
        self._init_components()

    def on_packet(self, pkt: Packet) -> None:
        """Вызывается серверным протоколом при получении пакета для этого соединения."""
        self._handle_packet(pkt)

    # ----------------------------- Внутренние методы -----------------------------
    def _init_components(self) -> None:
        if self._receiver is None:
            self._receiver = Receiver(self._deliver_data)
        if self._sender is None:
            self._sender = Sender(
                self._loop,
                self._send_raw,
                on_max_retransmit=self._on_max_retransmit
            )

    def _deliver_data(self, data: bytes) -> None:
        if self.on_data:
            self.on_data(data)

    def _send_packet(self, pkt: Packet) -> None:
        """Отправить пакет через транспорт."""
        if not self._transport:
            return
        data = pkt.serialize()
        if self._client_mode:
            # клиентский сокет уже знает адрес
            self._transport.sendto(data)
        else:
            # серверное соединение – отправляем конкретному клиенту
            self._transport.sendto(data, self._remote_addr)

    def _send_raw(self, data: bytes) -> None:
        """Колбэк для Sender – сырые данные."""
        self._send_packet(Packet.deserialize(data))  # немного неэффективно, но работает

    def _send_keepalive(self) -> None:
        if self._state != ConnState.ESTABLISHED or not self._receiver:
            return
        ack = Packet(
            type=TYPE_ACK,
            ack_num=self._receiver._next_expected,
            window=self._receiver.window_available()
        )
        self._send_packet(ack)

    def _on_max_retransmit(self, seq: int) -> None:
        logger.error("Max retransmits exceeded, aborting connection")
        self.abort()
        if self.on_close:
            self.on_close(self)

    def _close_transport(self) -> None:
        if self._transport and not self._transport.is_closing():
            self._transport.close()
        self._state = ConnState.CLOSED
        if self.on_close:
            self.on_close(self)

    def _handle_packet(self, pkt: Packet) -> None:
        # ---- НОВЫЙ БЛОК: обработка SYN в состоянии CLOSED (сервер) ----
        if self._state == ConnState.CLOSED and (pkt.flags & FLAG_SYN) and pkt.type == TYPE_CONTROL:
            self._state = ConnState.SYN_RECEIVED
            self._init_components()
            self._receiver._next_expected = (pkt.seq_num + 1) % 2**32
            syn_ack = Packet(
                version=Packet.make_syn_version(),
                type=TYPE_CONTROL,
                flags=FLAG_SYN | FLAG_ACK,
                seq_num=self._sender._next_seq,
                ack_num=(pkt.seq_num + 1) % 2**32,
                window=65535
            )
            self._send_packet(syn_ack)
            self._sender._next_seq = (self._sender._next_seq + 1) % 2**32
            return

        # Проверка версии (упрощённо)
        if (pkt.version & 0xFF00) != (PROTOCOL_VERSION & 0xFF00) and not (pkt.flags & FLAG_SYN):
            rst = Packet(type=TYPE_CONTROL, flags=FLAG_RST)
            self._send_packet(rst)
            return

        # Состояния
        if self._state == ConnState.SYN_SENT:
            if pkt.flags & FLAG_RST:
                self._close_transport()
                return
            if (pkt.flags & FLAG_SYN) and (pkt.flags & FLAG_ACK):
                self._state = ConnState.ESTABLISHED
                self._receiver._next_expected = (pkt.seq_num + 1) % 2**32
                self._sender.on_ack(pkt.ack_num, pkt.window)
                ack = Packet(
                    type=TYPE_ACK,
                    ack_num=(pkt.seq_num + 1) % 2**32,
                    window=self._receiver.window_available()
                )
                self._send_packet(ack)
            return

        if self._state == ConnState.SYN_RECEIVED:
            # Ждём ACK от клиента
            if pkt.type == TYPE_ACK:
                self._state = ConnState.ESTABLISHED
            else:
                # Если пришёл DATA, тоже переходим в ESTABLISHED (некоторые реализации)
                self._state = ConnState.ESTABLISHED
                # обработаем пакет как установленный
                self._handle_established(pkt)
            return

        if self._state in (ConnState.ESTABLISHED, ConnState.FIN_WAIT_1, ConnState.FIN_WAIT_2,
                           ConnState.CLOSE_WAIT, ConnState.LAST_ACK):
            self._handle_established(pkt)

    def _handle_established(self, pkt: Packet) -> None:
        if pkt.flags & FLAG_RST:
            self._close_transport()
            return

        if pkt.flags & FLAG_FIN:
            if self._state == ConnState.FIN_WAIT_1:
                self._state = ConnState.TIME_WAIT
                ack = Packet(type=TYPE_ACK, ack_num=(pkt.seq_num + 1) % 2**32,
                             window=self._receiver.window_available())
                self._send_packet(ack)
                self._close_transport()
            elif self._state == ConnState.FIN_WAIT_2:
                self._state = ConnState.TIME_WAIT
                ack = Packet(type=TYPE_ACK, ack_num=(pkt.seq_num + 1) % 2**32,
                             window=self._receiver.window_available())
                self._send_packet(ack)
                self._close_transport()
            else:
                self._state = ConnState.CLOSE_WAIT
                ack = Packet(type=TYPE_ACK, ack_num=(pkt.seq_num + 1) % 2**32,
                             window=self._receiver.window_available())
                self._send_packet(ack)
            return

        if pkt.type == TYPE_ACK:
            sack_blocks = []
            if len(pkt.payload) >= 8:
                import struct
                data = pkt.payload
                for i in range(0, len(data), 8):
                    start, end = struct.unpack('!II', data[i:i+8])
                    sack_blocks.append((start, end))
            self._sender.on_ack(pkt.ack_num, pkt.window, sack_blocks)
            self._keepalive.start()

            if self._state == ConnState.FIN_WAIT_1:
                self._state = ConnState.FIN_WAIT_2
            elif self._state == ConnState.LAST_ACK:
                self._close_transport()

        elif pkt.type == TYPE_DATA:
            ack = self._receiver.process_packet(pkt)
            if ack:
                ack.version = PROTOCOL_VERSION
                self._send_packet(ack)
                self._sender.set_recv_window(self._receiver.window_available())


class _RUTPClientProtocol(asyncio.DatagramProtocol):
    """Протокол для клиентского сокета (просто передаёт пакеты в соединение)."""
    def __init__(self, conn: RUTPConnection):
        self._conn = conn

    def datagram_received(self, data: bytes, addr) -> None:
        pkt = Packet.deserialize(data)
        if pkt:
            self._conn._handle_packet(pkt)
