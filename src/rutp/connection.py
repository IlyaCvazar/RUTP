"""
Реализация конечного автомата соединения RUTP.
Поддерживает рукопожатие, передачу данных, закрытие, проверку версии.
"""
import asyncio
import logging
from enum import Enum
from typing import Callable, Optional

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
    VERSION_SYN_BIT,
)
from .sender import Sender
from .receiver import Receiver
from .timers import KeepAliveTimer

logger = logging.getLogger(__name__)


class ConnState(Enum):
    LISTEN = 1
    SYN_SENT = 2
    SYN_RECEIVED = 3
    ESTABLISHED = 4
    FIN_WAIT_1 = 5
    FIN_WAIT_2 = 6
    CLOSE_WAIT = 7
    LAST_ACK = 8
    TIME_WAIT = 9
    CLOSED = 10


class RUTPConnection:
    def __init__(self, loop: asyncio.AbstractEventLoop,
                 on_data: Optional[Callable[[bytes], None]] = None,
                 on_error: Optional[Callable[[Exception], None]] = None) -> None:
        self._loop = loop
        self._state = ConnState.CLOSED
        self._transport: Optional[asyncio.DatagramTransport] = None
        self._protocol: Optional[asyncio.DatagramProtocol] = None

        self.on_data = on_data
        self.on_error = on_error
        self.on_connection: Optional[Callable[['RUTPConnection'], None]] = None

        self._sender: Optional[Sender] = None
        self._receiver: Optional[Receiver] = None

        self._peer_seq = 0
        self._peer_window = 65535
        self._peer_addr = None
        self._is_connected = False

        self._keepalive = KeepAliveTimer(loop, callback=self._send_keepalive)
        self._connection_tasks = set()   # для отслеживания задач on_connection

    # ----------------------------- Публичный API -----------------------------
    async def listen(self, port: int) -> None:
        self._state = ConnState.LISTEN
        self._is_connected = False
        self._transport, self._protocol = await self._loop.create_datagram_endpoint(
            lambda: _RUTPProtocol(self), local_addr=('0.0.0.0', port)
        )

    async def connect(self, host: str, port: int) -> None:
        self._init_components()
        self._state = ConnState.SYN_SENT
        self._is_connected = True
        self._transport, self._protocol = await self._loop.create_datagram_endpoint(
            lambda: _RUTPProtocol(self), remote_addr=(host, port)
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
        if self._state == ConnState.ESTABLISHED and self._sender:
            self._sender.send(data)
        else:
            logger.warning("Cannot send data in state %s", self._state)

    async def close(self) -> None:
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

    def _close_transport(self) -> None:
        # Отменить все фоновые задачи on_connection
        for task in self._connection_tasks:
            task.cancel()
        self._connection_tasks.clear()
        if self._transport:
            self._transport.close()
        self._state = ConnState.CLOSED

    # --------------------------- Внутренние методы ---------------------------
    def _init_components(self) -> None:
        self._receiver = Receiver(self._deliver_data)
        self._sender = Sender(
            self._loop,
            self._send_raw,
            on_max_retransmit=self._on_max_retransmit
        )

    def _deliver_data(self, data: bytes) -> None:
        if self.on_data:
            self.on_data(data)

    def _send_packet(self, pkt: Packet) -> None:
        if self._transport:
            data = pkt.serialize()
            if self._is_connected:
                self._transport.sendto(data)
            elif self._peer_addr:
                self._transport.sendto(data, self._peer_addr)

    def _send_raw(self, data: bytes) -> None:
        if self._transport:
            if self._is_connected:
                self._transport.sendto(data)
            elif self._peer_addr:
                self._transport.sendto(data, self._peer_addr)

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
        """Вызывается при превышении лимита ретрансляций."""
        logger.error("Max retransmits exceeded, aborting connection")
        if self.on_error:
            self.on_error(MaxRetransmitsExceeded(f"Seq {seq}"))
        self._reset_connection()

    def _check_version(self, pkt: Packet) -> bool:
        """Возвращает True, если версия совместима (мажорная часть)."""
        return (pkt.version & 0xFF00) == (PROTOCOL_VERSION & 0xFF00)

    def _handle_packet(self, pkt: Packet, addr) -> None:
        # Проверка мажорной версии (кроме SYN-пакетов, у них может быть установлен бит)
        if not self._check_version(pkt) and not pkt.flags & FLAG_SYN:
            # Несовместимая версия – отправить RST
            rst = Packet(type=TYPE_CONTROL, flags=FLAG_RST)
            self._send_packet(rst)
            return

        if self._state == ConnState.LISTEN:
            if pkt.flags & FLAG_RST:
                return
            if pkt.flags & FLAG_SYN:
                self._peer_addr = addr
                self._init_components()
                self._peer_seq = pkt.seq_num
                self._state = ConnState.SYN_RECEIVED
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
                if self.on_connection:
                    # Запускаем колбэк как задачу и сохраняем ссылку
                    if asyncio.iscoroutinefunction(self.on_connection):
                        task = asyncio.create_task(self._run_connection_handler(self.on_connection, self))
                    else:
                        task = asyncio.create_task(self._run_connection_handler_sync(self.on_connection, self))
                    self._connection_tasks.add(task)
                    task.add_done_callback(self._connection_tasks.discard)
            return

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

        if self._state in (ConnState.SYN_RECEIVED, ConnState.ESTABLISHED,
                           ConnState.FIN_WAIT_1, ConnState.FIN_WAIT_2,
                           ConnState.CLOSE_WAIT, ConnState.LAST_ACK):
            self._handle_established(pkt)

    async def _run_connection_handler(self, handler, conn):
        try:
            await handler(conn)
        except asyncio.CancelledError:
            logger.debug("Connection handler cancelled")
        except Exception as e:
            logger.exception("Error in connection handler")
            if self.on_error:
                self.on_error(e)

    async def _run_connection_handler_sync(self, handler, conn):
        try:
            handler(conn)
        except Exception as e:
            logger.exception("Error in connection handler")
            if self.on_error:
                self.on_error(e)

    def _handle_established(self, pkt: Packet) -> None:
        if pkt.flags & FLAG_RST:
            self._reset_connection()
            return

        if self._state == ConnState.SYN_RECEIVED:
            self._state = ConnState.ESTABLISHED

        if pkt.flags & FLAG_FIN:
            self._peer_seq = pkt.seq_num
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
            self._peer_window = pkt.window
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

    def _reset_connection(self) -> None:
        logger.debug("RST received or max retransmits, closing")
        self._close_transport()


class _RUTPProtocol(asyncio.DatagramProtocol):
    def __init__(self, conn: RUTPConnection) -> None:
        self._conn = conn
        super().__init__()

    def datagram_received(self, data: bytes, addr) -> None:
        pkt = Packet.deserialize(data)
        if pkt:
            self._conn._handle_packet(pkt, addr)


class MaxRetransmitsExceeded(Exception):
    pass
