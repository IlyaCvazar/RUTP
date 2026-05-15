"""
Управляет отправкой данных: буферизация, окно перегрузки, повторные передачи,
persist-таймер для zero-window probing.
"""
import asyncio
import logging
from collections import deque
from typing import Callable, Deque, Dict, Optional, List, Tuple

from .packet import Packet
from .timers import RetransmissionTimer
from .congestion import CongestionController
from .config import get_config
from .constants import TYPE_DATA, SAFE_PAYLOAD_IPV4
from .utils import seq_leq, seq_before, seq_diff

logger = logging.getLogger(__name__)


def seq_in_range(seq: int, start: int, end: int) -> bool:
    """
    Возвращает True, если seq принадлежит полуинтервалу [start, end)
    в 32-битном циклическом пространстве (RFC 1982).
    """
    if start == end:
        return False
    return seq_diff(start, seq) < seq_diff(start, end)


class MaxRetransmitsExceeded(Exception):
    pass


class Sender:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        send_callback: Callable[[bytes], None],
        on_max_retransmit: Optional[Callable[[int], None]] = None
    ) -> None:
        self._loop = loop
        self._send_raw = send_callback
        self._on_max_retransmit = on_max_retransmit

        # Очередь на отправку и неподтверждённые пакеты
        self._send_buffer: Deque[Packet] = deque()
        self._unacked: Dict[int, Packet] = {}
        self._timers: Dict[int, RetransmissionTimer] = {}
        self._retrans_count: Dict[int, int] = {}

        # Номера
        self._next_seq: int = 0

        # Окна
        self._peer_window: int = 65535      # последнее известное окно получателя
        self._recv_window: int = 65535      # своё окно (отправляем в пакетах)

        # Контроль перегрузки
        self._congestion = CongestionController()

        # Параметры ретрансмиссии
        self._max_retransmits = get_config()["max_retransmits"]

        # Persist-таймер для zero-window probing
        self._persist_timer: Optional[asyncio.TimerHandle] = None
        self._persist_probes: int = 0
        self._max_persist_probes: int = 10   # до сброса соединения

        self._closed: bool = False

    # -------------------------- Публичные методы --------------------------
    def set_recv_window(self, window: int) -> None:
        """Обновить размер приёмного окна (отправляется в заголовках)."""
        self._recv_window = window

    def send(self, data: bytes) -> None:
        """Нарезать данные на сегменты и поместить в буфер отправки."""
        mss = SAFE_PAYLOAD_IPV4
        while data:
            chunk = data[:mss]
            data = data[mss:]
            pkt = Packet(
                type=TYPE_DATA,
                seq_num=self._next_seq,
                payload=chunk,
                window=self._recv_window
            )
            self._send_buffer.append(pkt)
            self._next_seq = (self._next_seq + 1) % 2**32
        self._flush_packets()

    def on_ack(self, ack_num: int, window: int,
               sack_blocks: Optional[List[Tuple[int, int]]] = None) -> None:
        """
        Обработка входящего ACK (кумулятивного + SACK).
        window - новое окно получателя.
        """
        self._peer_window = window

        # Если окно получателя стало положительным – останавливаем persist-таймер
        if window > 0 and self._persist_timer is not None:
            self._stop_persist_timer()

        last_sent = (self._next_seq - 1) % 2**32
        self._congestion.on_ack(ack_num, last_sent)

        # Кумулятивное подтверждение (все seq < ack_num)
        confirmed = [seq for seq in self._unacked if seq_leq(seq, ack_num - 1)]

        # Выборочное подтверждение через SACK
        if sack_blocks:
            for seq in list(self._unacked.keys()):
                if seq in confirmed:
                    continue
                for start, end in sack_blocks:
                    if seq_in_range(seq, start, end):
                        confirmed.append(seq)
                        break

        # Удалить подтверждённые пакеты
        for seq in confirmed:
            self._remove_packet(seq)

        # Попытаться отправить следующие пакеты
        self._flush_packets()

    # -------------------------- Внутренние методы --------------------------
    def _flush_packets(self) -> None:
        """Отправляет пакеты из буфера, если позволяют окна."""
        while self._send_buffer:
            # Ограничение по окну перегрузки (в пакетах)
            if len(self._unacked) >= self._congestion.cwnd:
                break

            next_pkt = self._send_buffer[0]
            pkt_size = len(next_pkt.payload)

            # Ограничение по окну получателя (в байтах)
            bytes_in_flight = sum(len(p.payload) for p in self._unacked.values())
            if self._peer_window > 0:
                if bytes_in_flight + pkt_size > self._peer_window:
                    break
            else:
                # Нулевое окно получателя – запускаем persist-таймер и выходим
                if self._persist_timer is None:
                    self._start_persist_timer()
                break

            pkt = self._send_buffer.popleft()
            self._send_with_retransmit(pkt)

    def _send_with_retransmit(self, pkt: Packet) -> None:
        """Отправить пакет и запустить таймер повторной передачи."""
        seq = pkt.seq_num
        pkt.window = self._recv_window
        self._send_raw(pkt.serialize())
        self._unacked[seq] = pkt
        self._retrans_count[seq] = 0
        timer = RetransmissionTimer(self._loop)
        timer.start(lambda s=seq: self._on_retransmit(s))
        self._timers[seq] = timer

    def _on_retransmit(self, seq: int) -> None:
        pkt = self._unacked.get(seq)
        if not pkt:
            return
        count = self._retrans_count.get(seq, 0) + 1
        self._retrans_count[seq] = count
        if count > self._max_retransmits:
            ...
            return
        logger.info("Retransmit seq=%d (attempt %d)", seq, count)
        self._congestion.on_loss()
        pkt.window = self._recv_window
        self._send_raw(pkt.serialize())
        # СОЗДАЁМ НОВЫЙ ТАЙМЕР ДЛЯ ЭТОГО ЖЕ ПАКЕТА
        new_timer = RetransmissionTimer(self._loop)
        new_timer.start(lambda s=seq: self._on_retransmit(s))
        self._timers[seq] = new_timer

    def _remove_packet(self, seq: int) -> None:
        """Удалить пакет из неподтверждённых, остановить его таймер."""
        self._unacked.pop(seq, None)
        self._retrans_count.pop(seq, None)
        timer = self._timers.pop(seq, None)
        if timer:
            timer.stop()
            timer.reset_rto()

    # -------------------------- Persist-таймер (zero-window probing) ------
    def _start_persist_timer(self) -> None:
        """Запустить или перезапустить persist-таймер с экспоненциальной задержкой."""
        self._stop_persist_timer()
        # Задержка: 0.5, 1, 2, 4, ... до 60 сек
        delay = min(60.0, 0.5 * (2 ** self._persist_probes))
        self._persist_timer = self._loop.call_later(delay, self._send_probe)

    def _stop_persist_timer(self) -> None:
        if self._persist_timer:
            self._persist_timer.cancel()
            self._persist_timer = None
        self._persist_probes = 0

    def _send_probe(self) -> None:
        """Отправить зондирующий пакет (ретрансляция старого или keep-alive)."""
        self._persist_probes += 1
        if self._persist_probes > self._max_persist_probes:
            logger.error("Persist timer expired, closing connection")
            if self._on_max_retransmit:
                self._on_max_retransmit(-1)   # специальный код
            return

        # Попытка отправить самый старый неподтверждённый пакет
        if self._unacked:
            oldest_seq = min(self._unacked.keys())
            pkt = self._unacked[oldest_seq]
            logger.debug("Sending zero-window probe, retransmit seq=%d", oldest_seq)
            self._send_raw(pkt.serialize())
        else:
            # Нет данных – отправляем пустой ACK (keep-alive probe)
            logger.debug("Sending keep-alive probe")
            probe = Packet(type=TYPE_DATA, seq_num=self._next_seq, payload=b'')
            self._send_raw(probe.serialize())

        # Перезапустить таймер
        self._start_persist_timer()

    @property
    def window(self) -> int:
        """Последнее известное окно получателя."""
        return self._peer_window
