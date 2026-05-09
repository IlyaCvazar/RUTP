"""
Управляет отправкой данных: буферизация, окно перегрузки, повторные передачи.
"""
import asyncio
import logging
from collections import deque
from typing import Callable, Deque, Dict, Optional, List, Tuple
from .packet import Packet
from .timers import RetransmissionTimer
from .congestion import CongestionController
from .config import get_config
from .constants import TYPE_DATA, SAFE_PAYLOAD_IPV4, MAX_RETRANSMITS
from .utils import seq_leq, seq_before

logger = logging.getLogger(__name__)

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
        self._send_buffer: Deque[Packet] = deque()
        self._unacked: Dict[int, Packet] = {}
        self._timers: Dict[int, RetransmissionTimer] = {}
        self._retrans_count: Dict[int, int] = {}
        self._next_seq: int = 0
        self._peer_window: int = 65535
        self._recv_window: int = 65535
        self._congestion = CongestionController()
        self._max_retransmits = get_config()["max_retransmits"]
        self._closed = False

    def set_recv_window(self, window: int) -> None:
        self._recv_window = window

    def send(self, data: bytes) -> None:
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

    def _flush_packets(self) -> None:
        while self._send_buffer:
            bytes_in_flight = sum(len(pkt.payload) for pkt in self._unacked.values())
            if len(self._unacked) >= self._congestion.cwnd:
                break
            if self._peer_window > 0 and bytes_in_flight + len(self._send_buffer[0].payload) > self._peer_window:
                break
            if self._peer_window == 0:
                break
            pkt = self._send_buffer.popleft()
            self._send_with_retransmit(pkt)

    def _send_with_retransmit(self, pkt: Packet) -> None:
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
            logger.error("Max retransmits exceeded for seq=%d", seq)
            if self._on_max_retransmit:
                self._on_max_retransmit(seq)
            return
        logger.info("Retransmit seq=%d (attempt %d)", seq, count)
        self._congestion.on_loss()
        pkt.window = self._recv_window
        self._send_raw(pkt.serialize())

    def on_ack(self, ack_num: int, window: int, sack_blocks: Optional[List[Tuple[int, int]]] = None) -> None:
        """
        Обрабатывает ACK с необязательными SACK-блоками.
        """
        self._peer_window = window
        last_sent = (self._next_seq - 1) % 2**32
        self._congestion.on_ack(ack_num, last_sent)

        # кумулятивное подтверждение
        confirmed = [seq for seq in self._unacked if not seq_leq(ack_num, seq)]

        # выборочное подтверждение через SACK
        if sack_blocks:
            for seq in list(self._unacked.keys()):
                if seq in confirmed:
                    continue
                for start, end in sack_blocks:
                    if seq_leq(start, seq) and seq_before(seq, end):
                        confirmed.append(seq)
                        break

        for seq in confirmed:
            self._remove_packet(seq)
        self._flush_packets()

    def _remove_packet(self, seq: int) -> None:
        """Удалить пакет из неподтверждённых, остановить таймер, сбросить RTO."""
        self._unacked.pop(seq, None)
        self._retrans_count.pop(seq, None)
        t = self._timers.pop(seq, None)
        if t:
            t.stop()
            t.reset_rto()

    @property
    def window(self) -> int:
        return self._peer_window
