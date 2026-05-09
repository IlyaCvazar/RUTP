"""
Приёмник: сборка потока в правильном порядке, генерация ACK с SACK-блоками.
"""
import logging
from typing import Callable, Optional, Dict, List, Tuple
from .packet import Packet
from .flow import ReceiveWindow
from .constants import TYPE_DATA
from .utils import seq_diff

logger = logging.getLogger(__name__)

class Receiver:
    def __init__(self, deliver_callback: Callable[[bytes], None]) -> None:
        self._deliver: Callable[[bytes], None] = deliver_callback
        self._buffer: Dict[int, bytes] = {}
        self._next_expected: int = 0
        self._flow: ReceiveWindow = ReceiveWindow()

    def process_packet(self, pkt: Packet) -> Optional[Packet]:
        if pkt.type != TYPE_DATA:
            return None

        seq = pkt.seq_num
        if seq_diff(seq, self._next_expected) >= 2**31:
            return self._build_ack()

        payload = pkt.payload
        if len(payload) == 0:
            return self._build_ack()

        if seq == self._next_expected:
            self._deliver(payload)
            self._flow.remove_data(len(payload))
            self._next_expected = (self._next_expected + 1) % 2**32
            while self._next_expected in self._buffer:
                data = self._buffer.pop(self._next_expected)
                self._deliver(data)
                self._flow.remove_data(len(data))
                self._next_expected = (self._next_expected + 1) % 2**32
        elif seq_diff(self._next_expected, seq) > 0:
            if len(self._buffer) < 1000:
                self._buffer[seq] = payload
                self._flow.add_data(len(payload))
            else:
                logger.warning("Reorder buffer overflow")
        return self._build_ack()

    def _build_ack(self) -> Packet:
        """Формирует ACK с SACK-блоками на основе содержимого буфера."""
        sack_blocks = self._compute_sack_blocks()
        payload = self._encode_sack_blocks(sack_blocks) if sack_blocks else b''
        return Packet(
            version=0,
            type=0x02,
            flags=0,
            ack_num=self._next_expected,
            window=self._flow.window_available(),
            payload=payload
        )

    def _compute_sack_blocks(self) -> List[Tuple[int, int]]:
        """Возвращает список непрерывных интервалов (start, end) номеров seq в буфере."""
        if not self._buffer:
            return []
        sorted_seqs = sorted(self._buffer.keys())
        blocks = []
        start = sorted_seqs[0]
        end = (start + 1) % 2**32
        for seq in sorted_seqs[1:]:
            if seq == end:          # продляем интервал
                end = (end + 1) % 2**32
            else:
                blocks.append((start, end))
                start = seq
                end = (seq + 1) % 2**32
        blocks.append((start, end))
        return blocks

    @staticmethod
    def _encode_sack_blocks(blocks: List[Tuple[int, int]]) -> bytes:
        import struct
        data = b''
        for start, end in blocks:
            data += struct.pack('!II', start, end)
        return data

    def window_available(self) -> int:
        return self._flow.window_available()
