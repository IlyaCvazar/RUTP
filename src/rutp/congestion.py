"""
Алгоритм управления перегрузкой, основанный на TCP NewReno (RFC 5681).
Поддерживает медленный старт, избежание перегрузки, быструю повторную передачу и восстановление.
"""
import logging
from .constants import INITIAL_CWND, INITIAL_SSTHRESH
from .utils import seq_diff

logger = logging.getLogger(__name__)

class CongestionController:
    def __init__(self) -> None:
        self.cwnd = float(INITIAL_CWND)        # окно перегрузки в пакетах
        self.ssthresh = INITIAL_SSTHRESH
        self.dup_ack_count = 0
        self._last_ack = -1                     # последний обработанный ACK
        self._recovery = False                  # фаза fast recovery

    def on_ack(self, ack_num: int, last_sent: int) -> None:
        """
        Обновление состояния при получении ACK.
        ack_num: номер ACK (все пакеты < ack_num подтверждены)
        last_sent: наибольший отправленный seq (для детекта дубликатов)
        """
        if not self._is_new_ack(ack_num, last_sent):
            self.dup_ack_count += 1
            if self.dup_ack_count == 3:
                self._fast_retransmit()
            return

        # Новый ACK
        self.dup_ack_count = 0
        if self._recovery:
            self._recovery = False
            self.cwnd = self.ssthresh
        if self.cwnd < self.ssthresh:
            self.cwnd += 1          # slow start
        else:
            self.cwnd += 1.0 / self.cwnd   # congestion avoidance

    def _is_new_ack(self, ack_num: int, last_sent: int) -> bool:
        new = seq_diff(self._last_ack, ack_num) > 0
        if new:
            self._last_ack = ack_num
        return new

    def _fast_retransmit(self) -> None:
        self.ssthresh = max(self.cwnd // 2, 2.0)
        self.cwnd = self.ssthresh + 3  # RFC 2001 fast recovery
        self._recovery = True
        logger.debug("Fast retransmit triggered, ssthresh=%d cwnd=%d", self.ssthresh, self.cwnd)

    def on_loss(self) -> None:
        """Обработка тайм-аута (потеря пакета)."""
        self.ssthresh = max(self.cwnd // 2, 2.0)
        self.cwnd = float(INITIAL_CWND)
        self.dup_ack_count = 0
        self._recovery = False
