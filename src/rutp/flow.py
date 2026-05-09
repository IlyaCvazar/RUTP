"""Приёмное окно для flow control."""
from .constants import MAX_RECV_WINDOW

class ReceiveWindow:
    def __init__(self, max_window: int = MAX_RECV_WINDOW) -> None:
        self._max = max_window
        self._bytes_in_buffer = 0

    def window_available(self) -> int:
        """Сколько места осталось в приёмном буфере."""
        return max(0, self._max - self._bytes_in_buffer)

    def add_data(self, length: int) -> None:
        self._bytes_in_buffer += length

    def remove_data(self, length: int) -> None:
        self._bytes_in_buffer = max(0, self._bytes_in_buffer - length)

    def is_full(self) -> bool:
        return self._bytes_in_buffer >= self._max
