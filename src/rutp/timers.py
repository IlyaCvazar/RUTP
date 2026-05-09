"""Таймеры повторной передачи и keep-alive."""
import asyncio
import logging
from typing import Callable, Optional
from .config import get_config
from .constants import KEEPALIVE          # <-- добавлен импорт

logger = logging.getLogger(__name__)

class RetransmissionTimer:
    """
    Таймер RTO с экспоненциальным отбоем, согласно RFC 6298.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        cfg = get_config()
        self._rto = cfg["rto"]
        self._min_rto = cfg["min_rto"]
        self._max_rto = cfg["max_rto"]
        self._loop = loop
        self._handle: Optional[asyncio.TimerHandle] = None
        self._callback: Optional[Callable] = None

    def start(self, callback: Callable) -> None:
        """Запустить таймер; при срабатывании вызовет callback и применит отбой."""
        self.stop()
        self._callback = callback
        self._handle = self._loop.call_later(self._rto, self._on_timeout)

    def _on_timeout(self) -> None:
        if self._callback:
            self._callback()
        self._rto = min(self._rto * 2, self._max_rto)

    def stop(self) -> None:
        if self._handle:
            self._handle.cancel()
            self._handle = None
        self._callback = None

    def reset_rto(self, new_rto: Optional[float] = None) -> None:
        """Сбросить RTO к исходному или заданному значению."""
        if new_rto is not None:
            self._rto = max(self._min_rto, min(self._max_rto, new_rto))
        else:
            self._rto = get_config()["rto"]


class KeepAliveTimer:
    """
    Периодический keep-alive таймер и zero-window probe (RFC 1122, раздел 4.2.3.6).

    Args:
        loop: asyncio event loop.
        interval: интервал между пробами (по умолчанию KEEPALIVE).
        callback: функция, вызываемая при срабатывании.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop,
                 interval: float = None,
                 callback: Callable = None) -> None:
        self._loop = loop
        self._interval = interval or KEEPALIVE
        self._callback = callback
        self._handle: Optional[asyncio.TimerHandle] = None
        self._running = False

    def start(self, callback: Optional[Callable] = None) -> None:
        """
        Запускает или перезапускает таймер.
        Если передан callback, он заменяет текущий.
        """
        self.stop()
        if callback:
            self._callback = callback
        if self._callback:
            self._running = True
            self._schedule()

    def _schedule(self) -> None:
        self._handle = self._loop.call_later(self._interval, self._fire)

    def _fire(self) -> None:
        if self._running and self._callback:
            self._callback()
            self._schedule()

    def stop(self) -> None:
        self._running = False
        if self._handle:
            self._handle.cancel()
            self._handle = None

    def reset_interval(self, new_interval: float) -> None:
        """Изменить интервал и перезапустить, если активен."""
        self._interval = new_interval
        if self._running:
            self.start()
