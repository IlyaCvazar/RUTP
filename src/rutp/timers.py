"""Таймеры повторной передачи и keep-alive."""
import asyncio
import logging
from typing import Callable, Optional
from .config import get_config
from .constants import KEEPALIVE

logger = logging.getLogger(__name__)


class RetransmissionTimer:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        cfg = get_config()
        self._base_rto = cfg["rto"]
        self._rto = self._base_rto
        self._min_rto = cfg["min_rto"]
        self._max_rto = cfg["max_rto"]
        self._loop = loop
        self._handle: Optional[asyncio.TimerHandle] = None
        self._callback: Optional[Callable] = None
        self._backoff_count = 0
        self._is_running = False

    def start(self, callback: Callable) -> None:
        """Запустить таймер. Если уже запущен, остановить и перезапустить."""
        self.stop()
        self._callback = callback
        self._is_running = True
        self._backoff_count = 0
        self._rto = self._base_rto
        self._schedule()

    def _schedule(self) -> None:
        if self._is_running and self._callback:
            self._handle = self._loop.call_later(self._rto, self._on_timeout)

    def _on_timeout(self) -> None:
        if not self._is_running:
            return
        # Вызов callback (ретрансмиссия)
        if self._callback:
            self._callback()
        # Увеличиваем RTO (экспоненциальный отбой)
        self._backoff_count += 1
        new_rto = self._base_rto * (2 ** self._backoff_count)
        self._rto = min(new_rto, self._max_rto)
        # Перезапускаем таймер с новым RTO
        self._schedule()

    def stop(self) -> None:
        self._is_running = False
        if self._handle:
            self._handle.cancel()
            self._handle = None
        self._callback = None
        self._backoff_count = 0
        self._rto = self._base_rto

    def reset_rto(self, new_rto: Optional[float] = None) -> None:
        """Сбросить базовый RTO (действует при следующем start)."""
        if new_rto is not None:
            self._base_rto = max(self._min_rto, min(self._max_rto, new_rto))
        else:
            self._base_rto = get_config()["rto"]
        # Если таймер работает, не меняем текущий RTO до следующего тайм-аута
        if not self._is_running:
            self._rto = self._base_rto
            self._backoff_count = 0

    def restart(self, callback: Optional[Callable] = None) -> None:
        """Остановить и запустить заново с текущим RTO (без сброса отбоя)."""
        if callback:
            self._callback = callback
        if self._callback:
            was_running = self._is_running
            self.stop()
            self._is_running = was_running  # restore?
            # Лучше просто перезапустить с текущим _rto (не сбрасывая backoff)
            self._is_running = True
            self._schedule()


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
