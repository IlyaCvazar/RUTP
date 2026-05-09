"""
RUTP — Reliable UDP Transport Protocol.

Надёжный транспортный протокол поверх UDP с контролем перегрузки (NewReno),
управлением потоком и поддержкой жизненного цикла соединения.
"""

__version__ = "1.0.0"

from .packet import Packet
from .connection import RUTPConnection, ConnState
from .constants import (
    PROTOCOL_VERSION,
    PROTOCOL_VERSION_MAJOR,
    PROTOCOL_VERSION_MINOR,
)

__all__ = [
    "__version__",
    "Packet",
    "RUTPConnection",
    "ConnState",
    "PROTOCOL_VERSION",
    "PROTOCOL_VERSION_MAJOR",
    "PROTOCOL_VERSION_MINOR",
]
