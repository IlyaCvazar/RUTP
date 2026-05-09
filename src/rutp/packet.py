"""
Пакет протокола RUTP.

Wire format (все поля в big-endian):
    0                   1                   2                   3
    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  ProtocolVersion (16)         |  Type (8)       |   Flags (8)  |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Sequence Number (32)                                          |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Acknowledgement Number (32)                                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  Window (32)                   |  Length (16)   |   Payload...
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
"""
import struct
from typing import Optional
from .constants import HEADER_SIZE, MAX_PAYLOAD, VERSION_SYN_BIT, PROTOCOL_VERSION

class Packet:
    """Основной класс пакета."""
    __slots__ = ('version', 'type', 'flags', 'seq_num', 'ack_num', 'window', 'payload')
    HEADER_FORMAT = '!HBB LLI H'  # ! = network byte order
    _STRUCT = struct.Struct(HEADER_FORMAT)
    HEADER_SIZE = HEADER_SIZE

    def __init__(self,
                 version: int = PROTOCOL_VERSION,
                 type: int = 0,
                 flags: int = 0,
                 seq_num: int = 0,
                 ack_num: int = 0,
                 window: int = 0,
                 payload: bytes = b'') -> None:
        self.version = version
        self.type = type
        self.flags = flags
        self.seq_num = seq_num
        self.ack_num = ack_num
        self.window = window
        self.payload = payload

    def serialize(self) -> bytes:
        """Сериализация в байты для отправки. Вызывает ValueError при слишком длинном payload."""
        if len(self.payload) > MAX_PAYLOAD:
            raise ValueError(f"Payload too long: {len(self.payload)} > {MAX_PAYLOAD}")
        header = self._STRUCT.pack(
            self.version, self.type, self.flags,
            self.seq_num, self.ack_num, self.window,
            len(self.payload)
        )
        return header + self.payload

    @classmethod
    def deserialize(cls, data: bytes) -> Optional['Packet']:
        """Создание пакета из полученных данных. Возвращает None при ошибке."""
        if len(data) < cls.HEADER_SIZE:
            return None
        header = data[:cls.HEADER_SIZE]
        payload = data[cls.HEADER_SIZE:]
        try:
            version, ptype, flags, seq, ack, window, length = cls._STRUCT.unpack(header)
        except struct.error:
            return None
        if len(payload) != length:
            return None
        return cls(version, ptype, flags, seq, ack, window, payload[:length])

    @staticmethod
    def is_syn_version(version: int) -> bool:
        """Проверяет, установлен ли 15-й бит версии (флаг SYN)."""
        return bool(version & VERSION_SYN_BIT)

    @staticmethod
    def make_syn_version(base_version: int = PROTOCOL_VERSION) -> int:
        """Устанавливает 15-й бит для SYN пакета."""
        return base_version | VERSION_SYN_BIT
