"""Все константы протокола RUTP."""

# Версия протокола (SemVer MAJOR.MINOR в одном 16-битном поле)
PROTOCOL_VERSION_MAJOR = 1
PROTOCOL_VERSION_MINOR = 0
PROTOCOL_VERSION = (PROTOCOL_VERSION_MAJOR << 8) | PROTOCOL_VERSION_MINOR

# Битовый флаг SYN в поле версии (15-й бит) – согласно спецификации
VERSION_SYN_BIT = 0x8000

# Типы пакетов
TYPE_DATA    = 0x01
TYPE_ACK     = 0x02
TYPE_CONTROL = 0x03

# Флаги (поле Flags)
FLAG_SYN = 0x01
FLAG_FIN = 0x02
FLAG_RST = 0x04
FLAG_ACK = 0x08

# Размеры
HEADER_SIZE = 18          # 2+1+1+4+4+4+2
MAX_PAYLOAD = 65535
UDP_HEADER  = 8
MTU_IPV4    = 576
MTU_IPV6    = 1280
SAFE_PAYLOAD_IPV4 = MTU_IPV4 - UDP_HEADER - HEADER_SIZE
SAFE_PAYLOAD_IPV6 = MTU_IPV6 - UDP_HEADER - HEADER_SIZE

# Таймеры (в секундах)
DEFAULT_RTO = 1.0
MIN_RTO     = 0.2
MAX_RTO     = 60.0
KEEPALIVE   = 15.0

# Окна
INITIAL_CWND     = 10          # RFC 5681
INITIAL_SSTHRESH = 65535
MAX_RECV_WINDOW  = 65535       # 16 бит на поле Window? На самом деле 32 бита, но лимитируем

# Максимальное число попыток передачи одного пакета
MAX_RETRANSMITS = 10
