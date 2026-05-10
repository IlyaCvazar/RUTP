
# RUTP — Reliable UDP Transport Protocol

**RUTP** is a custom reliable transport protocol over UDP, implemented in pure Python 3.7+ with `asyncio`.  
It provides TCP-like reliability, congestion control, flow control, and selective acknowledgements, while remaining lightweight and embeddable.

## Features

- **Reliable delivery** – automatic retransmission with exponential backoff (RFC 6298)
- **Congestion control** – NewReno algorithm (slow start, congestion avoidance, fast retransmit/recovery)
- **Flow control** – receiver window advertisement and zero‑window probing
- **Selective acknowledgements (SACK)** – up to 1000 reorder buffer blocks for efficient retransmission
- **Keep‑alive** – periodic probes keep idle connections alive
- **Asynchronous API** – built on `asyncio`, supports both client and server
- **Pluggable parameters** – RTO, max retransmits, etc., configurable via environment variables

## Installation

Requires Python 3.7 or later. No external dependencies.

```bash
git clone https://github.com/IlyaCvazar/RUTP.git
cd RUTP
pip install -e .
```

Or directly from the source:

```bash
pip wheel .
pip install rutp-1.0.0-*.whl
```

## Quick Start

### Echo Server

```python
import asyncio
from rutp import RUTPConnection

async def handle_client(conn: RUTPConnection):
    queue = asyncio.Queue()
    conn.on_data = lambda data: queue.put_nowait(data)
    try:
        while True:
            data = await queue.get()
            conn.send(data)   # echo back
    except:
        pass
    finally:
        conn.close()

async def main():
    loop = asyncio.get_event_loop()
    server = RUTPConnection(loop)
    server.on_connection = handle_client
    await server.listen(9000)
    print("Server listening on port 9000")
    await asyncio.Event().wait()

asyncio.run(main())
```

### Client

```python
import asyncio
from rutp import RUTPConnection

async def main():
    client = RUTPConnection(asyncio.get_event_loop())
    received = bytearray()
    done = asyncio.Event()
    client.on_data = lambda d: (received.extend(d), done.set())

    await client.connect('127.0.0.1', 9000)
    await asyncio.sleep(0.1)   # wait for handshake
    message = "Hello, RUTP!".encode()
    print("Sending:", message)
    client.send(message)

    try:
        await asyncio.wait_for(done.wait(), timeout=5.0)
        print("Response:", bytes(received))
    except asyncio.TimeoutError:
        print("No response")
    await client.close()

asyncio.run(main())
```

---

## API

### RUTPConnection

Класс `RUTPConnection` управляет жизненным циклом соединения.

**Конструктор**
```python
RUTPConnection(loop: asyncio.AbstractEventLoop, on_data: Optional[Callable[[bytes], None]] = None)
```
- `loop` — event loop asyncio.
- `on_data` — функция, вызываемая при получении новых данных. Можно задать и позже через атрибут `conn.on_data`.

**Атрибуты**
- `on_data` — Callable[[bytes], None] | None
- `on_connection` — Callable[[RUTPConnection], None] | None  
  Только для сервера: вызывается при входящем соединении, аргументом получает новый объект `RUTPConnection` для общения с клиентом.

**Методы**
- `await listen(port: int)` — открыть серверный UDP‑сокет на указанном порту.
- `await connect(host: str, port: int)` — подключиться к серверу.
- `send(data: bytes)` — отправить данные. Работает только в состоянии `ESTABLISHED`.
- `await close()` — начать закрытие соединения (FIN-рукопожатие).
- (внутренние) `_close_transport()` — принудительный разрыв.

**Состояния соединения** (перечисление `ConnState`):
`LISTEN`, `SYN_SENT`, `SYN_RECEIVED`, `ESTABLISHED`, `FIN_WAIT_1`, `FIN_WAIT_2`, `CLOSE_WAIT`, `LAST_ACK`, `TIME_WAIT`, `CLOSED`.

### Packet

Класс `Packet` представляет wire-формат сообщения. Обычно не требуется создавать экземпляры вручную, но может быть полезен для отладки.

**Поля:**
- `version` (int)
- `type` (int) — `TYPE_DATA` (0x01), `TYPE_ACK` (0x02), `TYPE_CONTROL` (0x03)
- `flags` (int) — битовая маска `FLAG_SYN`, `FLAG_ACK`, `FLAG_FIN`, `FLAG_RST`
- `seq_num` (int)
- `ack_num` (int)
- `window` (int)
- `payload` (bytes)

**Методы:**
- `serialize() -> bytes`
- `Packet.deserialize(data: bytes) -> Optional[Packet]`
- `is_syn_version(version: int) -> bool` (статический)
- `make_syn_version(base_version: int = PROTOCOL_VERSION) -> int` (статический)

### Константы и конфигурация

Все константы определены в модуле `rutp.constants`.  
Параметры, изменяемые пользователем, читаются из переменных окружения (через `rutp.config.get_config()`):

| Переменная окружения     | По умолчанию | Описание                                  |
|--------------------------|--------------|-------------------------------------------|
| `RUTP_RTO`               | 1.0          | Базовый тайм-аут повторной передачи (сек) |
| `RUTP_MIN_RTO`           | 0.2          | Минимальный RTO                           |
| `RUTP_MAX_RTO`           | 60.0         | Максимальный RTO                          |
| `RUTP_MAX_RETRANSMITS`   | 10           | Максимальное число попыток отправки пакета|

Пример: для более агрессивной ретрансмиссии можно задать `RUTP_RTO=0.5 RUTP_MAX_RETRANSMITS=5 python server.py`.

---

## Архитектура протокола

### Рукопожатие

Трёхстороннее рукопожатие:

1. Клиент → Сервер: `SYN` (seq=x)
2. Сервер → Клиент: `SYN+ACK` (seq=y, ack=x+1)
3. Клиент → Сервер: `ACK` (ack=y+1) — после этого обе стороны переходят в `ESTABLISHED`.

SYN‑пакет помечается 15-м битом поля версии (`VERSION_SYN_BIT`).

### Передача данных

Данные, переданные через `send()`, нарезаются на сегменты размером `SAFE_PAYLOAD_IPV4` (548 байт — минимальная гарантированная датаграмма IPv4).  
Каждый сегмент нумеруется 32‑битным порядковым номером, циклически (RFC 1982). Получатель собирает поток в правильном порядке, буферизуя неупорядоченные пакеты.

### Контроль перегрузки

Реализован алгоритм **NewReno** (RFC 5681):

- **Медленный старт**: `cwnd` увеличивается на 1 пакет за ACK, пока `cwnd < ssthresh`.
- **Избежание перегрузки**: `cwnd += 1/cwnd` за ACK.
- **Быстрая ретрансмиссия**: при получении 3 дублированных ACK `ssthresh` уменьшается вдвое, `cwnd = ssthresh + 3`, выполняется повторная отправка потерянного пакета без тайм‑аута.
- **Тайм‑аут (потеря)**: `ssthresh = max(cwnd/2, 2)`, `cwnd` сбрасывается до `INITIAL_CWND` (10).

Так же обрабатываются повторные передачи с экспоненциальным ростом RTO (`rto = min(rto*2, max_rto)`).

### Выборочные подтверждения (SACK)

Приёмник формирует ACK, в котором помимо кумулятивного `ack_num` перечисляются блоки успешно полученных, но не доставленных по порядку сегментов (`SACK blocks`).  
Каждый блок — пара `(start, end)`, упакованная в big‑endian (`!II`).  
Отправитель использует SACK для отметки полученных сегментов, чтобы не перепосылать их, когда тайм‑аут срабатывает только для одного потерянного.

### Управление потоком

Каждый ACK содержит поле `window` — количество свободного места в приёмном буфере (до `MAX_RECV_WINDOW = 65535` байт).  
Отправитель не должен посылать данные, если окно равно 0 или если суммарный объём неподтверждённых данных + размер следующего сегмента превышает объявленное окно.  
Если окно долго равно 0, срабатывает **zero‑window probing** — сервер отправляет keep‑alive пакеты, чтобы разбудить получателя.

### Keep‑alive

Каждые `KEEPALIVE` секунд (по умолчанию 15) соединение отправляет пустой ACK, если линия простаивает. Это предотвращает разрыв соединения на промежуточных NAT‑маршрутизаторах.

---

## Примеры

### Многоклиентский сервер

```python
import asyncio
from rutp import RUTPConnection

async def handle_client(conn):
    queue = asyncio.Queue()
    conn.on_data = queue.put_nowait
    try:
        while True:
            data = await queue.get()
            # обработайте data...
            conn.send(data)   # эхо
    except:
        pass
    finally:
        conn.close()

async def main():
    server = RUTPConnection(asyncio.get_event_loop())
    server.on_connection = lambda conn: asyncio.create_task(handle_client(conn))
    await server.listen(9000)
    await asyncio.Event().wait()

asyncio.run(main())
```

### Передача файла

```python
# клиент
with open('myfile.bin', 'rb') as f:
    client.send(f.read())
```

Протокол сам нарежет данные, отправит, перепошлёт потерянные куски и доставит приёмнику в правильном порядке.

---

## Тестирование

### Unit‑тесты

В директории `tests/` находятся unit‑тесты. Установите `pytest` и запустите:

```bash
pip install pytest
pytest tests/
```

Тесты покрывают:
- Арифметику последовательностей (wraparound)
- Сериализацию/десериализацию пакетов
- Перегрузку (NewReno)
- Таймеры RTO и keep‑alive
- Приёмник и буферизацию
- Отправитель и его взаимодействие с окном получателя
- Полное рукопожатие и передачу данных

### Интеграционный тест

В корне репозитория находится скрипт **`TestScript.py`** – он запускает эхо‑сервер и клиент в одном процессе и проверяет полный цикл работы протокола:

- рукопожатие (ESTABLISHED),
- обмен короткими сообщениями (меньше одного сегмента),
- данные размером >1 сегмента,
- передачу большого файла (100 КБ) с проверкой SHA‑256,
- корректное закрытие соединения (FIN‑ACK обмен).

Запуск:

```bash
python TestScript.py
```

Пример успешного вывода:

```
Test server listening on port 46677
  ✓ Handshake: client → ESTABLISHED
  ✓ Small echo: bytearray(b'Hello, RUTP!') == b'Hello, RUTP!'
  ✓ Medium echo (1500 bytes): correct
  ✓ Large file (100000 bytes): SHA‑256 matches
  ✓ Close: client state = FIN_WAIT_2

========================================
Total: 5 passed, 0 failed
```

Если все пять проверок пройдены, библиотека полностью работоспособна на всех этапах соединения.

---

## Лицензия и атрибуция

Этот проект распространяется под специальной лицензией **RUTP License**, которая **требует явного указания**:

- названия протокола: **RUTP (Reliable UDP Transport Protocol)**,
- имени создателя: **Илья Околелов**.

Уведомление должно быть видимым для конечного пользователя (документация, интерфейс, экран загрузки и т.п.).  
Полный текст лицензии: файл [LICENSE.txt](LICENSE.txt).

© 2026 Илья Околелов. Все права защищены.

---

## Разработка

Исходный код открыт. Вы можете предлагать улучшения через pull‑запросы. Убедитесь, что добавленные изменения покрыты тестами и не нарушают работу существующего протокола.

Структура проекта:
```
├── src/rutp/        # пакет протокола
│   ├── __init__.py
│   ├── config.py
│   ├── congestion.py
│   ├── connection.py
│   ├── constants.py
│   ├── flow.py
│   ├── packet.py
│   ├── receiver.py
│   ├── sender.py
│   ├── timers.py
│   └── utils.py
├── tests/           # тесты
├── pyproject.toml
├── LICENSE.txt
└── README.md
```

## Вопросы и обратная связь

Если у вас возникли проблемы или предложения, создавайте issue в репозитории проекта.  
Автор: Илья (github: IlyaCvazar)
```
