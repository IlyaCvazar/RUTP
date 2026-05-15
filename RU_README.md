
<p align="center">
  <a href="README.md">
    <img src="https://img.shields.io/badge/English-README-blue?style=for-the-badge&logo=readthedocs" alt="English">
  </a>
</p>
# RUTP — простой и надёжный протокол поверх UDP

**RUTP** (Reliable UDP Transport Protocol) — это библиотека на Python, которая добавляет **надёжность** и **управление потоком** к обычному протоколу UDP.  
Она работает поверх `asyncio` и позволяет писать сетевые приложения (чат, передача файлов) так же просто, как с TCP, но на базе UDP.

## Зачем это нужно?

- UDP сам по себе **не гарантирует** доставку пакетов — они могут теряться, приходить не по порядку или дублироваться.
- RUTP добавляет всё, что есть в TCP: повторную отправку потерянных пакетов, сборку в правильном порядке, контроль перегрузки (чтобы не забивать сеть) и управление потоком (чтобы получатель успевал обрабатывать данные).
- В отличие от TCP, RUTP работает поверх UDP, что иногда удобно для обхода ограничений или написания нестандартных протоколов.

## Для кого эта библиотека?

Для Python-разработчиков, которые хотят:
- понять, как устроены надёжные протоколы;
- использовать асинхронное сетевое программирование (`asyncio`);
- написать простой чат или игру с гарантированной доставкой сообщений.

## Установка

```bash
git clone https://github.com/IlyaCvazar/RUTP.git
cd RUTP
pip install -e .
```

## Основные понятия (для новичков)

- **Пакет** — небольшой кусочек данных, который отправляется по сети. В RUTP каждый пакет имеет номер (`seq_num`).
- **Рукопожатие (handshake)** — обмен тремя специальными пакетами (`SYN`, `SYN-ACK`, `ACK`), чтобы установить соединение. После этого можно отправлять данные.
- **Окно перегрузки (cwnd)** — сколько пакетов можно отправить, не дожидаясь подтверждения. Если окно маленькое, отправитель ждёт, чтобы не перегружать сеть.
- **SACK (Selective Acknowledgment)** — механизм, при котором получатель сообщает не только последний полученный пакет, но и список промежутков пакетов, которые уже пришли. Это помогает быстрее понять, что именно нужно переслать заново.
- **Keep‑alive** — периодическая отправка пустых пакетов, чтобы соединение не разорвалось из‑за таймаута (например, если за NAT).
- **Persist‑таймер** — специальный таймер, который запускается, когда у получателя закончилось место (окно = 0). Он периодически «щупает» получателя, чтобы узнать, не освободилось ли место.

## Быстрый старт

### Эхо-сервер (обслуживает много клиентов)

```python
import asyncio
from rutp import RUTPServer, RUTPConnection

async def handle_client(conn: RUTPConnection):
    queue = asyncio.Queue()
    conn.on_data = lambda data: queue.put_nowait(data)
    try:
        while True:
            data = await queue.get()
            conn.send(data)   # эхо
    except asyncio.CancelledError:
        conn.close()
        raise

async def main():
    loop = asyncio.get_running_loop()
    # Важно: обёртываем корутину в create_task
    server = RUTPServer(loop, on_connection=lambda conn: asyncio.create_task(handle_client(conn)))
    await server.listen(8888)
    print("Сервер запущен на порту 8888")
    await asyncio.Event().wait()

asyncio.run(main())
```

### Клиент, отправляющий сообщение

```python
import asyncio
from rutp import RUTPConnection

async def main():
    loop = asyncio.get_running_loop()
    client = RUTPConnection(loop)

    await client.connect('127.0.0.1', 8888)
    await asyncio.sleep(0.1)   # ждём завершения рукопожатия

    received = bytearray()
    done = asyncio.Event()
    client.on_data = lambda d: (received.extend(d), done.set())

    client.send(b'Hello, RUTP!')

    try:
        await asyncio.wait_for(done.wait(), timeout=3.0)
        print("Ответ сервера:", received.decode())
    except asyncio.TimeoutError:
        print("Ответ не получен")

    await client.close()

asyncio.run(main())
```

## API (простыми словами)

### `RUTPServer` – сервер

```python
server = RUTPServer(loop, on_connection=обработчик)
await server.listen(порт)
```

- `on_connection` – вызывается для каждого нового подключения. Получает объект `RUTPConnection`.  
  **Важно:** если ваш обработчик — корутина (`async def`), оберните его в `asyncio.create_task`, например:  
  `RUTPServer(loop, on_connection=lambda conn: asyncio.create_task(my_handler(conn)))`.
- `listen(port)` – начинает слушать UDP-порт.

### `RUTPConnection` – одно соединение

**Создание (клиент или серверная сторона)**  
```python
conn = RUTPConnection(loop, on_data=обработчик_данных)
```

**Методы:**
- `await connect(host, port)` – подключиться к серверу (клиентский режим).
- `send(data)` – отправить данные (байты). Будут автоматически нарезаны на пакеты.
- `await close()` – корректно завершить соединение (отправить `FIN`).
- `abort()` – немедленно закрыть соединение, не дожидаясь подтверждений.

**Свойства:**
- `on_data` – можно назначить функцию, которая будет вызываться при получении данных.
- `on_close` – можно назначить функцию, вызываемую при закрытии соединения.

**Состояния (`conn._state`):**
- `CLOSED` – начальное / закрытое.
- `SYN_SENT` – клиент отправил `SYN`, ждёт ответа.
- `SYN_RECEIVED` – сервер получил `SYN`, отправил `SYN-ACK`.
- `ESTABLISHED` – соединение установлено, можно обмениваться данными.
- `FIN_WAIT_1`, `FIN_WAIT_2`, `CLOSE_WAIT`, `LAST_ACK`, `TIME_WAIT` – состояния при закрытии (аналогично TCP).

### `Packet` – структура пакета (обычно не требуется)

- `serialize()` – превратить пакет в байты для отправки.
- `Packet.deserialize(байты)` – восстановить пакет из полученных байтов.
- Поля: `seq_num` (номер пакета), `ack_num` (подтверждение), `flags` (SYN, FIN, RST, ACK), `payload` (данные).

## Пример: простой чат с регистрацией (много клиентов)

**Сервер** (сохраняет пользователей и пересылает сообщения):

```python
import asyncio
import json
from rutp import RUTPServer, RUTPConnection

users = {}

async def client_handler(conn: RUTPConnection):
    queue = asyncio.Queue()
    conn.on_data = lambda d: queue.put_nowait(d)
    username = None

    try:
        while True:
            data = await queue.get()
            msg = json.loads(data.decode())
            if msg['cmd'] == 'register':
                username = msg['username']
                users[username] = conn
                conn.send(json.dumps({'type': 'ok', 'message': f'Welcome {username}'}).encode())
            elif msg['cmd'] == 'message':
                target = msg['to']
                if target in users:
                    users[target].send(json.dumps({'from': username, 'text': msg['text']}).encode())
                else:
                    conn.send(json.dumps({'type': 'error', 'message': f'User {target} not found'}).encode())
    except asyncio.CancelledError:
        if username:
            users.pop(username, None)
        conn.close()
        raise

async def main():
    loop = asyncio.get_running_loop()
    server = RUTPServer(loop, on_connection=lambda conn: asyncio.create_task(client_handler(conn)))
    await server.listen(8888)
    await asyncio.Event().wait()

asyncio.run(main())
```

**Клиент** (консольный):

```python
import asyncio
import json
import sys
from rutp import RUTPConnection

async def main():
    loop = asyncio.get_running_loop()
    client = RUTPConnection(loop)
    await client.connect('127.0.0.1', 8888)
    await asyncio.sleep(0.1)  # ждём завершения handshake

    queue = asyncio.Queue()
    client.on_data = lambda data: queue.put_nowait(data)

    async def receive():
        while True:
            data = await queue.get()
            try:
                msg = json.loads(data.decode())
                if 'from' in msg:
                    print(f"\n[Сообщение от {msg['from']}]: {msg['text']}\n> ", end='', flush=True)
                elif msg.get('type') == 'ok':
                    print(f"\n[OK] {msg['message']}\n> ", end='', flush=True)
            except:
                pass

    asyncio.create_task(receive())

    async def ainput():
        return await loop.run_in_executor(None, sys.stdin.readline)

    print("Чат. Команды: reg <имя>, send <кому> <текст>, exit")
    while True:
        line = await ainput()
        if not line:
            break
        line = line.strip()
        if line.startswith('reg '):
            name = line.split()[1]
            client.send(json.dumps({'cmd': 'register', 'username': name}).encode())
        elif line.startswith('send '):
            parts = line.split(maxsplit=2)
            if len(parts) == 3:
                _, to, text = parts
                client.send(json.dumps({'cmd': 'message', 'to': to, 'text': text}).encode())
        elif line == 'exit':
            break

    await client.close()

asyncio.run(main())
```

## Настройка через переменные окружения

Можно менять параметры без правки кода:

```bash
export RUTP_RTO=0.5          # базовый тайм-аут повторной отправки (сек)
export RUTP_MAX_RETRANSMITS=5 # сколько раз пересылать пакет перед разрывом
python server.py
```

## Как это работает внутри (очень кратко)

1. **Отправка**: данные разбиваются на сегменты (максимум 548 байт), каждый получает порядковый номер (`seq_num`). Пакет помещается в буфер неподтверждённых и запускается таймер.
2. **Получение**: если номер пакета ожидаемый – данные отдаются приложению, иначе сохраняются в буфер переупорядочивания.
3. **Подтверждения (ACK)**: получатель отправляет `ACK` с номером следующего ожидаемого пакета. Если есть дырки – добавляет SACK-блоки.
4. **Потеря**: если таймер истёк, пакет отправляется снова, а тайм-аут увеличивается вдвое (экспоненциальный отбой).
5. **Три дублированных ACK** → быстрая повторная передача (fast retransmit) и уменьшение окна перегрузки.
6. **Нулевое окно** → включается persist-таймер, который раз в N секунд отправляет зондирующий пакет.

## Тестирование

В папке `tests/` есть unit-тесты. Запуск:

```bash
pip install pytest
pytest tests/
```

## Лицензия и обязательное указание авторства

RUTP распространяется под **лицензией RUTP**, которая **требует упоминания**:

- названия протокола: **«RUTP (Reliable UDP Transport Protocol)»**;
- имени создателя: **«Илья Околелов»** (или Ilya Okolelov).

Упоминание должно быть в документации, интерфейсе программы или другом заметном месте для конечного пользователя.

Полный текст лицензии: [LICENSE.txt](LICENSE.txt)

© 2026 Илья Околелов

## Вопросы и обратная связь

Задавайте вопросы через Issues на GitHub: [IlyaCvazar/RUTP](https://github.com/IlyaCvazar/RUTP)
