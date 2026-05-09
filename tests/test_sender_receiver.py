import asyncio
import pytest
from rutp.sender import Sender
from rutp.receiver import Receiver
from rutp.packet import Packet
from rutp.constants import TYPE_ACK, TYPE_DATA

@pytest.fixture
def loop(event_loop):
    return event_loop

def test_sender_send_and_ack(loop):
    sent_packets = []
    def send_cb(data):
        sent_packets.append(Packet.deserialize(data))
    s = Sender(loop, send_cb)
    s._congestion.cwnd = 10
    s.send(b'data')
    assert len(sent_packets) == 1
    # имитация ACK
    ack_num = sent_packets[0].seq_num + 1
    s.on_ack(ack_num, 65535)
    assert len(s._unacked) == 0

def test_receiver_reorder():
    delivered = []
    r = Receiver(delivered.append)
    p1 = Packet(type=TYPE_DATA, seq_num=1, payload=b'B')
    p0 = Packet(type=TYPE_DATA, seq_num=0, payload=b'A')
    ack1 = r.process_packet(p1)  # будущий
    assert ack1 is not None
    assert ack1.ack_num == 0  # ожидает 0
    ack2 = r.process_packet(p0)
    assert ack2.ack_num == 2
    assert delivered == [b'A', b'B']
    
def test_sender_respects_peer_window(loop):
    sent = []
    s = Sender(loop, lambda d: sent.append(d))
    s._congestion.cwnd = 100    # не ограничивает
    s.set_recv_window(65535)
    s.on_ack(0, 4)              # окно получателя = 4 байта
    s.send(b'Hello, World!')    # 13 байт, но отправляем по MSS
    # Должны отправиться только первые 4 байта? Нет, мы режем на куски mss,
    # но кусок всё равно может быть больше окна. В нашем коде mss = 548.
    # В реальности чанк 548 > 4 -> не отправится вообще.
    # Исправляем mss для теста, передав маленький mss или используем send с маленьким куском.
    # При текущей реализации send режет на куски по SAFE_PAYLOAD_IPV4 (548).
    # Отправится 0 пакетов, т.к. bytes_in_flight + 548 > 4. Так и должно быть.
    assert len(sent) == 0
    s.on_ack(0, 600)
    s.send(b'hello')
    # теперь должно уйти
    assert len(sent) == 1
