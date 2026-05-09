import pytest
from rutp.packet import Packet
from rutp.constants import PROTOCOL_VERSION, TYPE_DATA, FLAG_SYN, VERSION_SYN_BIT

def test_serialize_deserialize():
    p = Packet(version=PROTOCOL_VERSION, type=TYPE_DATA, flags=0,
               seq_num=100, ack_num=200, window=65535, payload=b'hello')
    data = p.serialize()
    p2 = Packet.deserialize(data)
    assert p2 is not None
    assert p2.seq_num == 100
    assert p2.payload == b'hello'

def test_payload_too_large():
    with pytest.raises(ValueError):
        Packet(payload=b'x' * 65536).serialize()

def test_syn_version_bit():
    v = Packet.make_syn_version()
    assert v & VERSION_SYN_BIT
    assert Packet.is_syn_version(v)
