from rutp.utils import seq_before, seq_after, seq_diff, seq_add

def test_wraparound():
    a = 2**32 - 10
    b = 5
    assert seq_before(a, b) == True
    assert seq_after(b, a) == True
    assert seq_diff(a, b) == 15
    assert seq_add(a, 20) == 10
