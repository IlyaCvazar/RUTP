from rutp.congestion import CongestionController

def test_slow_start():
    cc = CongestionController()
    cc.cwnd = 1
    cc.ssthresh = 100
    cc.on_ack(1, 0)  # new ACK
    assert cc.cwnd == 2

def test_fast_retransmit():
    cc = CongestionController()
    cc.cwnd = 10
    cc.ssthresh = 100
    # 3 дубликата
    for _ in range(3):
        cc.on_ack(cc._last_ack, 5)  # dup
    assert cc.cwnd == cc.ssthresh + 3
    assert cc._recovery == True

def test_timeout_loss():
    cc = CongestionController()
    cc.cwnd = 20
    cc.on_loss()
    assert cc.cwnd == 1
