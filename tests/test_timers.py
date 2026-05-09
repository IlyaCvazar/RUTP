import asyncio
import pytest
from rutp.timers import RetransmissionTimer, KeepAliveTimer

@pytest.mark.asyncio
async def test_rto_fires(event_loop):
    called = False
    def cb():
        nonlocal called
        called = True
    timer = RetransmissionTimer(event_loop)
    timer._rto = 0.01
    timer.start(cb)
    await asyncio.sleep(0.05)
    assert called
    timer.stop()

@pytest.mark.asyncio
async def test_keepalive_periodic(event_loop):
    count = 0
    def cb():
        nonlocal count
        count += 1
    timer = KeepAliveTimer(event_loop, interval=0.02, callback=cb)
    timer.start()
    await asyncio.sleep(0.07)
    timer.stop()
    assert count >= 3
