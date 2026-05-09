from rutp.flow import ReceiveWindow

def test_window_update():
    rw = ReceiveWindow(1000)
    assert rw.window_available() == 1000
    rw.add_data(600)
    assert rw.window_available() == 400
    rw.remove_data(600)
    assert rw.window_available() == 1000
