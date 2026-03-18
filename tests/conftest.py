
import time
import subprocess
import signal
import pytest


@pytest.fixture(scope="session")
def spade_broker():
    print("Starting spade XMPP server")
    proc = subprocess.Popen(["spade", "run", "--host", "127.0.0.1", "--memory"])
    time.sleep(2)
    yield "localhost", 5222
    print("closing Spade XMPP server")
    proc.send_signal(signal.SIGINT)
    proc.wait()
    print("Spade XMPP server closed")


