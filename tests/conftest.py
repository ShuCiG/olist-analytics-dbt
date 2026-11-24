"""No network in tests/: a call to another host fails at once instead of retrying for minutes.

A test that forgot to fake a feed (Open-Meteo, BCB, BrasilAPI) used to sit in urllib3's retry
loop after a 429 and hang the pre-push run for 30+ minutes. Loopback stays open (local
servers in tests); child processes are not covered, only this interpreter.
"""

import socket
from typing import Any

import pytest

_connect = socket.socket.connect


def _is_local(address: Any) -> bool:
    host = address[0] if isinstance(address, tuple) and address else address
    return host in ("localhost", "127.0.0.1", "::1") or not isinstance(host, str)


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def guarded(self: socket.socket, address: Any) -> None:
        if not _is_local(address):
            raise RuntimeError(f"network call in a test: {address!r}; fake it (see fault_injection/feeds.py)")
        _connect(self, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)
