"""The one test in here that fails if constraint 4 stops being enforced.

Every other test runs offline because nothing under `src/` opens a socket, which
is a property of the code and not a proof of it. The proof was four words in
`pyproject.toml`:

    addopts = "-q --disable-socket --allow-unix-socket"

Deleting them left all 305 tests of the day passing, the badge green, and nothing
in the repository noticing. What was in `tests/__pycache__` instead was a compiled
`test_zz_socket_probe` with no source file beside it, which is what checking this
once by hand and then deleting the check looks like from the outside.

Measured rather than assumed while writing this: the suite also passes with
`--allow-unix-socket` taken away, so `--disable-socket` is the flag carrying the
constraint today. Asserting the other one would be asserting an intention.

Both tests pin the warning text as well as the exception, because that text is
what `README.md` quotes. A pytest-socket release that reworded it should move
both, and the assertion is how that gets noticed.
"""

from __future__ import annotations

import socket

import pytest
from pytest_socket import SocketBlockedError


def test_a_tcp_socket_cannot_be_created_inside_a_test(recwarn):
    with pytest.raises(SocketBlockedError):
        socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    assert "A test tried to use socket.socket." in str(recwarn.pop(UserWarning).message)


def test_the_refusal_lands_before_any_packet(recwarn):
    """198.51.100.1 is TEST-NET-2 and port 9 is discard, so this would hang if it ran.

    It does not get that far, and not where I expected: the address is already
    an IP literal, and `create_connection` still calls `getaddrinfo` on it
    first, so that is the guard that fires. Either way the refusal happens at
    the line that wrote the call rather than in a timeout, which is what makes a
    stray network call in this suite a legible failure.
    """
    with pytest.raises(SocketBlockedError):
        socket.create_connection(("198.51.100.1", 9), timeout=0.01)
    assert "A test tried to use socket.getaddrinfo." in str(recwarn.pop(UserWarning).message)
