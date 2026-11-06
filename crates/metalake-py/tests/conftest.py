"""What several test files share."""

import _thread
import contextlib
import os
import subprocess
import threading

import pytest


@pytest.fixture
def interrupt_polars():
    """Interrupts, after a second, what Polars reads in the `with` it opens.

    Polars 1 holds the GIL as it reads, and a thread of Python needs it, so a timer might never run and the read might
    never end. What comes from outside, as from a terminal, needs no GIL. Windows has no such signal for one process.
    """
    polars = pytest.importorskip("polars")
    holds_the_gil = int(polars.__version__.split(".")[0]) < 2
    if holds_the_gil and os.name == "nt":
        pytest.skip("Polars 1 holds the GIL, and Windows cannot signal one process from outside")

    @contextlib.contextmanager
    def interrupt():
        if holds_the_gil:
            sender = subprocess.Popen(["sh", "-c", f"sleep 1; kill -INT {os.getpid()}"])
            finished = sender.wait
        else:
            sender = threading.Timer(1, _thread.interrupt_main)
            sender.start()
            finished = sender.join
        try:
            yield
        finally:
            finished()

    return interrupt
