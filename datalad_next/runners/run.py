"""
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from subprocess import DEVNULL
from typing import (
    IO,
)

from datalad.runner.nonasyncrunner import _ResultGenerator

from . import (
    Protocol,
    ThreadedRunner,
)


@contextmanager
def run(
    cmd: list,
    protocol_class: Protocol,
    *,
    cwd: Path | None = None,
    input: int | IO | bytes | Queue[bytes | None] | None = None,
    # only generator protocols make sense for timeout, and timeouts are
    # only checked when the generator polls
    timeout: float | None = None,
) -> dict | _ResultGenerator:
    runner = ThreadedRunner(
        cmd=cmd,
        protocol_class=protocol_class,
        stdin=DEVNULL if input is None else input,
        cwd=cwd,
        timeout=timeout,
    )
    try:
        yield runner.run()
    finally:
        # if we get here the subprocess has no business running
        # anymore. When run() exited normally, this should
        # already be the case -- we make sure that now zombies
        # accumulate
        if runner.process is not None:
            # TODO figure out what is the most graceful way to
            # tell a process to stop. Possibly
            # - process.terminate()
            # - process.wait(with timeout)
            # - catch TimeoutExpired exception and process.kill()
            #
            # send it the KILL signal
            runner.process.kill()
            # wait till the OS has reported the process dead
            runner.process.wait()
