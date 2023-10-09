"""
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from queue import Queue
from subprocess import (
    DEVNULL,
    TimeoutExpired,
)
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
        # already be the case -- we make sure that no zombies
        # accumulate
        if runner.process is not None:
            proc = runner.process
            # ssk friendly to terminate (SIGTERM)
            proc.terminate()
            # let the process die and exhaust its output pipe
            # so it can be garbage collected properly.
            try:
                # give it 10s
                proc.communicate(timeout=10)
            except TimeoutExpired:
                # the process did not manage to end before the
                # timeout -> SIGKILL
                proc.kill()
                # we still need to exhaust its output pipes
                proc.communicate()
