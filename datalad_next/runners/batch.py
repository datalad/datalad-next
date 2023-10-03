"""Helpers to execute batch commands

Some of the functionality provided by this module depends on specific
"generator" flavors of runner protocols, and additional dedicated
low-level tooling:

.. currentmodule:: datalad_next.runners.batch
.. autosummary::
   :toctree: generated

    StdOutCaptureGeneratorProtocol
    GeneratorAnnexJsonProtocol
    _ResultGenerator
"""

from __future__ import annotations

from contextlib import contextmanager

from pathlib import Path

from queue import Queue

from typing import (
    Any,
    Generator,
)

from datalad.runner.nonasyncrunner import _ResultGenerator
from datalad.support.annexrepo import GeneratorAnnexJsonProtocol

from . import (
    Protocol,
    ThreadedRunner,
)
from .protocols import StdOutCaptureGeneratorProtocol


class BatchProcess:
    """Representation of a (running) batch process

    An instance of this type is produced by any of the context manager variants
    in this module. It is a convenience wrapper around an instance of a
    :class:`_ResultGenerator` that is produced by a :meth:`ThreadedRunner.run`.

    A batch process instanced is used by passing ``bytes`` input to its
    ``__call__()`` method, and receiving the batch output as return value.

    While the process is still running, it ``return_code`` property will
    be ``None``. After the has terminated, the property will contain the
    respective exit code.
    """
    def __init__(self, rgen: _ResultGenerator):
        self._rgen = rgen

    def __call__(self, data: bytes) -> Any:
        self._rgen.runner.stdin_queue.put(data)
        return next(self._rgen)

    @property
    def return_code(self) -> None | int:
        return self._rgen.return_code


@contextmanager
def batchcommand(
    cmd: list,
    protocol_class: Protocol,
    cwd: Path | None = None,
) -> Generator[BatchProcess, None, None]:
    """Generic context manager for batch processes

    ``cmd`` is an ``argv``-list specification of a command. It is executed via
    a :class:`~datalad_next.runners.ThreadedRunner` runner instance.  This
    runner is parameterized with ``protocol_class`` (which can take any
    implementation of a DataLad runner protocol), responsible for
    (pre)processing the command's output.

    On leaving the context, the manager takes care of closing ``STDIN``
    of the underlying process, typically causing it to exit normally.

    While this generic context manager can be used directly, it can be
    more convenient to use any of the more specialized implementations
    that employ a specific protocol (e.g., :func:`stdout_batchcommand`,
    :func:`annexjson_batchcommand`).
    """
    input_q = Queue()
    proc = ThreadedRunner(
        cmd=cmd,
        protocol_class=protocol_class,
        stdin=input_q,
        cwd=cwd).run()
    try:
        bp = BatchProcess(proc)
        yield bp
    finally:
        input_q.put(None)
        # TODO should we block until `bp.return_code != None`?
        # TODO should we force terminate `bp` after a grace period?


def stdout_batchcommand(
    cmd: list,
    cwd: Path | None = None,
) -> Generator[BatchProcess, None, None]:
    """Context manager for commands that produce arbitrary output on ``stdout``

    Internally this calls :func:`batchcommand` with the
    :class:`StdOutCaptureGeneratorProtocol` protocol implementation.
    """
    return batchcommand(
        cmd,
        protocol_class=StdOutCaptureGeneratorProtocol,
        cwd=cwd,
    )


def annexjson_batchcommand(
    cmd: list,
    cwd: Path | None = None,
) -> Generator[BatchProcess, None, None]:
    """Context manager for git-annex commands that support ``--batch --json``

    The given ``cmd``-list must be complete, i.e., include
    ``git annex ... --batch --json``, and any additional flags that may be
    needed.

    Internally this calls :func:`batchcommand` with the
    :class:`GeneratorAnnexJsonProtocol` protocol implementation.
    """
    return batchcommand(
        cmd,
        protocol_class=GeneratorAnnexJsonProtocol,
        cwd=cwd,
    )
