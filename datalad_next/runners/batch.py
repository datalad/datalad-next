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

from . import Protocol
from .protocols import StdOutCaptureGeneratorProtocol
from .run import run


class BatchProcess:
    """Representation of a (running) batch process

    An instance of this class is produced by any of the context manager variants
    in this module. It is a convenience wrapper around an instance of a
    :class:`_ResultGenerator` that is produced by a :meth:`ThreadedRunner.run`.

    A batch process instanced is used by passing ``bytes`` input to its
    ``__call__()`` method, and receiving the batch output as return value.

    While the process is still running, it ``return_code`` property will
    be ``None``. After the has terminated, the property will contain the
    respective exit status.
    """
    def __init__(self, rgen: _ResultGenerator):
        self._rgen = rgen
        self._stdin_queue = rgen.runner.stdin_queue

    def __call__(self, data: bytes | None) -> Any:
        self._stdin_queue.put(data)
        try:
            return next(self._rgen)
        except StopIteration:
            return None

    def close_stdin(self) -> Any:
        return self(None)

    @property
    def return_code(self) -> None | int:
        return self._rgen.return_code


@contextmanager
def batchcommand(
        cmd: list,
        protocol_class: type[Protocol],
        cwd: Path | None = None,
        terminate_time: int | None = None,
        kill_time: int | None = None,
        protocol_kwargs: dict | None = None,
) -> Generator[BatchProcess, None, None]:
    """Generic context manager for batch processes

    ``cmd`` is an ``argv``-list specification of a command. It is executed via
    a :class:`~datalad_next.runners.run.run` context manager. This context
    manager is parameterized with ``protocol_class`` (which can take any
    implementation of a DataLad runner protocol), and optional keyword arguments
    that are passed to the protocol class.

    On leaving the context, the manager will perform a "closing_action". By
    default, this is to close ``stdin`` of the underlying process. This will
    typically cause the underlying process to exit. A caller can specify an
    alternative function, i.e. ``closing_action``. If ``closing_action`` is set,
    the function will be called with two arguments. The first argument is the
    :class:`BatchProcess`-instance, the second argument is the stdin-queue of
    the subprocess.
    A custom ``closing_action`` might, for example, send some kind of exit
    command to the subprocess, and then close stdin. This method exists because
    the control flow might enter the exit-handler through different mechanisms.
    One mechanism would be an un-caught exception.

    If ``terminate_time`` is given, the context handler will send a
    terminate-signal to the process, if it is still running ``terminate_time``
    seconds after the context is left. If ``kill_time`` is given, the context
    handler will send a kill-signal to the process, if it is still running
    ``(terminate_time or 0) + kill_time`` seconds after the context is left.

    If neither ``terminate_time`` nor ``kill_time`` are set and the process
    is not triggered to exit, e.g. because its stdin is not closed or because
    it requires different actions to trigger its exit, batchcommand will wait
    forever after the context exited. Note that the context might also be
    exited in an unexpected way by an ``Èxception``.

    While this generic context manager can be used directly, it can be
    more convenient to use any of the more specialized implementations
    that employ a specific protocol (e.g., :func:`stdout_batchcommand`,
    :func:`annexjson_batchcommand`).

    Parameters
    ----------
    cmd : list[str]
        A list containing the command and its arguments (argv-like).
    cwd : Path | None
        If not ``None``, determines a new working directory for the command.
    terminate_time: int | None
        The number of timeouts after which a terminate-signal will be sent to
        the process, if it is still running. If no timeouts were provided in the
        ``timeout``-argument, the timeout is set to ``1.0``.
    kill_time: int | None
        See documentation of :func:`datalad_next.runners.run.run`.
    protocol_kwargs: dict
        If ``terminate_time`` is given, a kill-signal will be sent to the
        subprocess after kill-signal after ``terminate_time + kill_time``
        timeouts. If ``terminate_time`` is not set, a kill-signal will be sent
        after ``kill_time`` timeouts.
        It is a good idea to set ``kill_time`` and ``terminate_time`` in order
        to let the process exit gracefully, if it is capable to do so.

    Yields
    -------
    BatchProcess
        A :class:`BatchProcess`-instance that can be used to interact with the
        cmd

    """
    input_queue = Queue()
    try:
        with run(
            cmd=cmd,
            protocol_class=protocol_class,
            stdin=input_queue,
            cwd=cwd,
            terminate_time=terminate_time,
            kill_time=kill_time,
            protocol_kwargs=protocol_kwargs
        ) as result_generator:
            batch_process = BatchProcess(result_generator)
            yield batch_process
            batch_process.close_stdin()
    finally:
        del input_queue


def stdout_batchcommand(
        cmd: list,
        cwd: Path | None = None,
        terminate_time: int | None = None,
        kill_time: int | None = None,
) -> Generator[BatchProcess, None, None]:
    """Context manager for commands that produce arbitrary output on ``stdout``

    Internally this calls :func:`batchcommand` with the
    :class:`StdOutCaptureGeneratorProtocol` protocol implementation.  See the
    documentation of :func:`batchcommand` for a description of the parameters.
    """
    return batchcommand(
        cmd,
        protocol_class=StdOutCaptureGeneratorProtocol,
        cwd=cwd,
        terminate_time=terminate_time,
        kill_time=kill_time,
    )


def annexjson_batchcommand(
        cmd: list,
        cwd: Path | None = None,
        terminate_time: int | None = None,
        kill_time: int | None = None,
        protocol_kwargs: dict | None = None,
) -> Generator[BatchProcess, None, None]:
    """
    Context manager for git-annex commands that support ``--batch --json``

    The given ``cmd``-list must be complete, i.e., include
    ``git annex ... --batch --json``, and any additional flags that may be
    needed.

    Internally this calls :func:`batchcommand` with the
    :class:`GeneratorAnnexJsonProtocol` protocol implementation. See the
    documentation of :func:`batchcommand` for a description of the parameters.
    """
    return batchcommand(
        cmd,
        protocol_class=GeneratorAnnexJsonProtocol,
        cwd=cwd,
        terminate_time=terminate_time,
        kill_time=kill_time,
        protocol_kwargs=protocol_kwargs,
    )
