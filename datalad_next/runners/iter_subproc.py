from __future__ import annotations

from pathlib import Path
from typing import (
    Iterable,
    List,
)

from datalad_next.iterable_subprocess.iterable_subprocess import (
    iterable_subprocess,
    OutputFrom,
)
from datalad_next.exceptions import CommandError
from datalad_next.consts import COPY_BUFSIZE

__all__ = ['iter_subproc']


def iter_subproc(
    args: List[str],
    *,
    input: Iterable[bytes] | None = None,
    chunk_size: int = COPY_BUFSIZE,
    cwd: Path | None = None,
    bufsize: int = -1,
):
    """Context manager to communicate with a subprocess using iterables

    This offers a higher level interface to subprocesses than Python's
    built-in ``subprocess`` module. It allows a subprocess to be naturally
    placed in a chain of iterables as part of a data processing pipeline.
    It is also helpful when data won't fit in memory and has to be streamed.

    This is a convenience wrapper around ``datalad_next.iterable_subprocess``,
    which itself is a slightly modified (for use on Windows) fork of
    https://github.com/uktrade/iterable-subprocess, written by
    Michal Charemza.

    This function provides a context manager.
    On entering the context, the subprocess is started, the thread to read
    from standard error is started, the thread to populate subprocess
    input is started.
    When running, the standard input thread iterates over the input,
    passing chunks to the process, while the standard error thread
    fetches the error output, and while the main thread iterates over
    the process's output from client code in the context.

    On context exit, the main thread closes the process's standard output,
    waits for the standard input thread to exit, waits for the standard error
    thread to exit, and wait for the process to exit. If the process exited
    with a non-zero return code, a ``CommandError`` is raised,
    containing the process's return code.

    If the context is exited due to an exception that was raised in the
    context, the main thread terminates the process via ``Popen.terminate()``,
    closes the process's standard output, waits for the standard input
    thread to exit, waits for the standard error thread to exit, waits
    for the process to exit, and re-raises the exception.

    Note, if an exception is raised in the context, this exception will bubble
    up to the main thread. That means no ``CommandError`` will
    be raised if the subprocess exited with a non-zero return code.
    To access the return code in case of an exception inside the context,
    use the ``code``-attribute of the ``as``-variable.
    This object will always contain the return code of the subprocess.
    For example, the following code will raise a ``StopIteration``-exception
    in the context (by repeatedly using :func:`next`). The subprocess
    will exit with ``2`` due to the illegal option ``-@``, and no
    ``CommandError`` is raised. The return code is read from
    the variable ``ls_stdout``

    .. code-block:: python

      >>> from datalad_next.runners import iter_subproc
      >>> try:
      ...     with iter_subproc(['ls', '-@']) as ls_stdout:
      ...         while True:
      ...             next(ls_stdout)
      ... except Exception as e:
      ...     print(repr(e), ls_stdout.returncode)
      StopIteration() 2


    Parameters
    ----------
    args: list
      Sequence of program arguments to be passed to ``subprocess.Popen``.
    input: iterable, optional
      If given, chunks of ``bytes`` to be written, iteratively, to the
      subprocess's ``stdin``.
    chunk_size: int, optional
      Size of chunks to read from the subprocess's stdout/stderr in bytes.
    cwd: Path
      Working directory for the subprocess, passed to ``subprocess.Popen``.
    bufsize: int, optional
      Buffer size to use for the subprocess's ``stdin``, ``stdout``, and
      ``stderr``. See ``subprocess.Popen`` for details.

    Returns
    -------
    contextmanager
    """
    return iterable_subprocess(
        args,
        tuple() if input is None else input,
        chunk_size=chunk_size,
        cwd=cwd,
        bufsize=bufsize,
    )
