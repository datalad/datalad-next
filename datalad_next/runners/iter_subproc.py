from __future__ import annotations
from typing import List

from datalad_next.iterable_subprocess.iterable_subprocess \
    import iterable_subprocess
from datalad_next.consts import COPY_BUFSIZE

__all__ = ['iter_subproc']


def iter_subproc(
    args: List[str],
    *,
    input: List[bytes] | None = None,
    chunk_size: int = COPY_BUFSIZE,
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

    Parameters
    ----------
    args: list
      Sequence of program arguments to be passed to ``subprocess.Popen``.
    input: iterable, optional
      If given, chunks of ``bytes`` to be written, iteratively, to the
      subprocess's ``stdin``.
    chunk_size: int, optional
      Size of chunks to read from the subprocess's stdout/stderr in bytes.
    bufsize: int, optional
      Buffer size to use for the subprocess's ``stdin``, ``stdout``, and
      ``stderr``. See ``subprocess.Popen`` for details.

    Returns
    -------
    contextmanager
      On entering the context, the subprocess is started, the thread to read
      from standard error is started, the thread to populate subprocess
      input is started.
      When running, the standard input thread iterates over the input,
      passing chunks to the process, while the standard error thread
      fetches the error output, and while the main thread iterates over
      the process's output from client code in the context.
      On context exit, the main thread closes the process's standard output,
      waits for the standard input thread to exit, waits for the standard
      error thread to exit, and wait for the process to exit.
    """
    return iterable_subprocess(
        args,
        tuple() if input is None else input,
        chunk_size=chunk_size,
        buf_size=-1,
    )
