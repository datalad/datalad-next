from __future__ import annotations

import logging
from pathlib import (
    Path,
    PurePosixPath,
)
from queue import Queue
from shlex import quote as posix_quote
from typing import (
    BinaryIO,
    Callable,
)

from .common import DownloadResponseGenerator
from ..shell import (
    ExecutionResult,
    ShellCommandExecutor,
    create_result,
)
from datalad_next.consts import COPY_BUFSIZE


__all__ = [
    'DownloadResponseGenerator',
    'DownloadResponseGeneratorPosix',
    'upload',
    'download',
    'delete',
]


lgr = logging.getLogger("datalad.ext.next.shell.operations")


class DownloadResponseGeneratorPosix(DownloadResponseGenerator):
    """A response generator for efficient download commands from Linux systems"""

    def get_final_command(self, remote_file_name: bytes) -> bytes:
        """Return a final command list for the download of ``remote_file_name``

        The POSIX version for download response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.

        Parameters
        ----------
        remote_file_name : bytes
            The name of the file that should be downloaded. If the file name
            contains special character, e.g. space or ``$``, it must be
            quoted for a POSIX shell, for example with ``shlex.quote``.

        Returns
        -------
        bytes
            The final command that will be executed in the persistent shell
            in order to start the download in the connected shell.
        """
        command = b"""
            test -r {remote_file_name}
            if [ $? -eq 0 ]; then
                LC_ALL=C ls -dln -- {remote_file_name} | awk '{print $5; exit}'
                cat {remote_file_name}
                echo $?
            else
                echo -1;
            fi
        """.replace(b'{remote_file_name}', remote_file_name)
        return command


def upload(
    shell: ShellCommandExecutor,
    local_path: Path,
    remote_path: PurePosixPath,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    check: bool = False,
) -> ExecutionResult:
    """Upload a local file to a named file in the connected shell

    This function uploads a file to the connected shell ``shell``. It uses
    ``head`` to limit the number of bytes that the remote shell will read.
    This ensures that the upload is terminated.

    The requirements for upload are:
    - The connected shell must be a POSIX shell.
    - ``head`` must be installed in the remote shell.

    Parameters
    ----------
    shell : ShellCommandExecutor
        The shell that should be used to upload the file.
    local_path : Path
        The path of the file that should be uploaded.
    remote_path : PurePosixPath
        The path of the file on the connected shell that will contain the
        uploaded content.
    progress_callback : callable[[int, int], None], optional, default: None
        If given, the callback is called with the number of bytes that have
        been sent and the total number of bytes that should be sent.
    check : bool, optional, default: False
        If ``True``, raise a :class:`CommandError` if the remote operation does
        not exit with a ``0`` as return code.

    Returns
    -------
    ExecutionResult
        The result of the upload operation.

    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, and
        ``check`` is ``True``, a :class:`CommandError` is raised. It will
        contain the exit code and the last (up to ``chunk_size`` (defined by the
        ``chunk_size`` keyword argument to :func:`shell`)) bytes of stderr
        output.
    """

    def signaling_read(
            file: BinaryIO,
            size: int,
            queue: Queue,
            *,
            chunk_size: int = COPY_BUFSIZE
    ):
        """iterator that reads from a file and signals EOF via a queue

        This iterator is used to prevent the situation where a file that
        should be uploaded is completely read and uploaded, but the final
        EOF-triggering `read()` call has not yet been made. In this case it can
        happen that the server provides an answer. If the answer is interpreted
        as indicator for a completed operation, the calling code assumes
        that it can close all file handles associated with the operation. This
        can lead to the final `read()` call being performed on a closed file,
        which would raise a `ValueError`. To prevent this, ``signaling_read``
        signals the end of the read-operation, i.e. an EOF was read, by
        enqueuing ``Ǹone`` into the signaling queue. The caller can wait for
        that event to ensure that the read operation is really done.
        """
        processed = 0
        while True:
            data = file.read(chunk_size)
            if data == b"":
                break
            yield data
            processed += len(data)
            if progress_callback is not None:
                progress_callback(processed, size)
        queue.put(None)

    # The following command line ensures that content that we send to the shell
    # either goes to the destination file or into `/dev/null`, but not into the
    # stdin of the shell. In the latter case it would be interpreted as the
    # next command, and that might be bad, e.g. if the uploaded content was
    # `rm -rf $HOME`.
    file_size = local_path.stat().st_size
    cmd_line = (
        f'head -c {file_size} > {posix_quote(str(remote_path))}'
        f"|| (head -c {file_size} > /dev/null; test 1 == 2)"
    )
    with local_path.open("rb") as local_file:
        # We use the `signaling_read` iterator to deal with the situation where
        # the content of a file that should be uploaded is completely read and
        # uploaded, but the final, EOF-triggering, `read()` call has not yet been
        # made. In this case it can happen that the server provides an answer,
        # and we leave the context, thereby closing the file. When the
        # `iterable_subprocess.<locals>.input_to`-thread then tries to read
        # from the file, a `ValueError` would be raised. This exception would
        # in turn lead to the closing of stdin of the `shell`-subprocess and
        # render it unusable.`signaling_read` allows us to wait for a completed
        # read, including the EOF reading.
        signal_queue: Queue = Queue()
        result = shell(
            cmd_line, stdin=signaling_read(local_file, file_size, signal_queue)
        )
        signal_queue.get()
    if check:
        result.to_exception(cmd_line, 'upload failed')
    return result


def download(
    shell: ShellCommandExecutor,
    remote_path: PurePosixPath,
    local_path: Path,
    *,
    progress_callback: Callable[[int, int], None] | None = None,
    response_generator_class: type[
        DownloadResponseGenerator
    ] = DownloadResponseGeneratorPosix,
    check: bool = False,
) -> ExecutionResult:
    """Download a file from the connected shell

    This method downloads a file from the connected shell.

    The requirements for download via instances of class
    :class:`DownloadResponseGeneratorPosix` are:
    - The connected shell must support `ls -dln`.
    - The connected shell must support `echo -e`.
    - The connected shell must support `awk`.
    - The connected shell must support `cat`.

    Parameters
    ----------
    shell: ShellCommandExecutor
        The shell from which a file should be downloaded.
    remote_path : PurePosixPath
        The path of the file on the connected shell that should be
        downloaded.
    local_path : Path
        The path of the local file that will contain the downloaded content.
    progress_callback : callable[[int, int], None], optional, default: None
        If given, the callback is called with the number of bytes that have
        been received and the total number of bytes that should be received.
    response_generator_class : type[DownloadResponseGenerator], optional, default: DownloadResponseGeneratorPosix
        The response generator that should be used to handle the download
        output. It must be a subclass of :class:`DownloadResponseGenerator`.
        The default works if the connected shell runs on a Unix-like system that
        provides `ls -dln`, `cat`, `echo`, and `awk`, e.g. ``Linux`` or ``OSX``.
    check : bool, optional, default: False
        If ``True``, raise a :class:`CommandError` if the remote operation does
        not exit with a ``0`` as return code.

    Returns
    -------
    ExecutionResult
        The result of the download operation.

    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, and
        ``check`` is ``True``, a :class:`CommandError` is raised. It will
        contain the exit code and the last (up to ``chunk_size`` (defined by the
        ``chunk_size`` keyword argument to :func:`shell`)) bytes of stderr
        output.
    """
    command = posix_quote(str(remote_path)).encode()
    response_generator = response_generator_class(shell.stdout)
    result_generator = shell.start(
        command,
        response_generator=response_generator,
    )
    with local_path.open("wb") as local_file:
        processed = 0
        for chunk in result_generator:
            local_file.write(chunk)
            processed += len(chunk)
            if progress_callback is not None:
                progress_callback(processed, response_generator.length)

    stderr = b''.join(result_generator.stderr_deque)
    result_generator.stderr_deque.clear()
    return create_result(
        result_generator,
        command,
        stdout=b'',
        stderr=stderr,
        check=check,
        error_message='download failed',
    )


def delete(
    shell: ShellCommandExecutor,
    files: list[PurePosixPath],
    *,
    force: bool = False,
    check: bool = False,
) -> ExecutionResult:
    """Delete files on the connected shell

    The requirements for delete are:
    - The connected shell must be a POSIX shell.
    - ``rm`` must be installed in the remote shell.

    Parameters
    ----------
    shell: ShellCommandExecutor
        The shell from which a file should be downloaded.
    files : list[PurePosixPath]
        The "paths"  of the files that should be deleted.
    force : bool
        If ``True``, enforce removal, if possible. For example, the command
        could change the permissions of the files to be deleted to ensure
        their removal.
    check : bool, optional, default: False
        If ``True``, raise a :class:`CommandError` if the remote operation does
        not exit with a ``0`` as return code.

    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, and
        ``check`` is ``True``, a :class:`CommandError` is raised. It will
        contain the exit code and the last (up to ``chunk_size`` (defined by the
        ``chunk_size`` keyword argument to :func:`shell`)) bytes of stderr
        output.
    """
    cmd_line = (
        "rm "
        + ("-f " if force else "")
        + " ".join(
            f"{posix_quote(str(f))}"
            for f in files
        )
    )
    result = shell(cmd_line.encode())
    if check:
        result.to_exception(cmd_line, 'delete failed')
    return result
