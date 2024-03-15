from __future__ import annotations

import logging
from pathlib import (
    Path,
    PurePosixPath,
)
from queue import Queue
from typing import (
    BinaryIO,
    Callable,
)

from more_itertools import consume

from .common import DownloadResponseGenerator
from ..shell import ShellCommandExecutor
from datalad_next.consts import COPY_BUFSIZE


lgr = logging.getLogger("datalad.ext.next.shell.operations")


class DownloadResponseGeneratorPosix(DownloadResponseGenerator):
    """A response generator for efficient download commands on Linux systems"""

    def get_final_command(self, remote_file_name: bytes) -> bytes:
        """Return a final command list for the download of ``remote_file_name``

        The Linux version for download response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return (
            b"(stat -c %s "
            + remote_file_name
            + b"|| echo -e -1)"
            + b"&& cat "
            + remote_file_name
            + b"&& echo $?\n"
        )


class DownloadResponseGeneratorOSX(DownloadResponseGenerator):
    def get_final_command(self, remote_file_name: bytes) -> bytes:
        """Return a final command list for the download of ``remote_file_name``

        The OSX version for download response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return (
            b"(stat -f %z "
            + remote_file_name
            + b"|| echo -e -1)"
            + b"&& cat "
            + remote_file_name
            + b"&& echo $?\n"
        )


def upload(
    shell: ShellCommandExecutor,
    local_path: Path,
    remote_path: PurePosixPath,
    progress_callback: Callable[[int, int], None] | None = None,
) -> None:
    """Upload a local file to a named file in the connected shell

    This function uploads a file to the connected shell ``shell``. It uses
    ``head`` to limit the number of bytes that the remote shell will read.
    This ensures that the upload is terminated.

    The requirements for upload are:
    - The connected shell must be a POSIX shell.
    - ``head`` must be installed in the remote shell.
    - ``cat`` must be installed in the remote shell.

    Parameters
    ----------
    shell : ShellCommandExecutor
        The shell that should be used to upload the file.
    local_path : Path
        The file that should be uploaded.
    remote_path : PurePosixPath
        The name of the file on the connected shell that will contain the
        uploaded content.
    progress_callback : callable[[int, int], None], optional, default: None
        If given, the callback is called with the number of bytes that have
        been sent and the total number of bytes that should be sent.
    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, a
        :class:`CommandError` is raised. It will contain the exit code and
        the last ``chunk_size`` (defined by the ``chunk_size`` keyword argument
        to :func:`shell`) bytes of stderr output.
    """

    def signaling_read(
        file: BinaryIO, size: int, queue: Queue, chunk_size: int = COPY_BUFSIZE
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

    file_size = local_path.stat().st_size
    signal_queue = Queue()
    cmd_line = f"head -c {file_size} > {remote_path.as_posix()}"
    with local_path.open("rb") as local_file:
        # We use the `signaling_read` iterator to deal with the situation where
        # the content of a file that should be uploaded is completely read and
        # uploaded, but the final EOF-triggering `read()` call has not yet been
        # made. In this case it can happen that the server provides an answer,
        # and we leave the context, thereby closing the file. When the
        # `iterable_subprocess.<locals>.input_to`-thread then tries to read
        # from the file, a `ValueError` would be raised. This exception would
        # in turn lead to the closing of stdin of the `shell`-subprocess and
        # render it unusable.`signaling_read` allows us to wait for a completed
        # read, including the EOF reading.
        result = shell(
            cmd_line, stdin=signaling_read(local_file, file_size, signal_queue)
        )
        consume(result)
        signal_queue.get()


def download(
    shell: ShellCommandExecutor,
    remote_path: PurePosixPath,
    local_path: Path,
    response_generator_class: type[
        DownloadResponseGenerator
    ] = DownloadResponseGeneratorPosix,
) -> None:
    """Download a file from the connected shell

    This method downloads a file from the connected shell. It uses ``base64`` in
    the shell to encode the file. The encoding is mainly done to ensure that the
    end-marker is significant, i.e. not contained in the transferred file
    content, and to ensure that no control-sequences are sent.

    The requirements for download via instances of class
    :class:`DownloadResponseGeneratorPosix` are:
    - The connected shell must support `stat -c`.
    - The connected shell must support `echo -e`.
    - The connected shell must support `cat`.

    Parameters
    ----------
    shell: ShellCommandExecutor
        The shell from which a file should be downloaded.
    remote_path : PurePosixPath
        The name of the file on the connected shell that should be
        downloaded.
    local_path : Path
        The name of the local file that will contain the downloaded content.
    response_generator_class : type[DownloadResponseGenerator], optional, default: DownloadResponseGeneratorPosix
        The response generator that should be used to handle the download
        output. It must be a subclass of :class:`DownloadResponseGenerator`.
        The default works if the connected shell runs on a ``Linux`` system.
        On ``OSX`` systems, the response generator should be
        :class:`DownloadResponseGeneratorOSX`. The two classes only differ in
        the command line arguments that they provide to ``stat``. Those are
        different on ``Linux`` and ``OSX``.

    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, a
        :class:`CommandError` is raised. It will contain the exit code and
        the last ``chunk_size`` (defined by the ``chunk_size`` keyword argument
        to :func:`shell`) bytes of stderr output.
    """
    result = shell(
        remote_path.as_posix().encode(),
        response_generator=response_generator_class(shell.stdout),
    )
    with local_path.open("wb") as local_file:
        for chunk in result:
            local_file.write(chunk)


def delete(
    shell: ShellCommandExecutor,
    files: list[PurePosixPath],
    *,
    force: bool = False,
) -> None:
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

    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, a
        :class:`CommandError` is raised. It will contain the exit code and
        the last ``chunk_size`` (defined by the ``chunk_size`` keyword argument
        to :func:`shell`) bytes of stderr output.
    """
    cmd_line = (
        "rm " + ("-f " if force else "") + " ".join(f"{f.as_posix()}" for f in files)
    )
    result = shell(cmd_line.encode())
    consume(result)
