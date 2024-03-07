from __future__ import annotations

import logging
from pathlib import (
    Path,
    PurePosixPath,
)

from more_itertools import consume

from ..shell import ShellCommandExecutor
from datalad_next.exceptions import CommandError
from .common import DownloadResponseGenerator
from datalad_next.consts import COPY_BUFSIZE


lgr = logging.getLogger('datalad.ext.next.shell.operations')


class DownloadResponseGeneratorPosix(DownloadResponseGenerator):
    """A response generator for efficient download commands on Linux systems"""
    def get_command_list(self, remote_file_name: bytes) -> list[bytes]:
        """Return a final command list for the download of ``remote_file_name``

        The Linux version for download response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return [
            b'(stat -c %s ' + remote_file_name + b'|| echo -e -1)'
            + b'&& cat ' + remote_file_name
            + b'&& echo $?\n'
        ]


class DownloadResponseGeneratorOSX(DownloadResponseGenerator):
    def get_command_list(self, remote_file_name: bytes) -> list[bytes]:
        """Return a final command list for the download of ``remote_file_name``

        The OSX version for download response generators.

        This method is usually only called by
        :meth:`ShellCommandExecutor.__call__`.
        """
        return [
            b'(stat -f %z ' + remote_file_name + b'|| echo -e -1)'
            + b'&& cat ' + remote_file_name
            + b'&& echo $?\n'
        ]


def upload(shell: ShellCommandExecutor,
           local_path: Path,
           remote_path: PurePosixPath,
           ) -> None:
    """Upload a local file to a named file in the connected shell

    This function uploads a file to the connected shell ``shell``. It uses
    ``head`` to limit the number of bytes that the remote shell will read.
    This ensures that the upload is terminated..

    The requirements for upload are:
    - The connected shell must be a POSIX shell.
    - ``head`` must be installed in the remote shell.
    - ``base64`` must be installed in the remote shell.

    Parameters
    ----------
    shell : ShellCommandExecutor
        The shell that should be used to upload the file.
    local_path : Path
        The file that should be uploaded.
    remote_path : PurePosixPath
        The name of the file on the connected shell that will contain the
        uploaded content.
    Raises
    -------
    CommandError:
        If the remote operation does not exit with a ``0`` as return code, a
        :class:`CommandError` is raised. It will contain the exit code and
        the last ``chunk_size`` (defined by the ``chunk_size`` keyword argument
        to :func:`shell`) bytes of stderr output.
    """
    def safe_read(file, size=COPY_BUFSIZE):
        while True:
            try:
                data = file.read(size)
            except ValueError:
                break
            if data == 'b':
                break
            yield data

    file_size = local_path.stat().st_size
    cmd_line = f'head -c {file_size} > {remote_path.as_posix()}'
    with local_path.open('rb') as local_file:
        result = shell(cmd_line.encode(), stdin=safe_read(local_file))
        consume(result)
        _check_result(
            result,
            cmd_line,
            f'failed: upload({shell!r}, {local_path!r}, {remote_path!r})'
        )


def download(shell: ShellCommandExecutor,
             remote_path: PurePosixPath,
             local_path: Path,
             response_generator_class: type[DownloadResponseGenerator] = DownloadResponseGeneratorPosix
             ) -> None:
    """Download a file from the connected shell

    This method downloads a file from the connected shell. It uses ``base64`` in
    the shell to encode the file. The encoding is mainly done to ensure that the
    end-marker is significant, i.e. not contained in the transferred file
    content, and to ensure that no control-sequences are sent.

    The requirements for upload are:
    - The connected shell must be a POSIX shell.
    - ``base64`` must be installed in the remote shell.

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
    TimeoutError:
        If ``timeout`` is not ``None`` and data is received after ``timeout``
        seconds.
    """
    result = shell(
        remote_path.as_posix().encode(),
        response_generator=response_generator_class(shell.stdout)
    )
    with local_path.open('wb') as local_file:
        for chunk in result:
            local_file.write(chunk)
    _check_result(
        result,
        f'download {remote_path} {local_path}',
        f'failed: download({shell!r}, {remote_path!r}, {local_path!r})'
    )


def delete(shell: ShellCommandExecutor,
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
    cmd_line = \
        'rm ' \
        + ('-f ' if force else '') \
        + ' '.join(f'{f.as_posix()}' for f in files)
    result = shell(cmd_line.encode())
    consume(result)
    _check_result(result, cmd_line, f'delete failed: {files!r}')


def _check_result(result, cmd_line, message):
    if result.returncode != 0:
        raise CommandError(
            cmd=cmd_line,
            msg=message,
            code=result.returncode,
            stdout=result.stdout,
            stderr=b''.join(result.stderr_deque)
        )
