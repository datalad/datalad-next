"""Handler for operations, such as "download", on ssh:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
import random
import sys
import time
from functools import partial
from math import floor
from pathlib import (
    Path,
    PurePosixPath,
)
from queue import (
    Full,
    Queue,
)
from typing import (
    Any,
    Dict,
    IO,
)
from urllib.parse import (
    urlparse,
    ParseResult,
)

from more_itertools import consume

from datalad_next.consts import COPY_BUFSIZE
from datalad_next.config import ConfigManager
from datalad_core.runners import CommandError
from datalad_next.shell import (
    FixedLengthResponseGeneratorPosix,
    ShellCommandExecutor,
    shell,
)

from .base import UrlOperations
from .exceptions import (
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)

lgr = logging.getLogger('datalad.ext.next.ssh_url_operations')


__all__ = ['SshUrlOperations']


class SshUrlOperations(UrlOperations):
    """Handler for operations on ``ssh://`` URLs

    For downloading files, only servers that support execution of the commands
    'ls -dln', 'awk', and 'cat' are supported. This includes a wide
    range of operating systems, including devices that provide these commands
    via the 'busybox' software.

    .. note::
       Any instance of ``SshUrlOperations`` must be deleted before ending the
       program, otherwise python might not exit. The reason is, that
       ``SshUrlOperations`` retains and reuses SSH connections for subsequent
       command execution. Each connection has two threads associated with it.
       Those threads are only terminated when the connection is closed. The
       destructor of ``SshUrlOperations`` closes all connections and terminates
       all associated threads.
    """
    def __init__(self, *, cfg: ConfigManager | None = None):
        super().__init__(cfg=cfg)
        self.ssh_shells: dict[tuple[str, ...], tuple[ShellCommandExecutor, Any]] = dict()

    def __del__(self):
        for ssh_executor, context in self.ssh_shells.values():
            ssh_executor.close()
            context.__exit__(None, None, None)

    @staticmethod
    def _check_return_code(return_code: int | None, url: str, msg: str = ''):
        if return_code == 244:
            # this is the special code for a file-not-found
            raise UrlOperationsResourceUnknown(url, message=msg)
        elif return_code != 0:
            raise UrlOperationsRemoteError(
                url,
                message=f'ssh command returned {return_code}'
                + f': {msg}' if msg else ''
            )

    def ssh_shell_for(self,
                      url: str) -> ShellCommandExecutor:
        """Get a ShellCommandExecutor for the url (cached or newly created)"""
        open_args = ssh_url2openargs(url, self.cfg)[0]
        key = tuple(open_args)
        if key not in self.ssh_shells:
            context = shell(['ssh'] + open_args)
            try:
                ssh_executor = context.__enter__()
            except CommandError as e:
                context.__exit__(None, None, None)
                raise UrlOperationsRemoteError(url) from e
            self.ssh_shells[key] = (ssh_executor, context)
        return self.ssh_shells[key][0]

    def close_shell_for(self, url: str):
        """Close the ShellCommandExecutor for the url and remove it"""
        open_args = ssh_url2openargs(url, self.cfg)[0]
        key = tuple(open_args)
        if key in self.ssh_shells:
            ssh_executor, context = self.ssh_shells.pop(key)
            ssh_executor.close()
            context.__exit__(None, None, None)

    def stat(self,
             url: str,
             *,
             credential: str | None = None,
             timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.stat`
        for parameter documentation and exception behavior.
        """
        # Check whether a readable file exists at the path. If not signal a
        # dedicated 244 return code. This allows the user to distinguish the
        # absence of a readable file from other errors, e.g. from an error in
        # awk. Only a missing file would yield 244. A ssh-connection problem
        # would lead to a 255 error (and a closed connection).
        stat_cmd = """
            ret() {{ return $1; }}
            test -r {fpath}
            if [ $? -eq 0 ]; then
                LC_ALL=C ls -dln -- {fpath} | awk '{{print $5; exit}}'
            else
                ret 244
            fi"""

        cmd = self.format_cmd(stat_cmd, url)
        ssh = self.ssh_shell_for(url)
        result = ssh(cmd)
        self._check_return_code(result.returncode, url, result.stderr.decode())
        return {'content-length': int(result.stdout)}

    def delete(self,
               url: str,
               *,
               credential: str | None = None,
               timeout: float | None = None) -> Dict:
        """Delete the target of a shh://-URL

        The target can be a file or a directory. `delete` will attempt to
        delete write protected targets (by setting write permissions). If
        the target is a directory, the complete directory and all its
        content will be deleted. `delete` will not modify the permissions
        of the parent of the target. That means, it will not delete a target
        in a write protected directory, but it will empty target, if target is
        a directory.

        See :meth:`datalad_next.url_operations.UrlOperations.delete`
        for parameter documentation and exception behavior.

        Raises
        ------
        UrlOperationsResourceUnknown
          For deletion targets found absent.
        """

        delete_cmd = """
            ret() {{ return $1; }}
            if [ -f {fpath} ]; then
                chmod u+w {fpath}
                rm -f {fpath}
            elif [ -d {fpath} ]; then
                chmod -R u+wx {fpath}
                rm -rf {fpath}
            else
                ret 244
            fi"""

        cmd = self.format_cmd(delete_cmd, url)
        ssh = self.ssh_shell_for(url)
        result = ssh(cmd)
        self._check_return_code(result.returncode, url, result.stderr.decode())
        return {}

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 # unused, but theoretically could be used to
                 # obtain escalated/different privileges on a system
                 # to gain file access
                 credential: str | None = None,
                 hash: list[str] | None = None,
                 timeout: float | None = None) -> Dict:
        """Download a file by streaming it through an SSH connection.

        On the server-side, the file size is determined and sent. Afterwards
        the file content is sent via `cat` to the SSH client.

        See :meth:`datalad_next.url_operations.UrlOperations.download`
        for parameter documentation and exception behavior.
        """
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, str(to_path))

        # get the size of the file to download
        stat = self.stat(from_url, credential=credential, timeout=timeout)
        expected_size = stat['content-length']

        # get a shell command executor and a fixed length response generator
        ssh = self.ssh_shell_for(from_url)
        response_generator = FixedLengthResponseGeneratorPosix(
            ssh.stdout,
            expected_size
        )

        dst_fp = sys.stdout.buffer \
            if to_path is None \
            else open(to_path, 'wb')

        # Localize variable access to minimize overhead
        dst_fp_write = dst_fp.write

        # We already know that file exists, so we can just cat it.
        cmd = self.format_cmd('cat {fpath}', from_url)
        result_generator = ssh.start(
            cmd,
            response_generator=response_generator
        )
        # We do not use the `shell.operations.posix.download`-method here
        # because we need access to every individual chunk in order to calculate
        # the hash on the fly.
        for chunk in self._with_progress(
                result_generator,
                progress_id=progress_id,
                label='downloading',
                expected_size=expected_size,
                start_log_msg=('Download %s to %s', from_url, to_path),
                end_log_msg=('Finished download',),
                update_log_msg=('Downloaded chunk',)
        ):
            # write data
            dst_fp_write(chunk)
            # compute hash simultaneously
            hasher.update(chunk)

        if dst_fp and to_path is not None:
            dst_fp.close()

        self._check_return_code(
            result_generator.returncode,
            from_url,
            ''.join(result_generator.stderr_deque)
        )

        return {
            **stat,
            **hasher.get_hexdigest(),
        }

    def upload(self,
               from_path: Path | None,
               to_url: str,
               *,
               credential: str | None = None,
               hash: list[str] | None = None,
               timeout: float | None = None) -> Dict:
        """Upload a file by streaming it through an SSH connection.

        It, more or less, runs `ssh <host> 'cat > <path>'` or
        `ssh <host> 'head -c <file-size> > <path>'` on the remote side.

        See :meth:`datalad_next.url_operations.UrlOperations.upload`
        for parameter documentation and exception behavior.
        """

        if from_path is None:
            source_name = '<STDIN>'
            return self._perform_upload(
                src_fp=sys.stdin.buffer,
                source_name=source_name,
                to_url=to_url,
                hash_names=hash,
                expected_size=None,
                timeout=timeout,
            )
        else:
            # die right away, if we lack read permissions or there is no file
            with from_path.open("rb") as src_fp:
                return self._perform_upload(
                    src_fp=src_fp,
                    source_name=str(from_path),
                    to_url=to_url,
                    hash_names=hash,
                    expected_size=from_path.stat().st_size,
                    timeout=timeout,
                )

    def _perform_upload(self,
                        src_fp: IO,
                        source_name: str,
                        to_url: str,
                        hash_names: list[str] | None,
                        expected_size: int | None,
                        timeout: float | None) -> dict:

        hasher = self._get_hasher(hash_names)

        # we use a queue to implement timeouts.
        # we limit the queue to few items in order to `make queue.put()`
        # block relatively quickly, and thereby have the progress report
        # actually track the upload, i.e. the feeding of the stdin pipe
        # of the ssh-process, and not just the feeding of the
        # queue.
        # If we did not support timeouts, we could just use the following
        # as `input`-iterable for `iter_subproc`:
        #
        #   `iter(partial(src_fp.read, COPY_BUFSIZE), b'')
        #
        upload_queue: Queue = Queue(maxsize=2)

        if expected_size:
            read_cmd = f"head -c {expected_size}"
        else:
            read_cmd = "cat"

        cmd = self.format_cmd(
            # copy the file to its destination location with a randomized
            # name, and move it to its final location after upload. This
            # way, upload appears atomic, i.e. no half uploaded file will
            # be seen at the destination URL
            # leave special exit code when writing or moving fails, but not
            # the general SSH access
            "ret() {{ return $1; }}; ( mkdir -p '{fdir}' "
            f"&& {read_cmd} "
            "> '{fpath}.transfer-{nonce}' "
            "&& mv '{fpath}.transfer-{nonce}' '{fpath}' ) || ret 243",
            to_url,
        )

        progress_id = self._get_progress_id(source_name, to_url)

        ssh = self.ssh_shell_for(to_url)
        result_generator = ssh.start(
            cmd,
            stdin=self._with_progress(
                iter(upload_queue.get, None),
                progress_id=progress_id,
                label='uploading',
                expected_size=expected_size,
                start_log_msg=('Upload %s to %s', source_name, to_url),
                end_log_msg=('Finished upload',),
                update_log_msg=('Uploaded chunk',)
            )
        )

        try:
            upload_size = 0
            for chunk in iter(partial(src_fp.read, COPY_BUFSIZE), b''):

                # we are just putting stuff in the queue, and rely on
                # its maxsize to cause it to block the next call to
                # have the progress reports be anyhow valid, we also
                # rely on put-timeouts to implement timeout.
                upload_queue.put(chunk, timeout=timeout)

                # compute hash simultaneously
                hasher.update(chunk)
                upload_size += len(chunk)

            upload_queue.put(None, timeout=timeout)

        except Full:
            # we had a timeout while uploading
            raise TimeoutError(f'timeout while executing: {cmd}')

        if expected_size:
            consume(result_generator)
        else:
            # If the remote shell reads from stdin, its stdin has to be close
            # for the upload-command to terminate
            if expected_size is None:
                ssh.close()
            consume(result_generator)
            # stdin of the shell was closed, it cannot be used anymore.
            self.close_shell_for(to_url)

        self._check_return_code(
            result_generator.returncode,
            to_url,
            ''.join(result_generator.stderr_deque)
        )

        return {
            **hasher.get_hexdigest(),
            # return how much was copied. we could compare with
            # `expected_size` and error on mismatch, but not all
            # sources can provide that (e.g. stdin)
            'content-length': upload_size
        }

    def format_cmd(self,
                   cmd: str,
                   url: str) -> str:
        ssh_command_builder = _SshCommandBuilder(url, self.cfg)
        return ssh_command_builder.substitute(cmd)


class _SshCommandBuilder:
    def __init__(
            self,
            url: str,
            cfg: ConfigManager,
    ):
        self.ssh_args, self._parsed = ssh_url2openargs(url, cfg)
        self.ssh_args.extend(('-e', 'none'))
        # make sure the essential pieces exist
        assert self._parsed.path
        time_stamp = time.time()
        self.substitutions = dict(
            fdir=str(PurePosixPath(self._parsed.path).parent),
            fpath=self._parsed.path,
            nonce=(
                str(random.randint(1000000000, 9999999999))
                + '_'
                + str(time_stamp - floor(time_stamp))[2:0]
            )
        )

    def substitute(self, payload_cmd: str) -> str:
        return payload_cmd.format(**self.substitutions)


def ssh_url2openargs(
    url: str,
    cfg: ConfigManager,
) -> tuple[list[str], ParseResult]:
    """Helper to report ssh-open arguments from a URL and config

    Returns a tuple with the argument list and the parsed URL.
    """
    args: list[str] = list()
    parsed = urlparse(url)
    # make sure the essential pieces exist
    assert parsed.hostname
    for opt, arg in (('-p', parsed.port),
                     ('-l', parsed.username),
                     ('-i', cfg.get('datalad.ssh.identityfile'))):
        if arg:
            # f-string, because port is not str
            args.extend((opt, f'{arg}'))
    # we could also use .netloc here and skip -p/-l above
    args.append(parsed.hostname)
    return args, parsed
