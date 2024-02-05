"""Handler for operations, such as "download", on ssh:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
import sys
from functools import partial
from itertools import chain
from pathlib import (
    Path,
    PurePosixPath,
)
from queue import (
    Full,
    Queue,
)
from typing import (
    Dict,
    Generator,
    IO,
    cast,
)
from urllib.parse import (
    urlparse,
    ParseResult,
)

from datalad_next.consts import COPY_BUFSIZE
from datalad_next.config import ConfigManager
from datalad_next.itertools import align_pattern
from datalad_next.runners import (
    iter_subproc,
    CommandError,
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
    'printf', 'ls -nl', 'awk', and 'cat' are supported. This includes a wide
    range of operating systems, including devices that provide these commands
    via the 'busybox' software.

    .. note::
       The present implementation does not support SSH connection multiplexing,
       (re-)authentication is performed for each request. This limitation is
       likely to be removed in the future, and connection multiplexing
       supported where possible (non-Windows platforms).
    """
    # first try ls'ing the path, and catch a missing path with a dedicated 244
    # exit code, to be able to distinguish the original exit=2 that ls-call
    # from a later exit=2 from awk in case of a "fatal error".
    # when executed through ssh, only a missing file would yield 244, while
    # a connection error or other problem unrelated to the present of a file
    # would a different error code (255 in case of a connection error)
    _stat_cmd = "printf \"\\1\\2\\3\"; ls '{fpath}' &> /dev/null " \
                "&& ls -nl '{fpath}' | awk 'BEGIN {{ORS=\"\\1\"}} {{print $5}}' " \
                "|| exit 244"
    _cat_cmd = "cat '{fpath}'"

    @staticmethod
    def _check_return_code(return_code: int, url: str):
        # At this point the subprocess has either exited, was terminated, or
        # was killed.
        if return_code == 244:
            # this is the special code for a file-not-found
            raise UrlOperationsResourceUnknown(url)
        elif return_code != 0:
            raise UrlOperationsRemoteError(
                url,
                message=f'ssh process returned {return_code}'
            )

    def stat(self,
             url: str,
             *,
             credential: str | None = None,
             timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.stat`
        for parameter documentation and exception behavior.
        """
        ssh_cat = _SshCommandBuilder(url, self.cfg)
        cmd = ssh_cat.get_cmd(SshUrlOperations._stat_cmd)
        try:
            with iter_subproc(cmd) as stream:
                try:
                    props = self._get_props(url, stream)
                except StopIteration:
                    # we did not receive all data that should be sent, if a
                    # remote file exists. This indicates a non-existing
                    # resource or some other problem. The remotely executed
                    # command should signal the error via a non-zero exit code.
                    # That will trigger a `CommandError` below.
                    pass
        except CommandError:
            self._check_return_code(stream.returncode, url)
        return {k: v for k, v in props.items() if not k.startswith('_')}

    def _get_props(self, url, stream: Generator) -> dict:
        # Any stream must start with this magic marker, or we do not
        # recognize what is happening
        # after this marker, the server will send the size of the
        # to-be-downloaded file in bytes, followed by another magic
        # b'\1', and the file content after that.
        magic_marker = b'\1\2\3'

        # use the `align_pattern` iterable to guarantees, that the magic
        # marker is always contained in a complete chunk.
        aligned_stream = align_pattern(stream, magic_marker)

        # Because the stream should start with the pattern, the first chunk of
        # the aligned stream must contain it.
        # We know that the stream will deliver bytes, cast the result
        # accordingly.
        chunk = cast(bytes, next(aligned_stream))
        if chunk[:len(magic_marker)] != magic_marker:
            raise RuntimeError("Protocol error: report header not received")
        chunk = chunk[len(magic_marker):]

        # We are done with the aligned stream, use the original stream again.
        # This is possible because `align_pattern` does not cache any data
        # after a `yield`.
        del aligned_stream

        # The length is transferred now and terminated by b'\x01'.
        while b'\x01' not in chunk:
            chunk += next(stream)

        marker_index = chunk.index(b'\x01')
        expected_size = int(chunk[:marker_index])
        chunk = chunk[marker_index + 1:]
        props = {
            'content-length': expected_size,
            # go back to the original iterator, no need to keep looking for
            # a pattern
            '_stream': chain([chunk], stream) if chunk else stream
        }
        return props

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
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, str(to_path))

        dst_fp = None

        ssh_cat = _SshCommandBuilder(from_url, self.cfg)
        cmd = ssh_cat.get_cmd(f'{SshUrlOperations._stat_cmd}; {SshUrlOperations._cat_cmd}')
        try:
            with iter_subproc(cmd) as stream:
                try:
                    props = self._get_props(from_url, stream)
                    expected_size = props['content-length']
                    # The stream might have changed due to not yet processed, but
                    # fetched data, that is now chained in front of it. Therefore we
                    # get the updated stream from the props
                    download_stream = props.pop('_stream')

                    dst_fp = sys.stdout.buffer \
                        if to_path is None \
                        else open(to_path, 'wb')

                    # Localize variable access to minimize overhead
                    dst_fp_write = dst_fp.write

                    # download can start
                    for chunk in self._with_progress(
                            download_stream,
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
                except StopIteration:
                    # we did not receive all data that should be sent, if a
                    # remote file exists. This indicates a non-existing
                    # resource or some other problem. The remotely executed
                    # command should signal the error via a non-zero exit code.
                    # That will trigger a `CommandError` below.
                    pass
        except CommandError:
            self._check_return_code(stream.returncode, from_url)
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()

        return {
            **props,
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

        It, more or less, runs `ssh <host> 'cat > <path>'`.

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

        cmd = _SshCommandBuilder(to_url, self.cfg).get_cmd(
            # leave special exit code when writing fails, but not the
            # general SSH access
            "( mkdir -p '{fdir}' && cat > '{fpath}' ) || exit 244"
        )

        progress_id = self._get_progress_id(source_name, to_url)
        try:
            with iter_subproc(
                    cmd,
                    input=self._with_progress(
                        iter(upload_queue.get, None),
                        progress_id=progress_id,
                        label='uploading',
                        expected_size=expected_size,
                        start_log_msg=('Upload %s to %s', source_name, to_url),
                        end_log_msg=('Finished upload',),
                        update_log_msg=('Uploaded chunk',)
                    )
            ):
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

        except CommandError as e:
            self._check_return_code(e.returncode, to_url)
        except Full:
            if chunk != b'':
                # we had a timeout while uploading
                raise TimeoutError

        return {
            **hasher.get_hexdigest(),
            # return how much was copied. we could compare with
            # `expected_size` and error on mismatch, but not all
            # sources can provide that (e.g. stdin)
            'content-length': upload_size
        }


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
        self.substitutions = dict(
            fdir=str(PurePosixPath(self._parsed.path).parent),
            fpath=self._parsed.path,
        )

    def get_cmd(self,
            payload_cmd: str,
            ) -> list[str]:
        cmd = ['ssh']
        cmd.extend(self.ssh_args)
        cmd.append(payload_cmd.format(**self.substitutions))
        return cmd


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
