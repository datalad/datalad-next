"""Handler for operations, such as "download", on ssh:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import (
    Any,
    Dict,
    Generator,
)
from urllib.parse import urlparse

from datalad.runner import StdOutCapture
from datalad.runner.protocol import GeneratorMixIn
from datalad.runner.nonasyncrunner import ThreadedRunner
from datalad_next.exceptions import (
    AccessFailedError,
    CommandError,
    DownloadError,
    UrlTargetNotFound,
)

from . import UrlOperations

lgr = logging.getLogger('datalad.ext.next.ssh_url_operations')


__all__ = ['SshUrlOperations']


class SshUrlOperations(UrlOperations):
    """Handler for operations on `ssh://` URLs

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
    # a conenction error or other problem unrelated to the present of a file
    # would a different error code (255 in case of a connection error)
    _stat_cmd = "printf \"\1\2\3\"; ls '{fpath}' &> /dev/null " \
                "&& ls -nl '{fpath}' | awk 'BEGIN {{ORS=\"\1\"}} {{print $5}}' " \
                "|| exit 244"
    _cat_cmd = "cat '{fpath}'"

    def sniff(self, url: str, *, credential: str = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.sniff`
        for parameter documentation.

        Raises
        ------
        AccessFailedError
        UrlTargetNotFound
          Raises `AccessFailedError` for connection errors, and
          `UrlTargetNotFound` for download targets found absent after a
          connection was established successfully.
        """
        try:
            props = self._sniff(
                url,
                cmd=SshUrlOperations._stat_cmd,
            )
        except CommandError as e:
            if e.code == 244:
                # this is the special code for a file-not-found
                raise UrlTargetNotFound(url) from e
            else:
                raise AccessFailedError(str(e)) from e

        return {k: v for k, v in props.items() if not k.startswith('_')}

    def _sniff(self, url: str, cmd: str) -> Dict:
        # any stream must start with this magic marker, or we do not
        # recognize what is happening
        # after this marker, the server will send the size of the
        # to-be-downloaded file in bytes, followed by another magic
        # b'\1', and the file content after that
        need_magic = b'\1\2\3'
        expected_size_str = b''
        expected_size = None

        ssh_cat = SshCat(url)
        stream = ssh_cat.run(cmd)
        for chunk in stream:
            if need_magic:
                expected_magic = need_magic[:min(len(need_magic),
                                                 len(chunk))]
                incoming_magic = chunk[:len(need_magic)]
                # does the incoming data have the remaining magic bytes?
                if incoming_magic != expected_magic:
                    raise RuntimeError(
                        "Protocol error: report header not received")
                # reduce (still missing) magic, if any
                need_magic = need_magic[len(expected_magic):]
                # strip magic from input
                chunk = chunk[len(expected_magic):]
            if chunk and expected_size is None:
                # we have incoming data left and
                # we have not yet consumed the size info
                size_data = chunk.split(b'\1', maxsplit=1)
                expected_size_str += size_data[0]
                if len(size_data) > 1:
                    # this is not only size info, but we found the start of
                    # the data
                    expected_size = int(expected_size_str)
                    chunk = size_data[1]
            if expected_size:
                props = {
                    'content-length': expected_size,
                    '_stream': chain([chunk], stream) if chunk else stream,
                }
                return props
            # there should be no data left to process, or something went wrong
            assert not chunk

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 # unused, but theoretically could be used to
                 # obtain escalated/different privileges on a system
                 # to gain file access
                 credential: str = None,
                 hash: str = None) -> Dict:
        """Download a file by streaming it through an SSH connection.

        On the server-side, the file size is determined and sent. Afterwards
        the file content is sent via `cat` to the SSH client.

        See :meth:`datalad_next.url_operations.UrlOperations.download`
        for parameter documentation.

        Raises
        ------
        AccessFailedError
        UrlTargetNotFound
          Raises `AccessFailedError` for connection errors, and
          `UrlTargetNotFound` for download targets found absent after a
          connection was established successfully.
        """
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)

        dst_fp = None

        try:
            props = self._sniff(
                from_url,
                cmd=f'{SshUrlOperations._stat_cmd}; {SshUrlOperations._cat_cmd}',
            )
            stream = props.pop('_stream')
            expected_size = props['content-length']
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')
            # Localize variable access to minimize overhead
            dst_fp_write = dst_fp.write
            # download can start
            self._progress_report_start(
                progress_id, from_url, to_path, expected_size)
            for chunk in stream:
                # write data
                dst_fp_write(chunk)
                # compute hash simultaneously
                for h in hasher:
                    h.update(chunk)
                self._progress_report_update(progress_id, len(chunk))
            props.update(self._get_hash_report(hash, hasher))
            return props
        except CommandError as e:
            if e.code == 244:
                # this is the special code for a file-not-found
                raise UrlTargetNotFound(from_url) from e
            else:
                # wrap this into the datalad-standard, but keep the
                # original exception linked
                raise AccessFailedError(msg=str(e)) from e
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            self._progress_report_stop(progress_id)


class _SshCatProtocol(StdOutCapture, GeneratorMixIn):
    def __init__(self, done_future=None, encoding=None):
        StdOutCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self, )

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd == 1
        self.send_result(data)


class SshCat:
    def __init__(self, url: str, *additional_ssh_args):
        self._parsed = urlparse(url)
        # make sure the essential pieces exist
        assert self._parsed.hostname
        assert self._parsed.path
        self.ssh_args: list[str] = list(additional_ssh_args)

    def run(self, payload_cmd) -> Any | Generator:
        fpath = self._parsed.path
        cmd = ['ssh']
        cmd.extend(self.ssh_args)
        cmd.extend([
            '-e', 'none',
            self._parsed.hostname,
            payload_cmd.format(fpath=fpath),
        ])
        return ThreadedRunner(
            cmd=cmd,
            protocol_class=_SshCatProtocol,
            stdin=None,
        ).run()
