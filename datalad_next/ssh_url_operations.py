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
from datalad.support.exceptions import DownloadError

from .url_operations import UrlOperations

lgr = logging.getLogger('datalad.ext.next.ssh_url_operations')


__all__ = ['SshUrlOperations']


class SshUrlOperations(UrlOperations):
    """
    For downloading files, only server that support execution of the commands
    'printf', 'ls -nl', 'awk', and 'cat' are supported. This include a wide
    range of operating systems, including devices that provide these commands
    via the 'busybox' software.
    """
    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 # unused, but theoretically could be used to
                 # obtain escalated/different privileges on a system
                 # to gain file access
                 credential: str = None,
                 hash: str = None) -> Dict:
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)

        dst_fp = None

        # any stream must start with this magic marker, or we do not
        # recognize what is happening
        # after this marker, the server will send the size of the
        # to-be-downloaded file in bytes, followed by another magic
        # b'\1', and the file content after that
        need_magic = b'\1\2\3'
        expected_size_str = b''
        expected_size = None

        try:
            props = {}
            ssh_cat = SshCat(from_url)
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')
            # Localize variable access to minimize overhead
            dst_fp_write = dst_fp.write
            for chunk in ssh_cat.run():
                if need_magic:
                    expected_magic = need_magic[:min(len(need_magic),
                                                     len(chunk))]
                    incoming_magic = chunk[:len(need_magic)]
                    # does the incoming data have the remaining magic bytes?
                    if incoming_magic != expected_magic:
                        raise ValueError("magic missing")
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
                        # download can start
                        self._progress_report_start(
                            progress_id, from_url, to_path, expected_size)
                if not expected_size:
                    # if we do not yet have the size info
                    continue
                # write data
                dst_fp_write(chunk)
                # compute hash simultaneously
                for h in hasher:
                    h.update(chunk)
                self._progress_report_update(progress_id, len(chunk))
            props.update(self._get_hash_report(hash, hasher))
            return props
        except Exception as e:
            # wrap this into the datalad-standard, but keep the
            # original exception linked
            raise DownloadError(msg=str(e)) from e
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

    def run(self) -> Any | Generator:
        fpath = self._parsed.path
        cmd = ['ssh']
        cmd.extend(self.ssh_args)
        cmd.extend([
            '-e', 'none',
            self._parsed.hostname,
            f"printf \"\1\2\3\"; ls -nl '{fpath}' | awk 'BEGIN {{ORS=\"\1\"}} {{print $5}}'; cat '{fpath}'",
        ])
        return ThreadedRunner(
            cmd=cmd,
            protocol_class=_SshCatProtocol,
            stdin=None,
        ).run()
