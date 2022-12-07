"""Handler for operations, such as "download", on ssh:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
import subprocess
import sys
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
    Any,
    Dict,
    Generator,
    IO,
)
from urllib.parse import urlparse

from datalad.runner.protocol import WitlessProtocol
from datalad.runner.coreprotocols import NoCapture

from datalad.runner import StdOutCapture
from datalad.runner.protocol import GeneratorMixIn
from datalad.runner.nonasyncrunner import ThreadedRunner

from datalad_next.exceptions import CommandError
from datalad_next.utils.consts import COPY_BUFSIZE

from . import (
    UrlOperations,
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
    # a conenction error or other problem unrelated to the present of a file
    # would a different error code (255 in case of a connection error)
    _stat_cmd = "printf \"\1\2\3\"; ls '{fpath}' &> /dev/null " \
                "&& ls -nl '{fpath}' | awk 'BEGIN {{ORS=\"\1\"}} {{print $5}}' " \
                "|| exit 244"
    _cat_cmd = "cat '{fpath}'"

    def sniff(self,
              url: str,
              *,
              credential: str | None = None,
              timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.sniff`
        for parameter documentation and exception behavior.
        """
        try:
            props = self._sniff(
                url,
                cmd=SshUrlOperations._stat_cmd,
            )
        except CommandError as e:
            if e.code == 244:
                # this is the special code for a file-not-found
                raise UrlOperationsResourceUnknown(url) from e
            else:
                raise UrlOperationsRemoteError(url, message=str(e)) from e

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

        ssh_cat = _SshCat(url)
        stream = ssh_cat.run(cmd, protocol=_StdOutCaptureGeneratorProtocol)
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
                 credential: str | None = None,
                 hash: str | None = None,
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
                progress_id,
                ('Download %s to %s', from_url, to_path),
                'downloading',
                expected_size,
            )
            for chunk in stream:
                # write data
                dst_fp_write(chunk)
                # compute hash simultaneously
                for h in hasher:
                    h.update(chunk)
                self._progress_report_update(
                    progress_id, ('Downloaded chunk',), len(chunk))
            props.update(self._get_hash_report(hash, hasher))
            return props
        except CommandError as e:
            if e.code == 244:
                # this is the special code for a file-not-found
                raise UrlOperationsResourceUnknown(from_url) from e
            else:
                # wrap this into the datalad-standard, but keep the
                # original exception linked
                raise UrlOperationsRemoteError(from_url, message=str(e)) from e
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            self._progress_report_stop(progress_id, ('Finished download',))

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
                    source_name=from_path,
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
                        timeout: int | None) -> dict:

        hasher = self._get_hasher(hash_names)

        # we limit the queue to few items in order to `make queue.put()`
        # block relatively quickly, and thereby have the progress report
        # actually track the upload, and not just the feeding of the
        # queue
        upload_queue = Queue(maxsize=2)

        ssh_cat = _SshCat(to_url)
        ssh_runner_generator = ssh_cat.run(
            # leave special exit code when writing fails, but not the
            # general SSH access
            "( mkdir -p '{fdir}' && cat > '{fpath}' ) || exit 244",
            protocol=_NoCaptureGeneratorProtocol,
            stdin=upload_queue,
            timeout=timeout,
        )

        # file is open, we can start progress tracking
        progress_id = self._get_progress_id(source_name, to_url)
        self._progress_report_start(
            progress_id,
            ('Upload %s to %s', source_name, to_url),
            'uploading',
            expected_size,
        )
        try:
            upload_size = 0
            while ssh_runner_generator.runner.process.poll() is None:
                chunk = src_fp.read(COPY_BUFSIZE)
                if chunk == b'':
                    break
                chunk_size = len(chunk)
                # compute hash simultaneously
                for h in hasher:
                    h.update(chunk)
                # we are just putting stuff in the queue, and rely on
                # its maxsize to cause it to block the next call to
                # have the progress reports be anyhow valid
                upload_queue.put(chunk, timeout=timeout)
                self._progress_report_update(
                    progress_id, ('Uploaded chunk',), chunk_size)
                upload_size += chunk_size
            # we're done, close queue
            upload_queue.put(None, timeout=timeout)

            # Exhaust the generator, that might raise CommandError
            # or TimeoutError, if timeout was not `None`.
            tuple(ssh_runner_generator)
        except CommandError as e:
            if e.code == 244:
                raise UrlOperationsResourceUnknown(to_url) from e
            else:
                raise UrlOperationsRemoteError(to_url, message=str(e)) from e
        except (TimeoutError, Full):
            ssh_runner_generator.runner.process.kill()
            raise TimeoutError
        finally:
            self._progress_report_stop(progress_id, ('Finished upload',))

        assert ssh_runner_generator.return_code == 0, "Unexpected ssh " \
            f"return value: {ssh_runner_generator.return_code}"

        return {
            **self._get_hash_report(hash_names, hasher),
            # return how much was copied. we could compare with
            # `expected_size` and error on mismatch, but not all
            # sources can provide that (e.g. stdin)
            'content-length': upload_size
        }


class _SshCat:
    def __init__(self, url: str, *additional_ssh_args):
        self._parsed = urlparse(url)
        # make sure the essential pieces exist
        assert self._parsed.hostname
        assert self._parsed.path
        self.ssh_args: list[str] = list(additional_ssh_args)

    def run(self,
            payload_cmd: str,
            protocol: type[WitlessProtocol],
            stdin: Queue | None = None,
            timeout: float | None = None) -> Any | Generator:
        fpath = self._parsed.path
        cmd = ['ssh']
        cmd.extend(self.ssh_args)
        cmd.extend([
            '-e', 'none',
            self._parsed.hostname,
            payload_cmd.format(
                fdir=str(PurePosixPath(fpath).parent),
                fpath=fpath,
            ),
        ])
        return ThreadedRunner(
            cmd=cmd,
            protocol_class=protocol,
            stdin=subprocess.DEVNULL if stdin is None else stdin,
            timeout=timeout,
        ).run()


#
# Below are generic generator protocols that should be provided
# upstream
#
class _NoCaptureGeneratorProtocol(NoCapture, GeneratorMixIn):
    def __init__(self, done_future=None, encoding=None):
        NoCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout: process has not terminated yet")


class _StdOutCaptureGeneratorProtocol(StdOutCapture, GeneratorMixIn):
    def __init__(self, done_future=None, encoding=None):
        StdOutCapture.__init__(self, done_future, encoding)
        GeneratorMixIn.__init__(self)

    def pipe_data_received(self, fd: int, data: bytes):
        assert fd == 1
        self.send_result(data)

    def timeout(self, fd):
        raise TimeoutError(f"Runner timeout {fd}")
