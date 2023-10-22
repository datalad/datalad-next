"""Handler for operations, such as "download", on ssh:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
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
    Dict,
    Generator,
    IO,
)
from urllib.parse import urlparse

from datalad_next.runners import (
    NoCaptureGeneratorProtocol,
    StdOutCaptureGeneratorProtocol,
)

from datalad_next.runners.data_processors import pattern_processor
from datalad_next.runners.data_processor_pipeline import DataProcessorPipeline
from datalad_next.runners.run import run
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
    # a connection error or other problem unrelated to the present of a file
    # would a different error code (255 in case of a connection error)
    _stat_cmd = "printf \"\1\2\3\"; ls '{fpath}' &> /dev/null " \
                "&& ls -nl '{fpath}' | awk 'BEGIN {{ORS=\"\1\"}} {{print $5}}' " \
                "|| exit 244"
    _cat_cmd = "cat '{fpath}'"

    def _check_return_code(self, url, stream):
        # At this point the subprocess has either exited, was terminated, or
        # was killed.
        if stream.return_code == 244:
            # this is the special code for a file-not-found
            raise UrlOperationsResourceUnknown(url)
        elif stream.return_code != 0:
            raise UrlOperationsRemoteError(
                url,
                message=f'ssh process returned {stream.return_code}'
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
        ssh_cat = _SshCat(url)
        cmd = ssh_cat.get_cmd(SshUrlOperations._stat_cmd)
        with run(cmd, protocol_class=StdOutCaptureGeneratorProtocol) as stream:
            props = self._get_props(url, stream)

        # At this point the subprocess has either exited, was terminated, or
        # was killed.
        self._check_return_code(url, stream)
        return {k: v for k, v in props.items() if not k.startswith('_')}

    def _get_props(self, url, stream: Generator) -> Dict:
        # The try clause enables us to execute the code after the context
        # handler if the iterator stops unexpectedly. That would, for
        # example be the case, if the ssh-subprocess terminates prematurely,
        # for example, due to a missing file.
        # (An alternative way to detect and handle the exit would be to
        # implement some handling in the protocol.connection_lost callback
        # and send the result to the generator, e.g. via:
        # protocol.send(('process-exit', self.process.poll()))
        try:
            # any stream must start with this magic marker, or we do not
            # recognize what is happening
            # after this marker, the server will send the size of the
            # to-be-downloaded file in bytes, followed by another magic
            # b'\1', and the file content after that
            magic_marker = b'\1\2\3'

            # Create a pipeline object that contains a single data
            # processors, i.e. the "pattern_border_processor". It guarantees, that
            # each chunk has at least the size of the pattern and that no chunk
            # ends with a pattern prefix (except from the last chunk).
            # (We could have used the convenience wrapper "process_from", but we
            # want to remove the filter again below. This requires us to have a
            # ProcessorPipeline-object).
            pipeline = DataProcessorPipeline([pattern_processor(magic_marker)])
            filtered_stream = pipeline.process_from(stream)

            # The first chunk should start with the magic marker, i.e. b'\1\2\3'
            chunk = next(filtered_stream)
            if chunk[:len(magic_marker)] != magic_marker:
                raise RuntimeError("Protocol error: report header not received")

            # Remove the filter again. The chunk is extended to contain all
            # data that was buffered in the pipeline.
            chunk = b''.join([chunk[len(magic_marker):]] + pipeline.finalize())

            # The length is transferred now and terminated by b'\x01'.
            while b'\x01' not in chunk:
                chunk += next(stream)

            marker_index = chunk.index(b'\x01')
            expected_size = int(chunk[:marker_index])
            chunk = chunk[marker_index + 1:]
            props = {
                'content-length': expected_size,
                '_stream': chain([chunk], stream) if chunk else stream
            }
            return props

        except StopIteration:
            self._check_return_code(url, stream)

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

        ssh_cat = _SshCat(from_url)
        cmd = ssh_cat.get_cmd(f'{SshUrlOperations._stat_cmd}; {SshUrlOperations._cat_cmd}')
        with run(cmd, protocol_class=StdOutCaptureGeneratorProtocol) as stream:

            props = self._get_props(from_url, stream)
            expected_size = props['content-length']
            # The stream might have changed due to not yet processed, but
            # fetched data, that is now chained in front of it. Therefore we get
            # the updated stream from the props
            stream = props.pop('_stream')

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
                hasher.update(chunk)
                self._progress_report_update(
                    progress_id, ('Downloaded chunk',), len(chunk))
            props.update(hasher.get_hexdigest())

        # At this point the subprocess has either exited, was terminated, or
        # was killed.
        if stream.return_code == 244:
            # this is the special code for a file-not-found
            raise UrlOperationsResourceUnknown(from_url)
        elif stream.return_code != 0:
            raise UrlOperationsRemoteError(
                from_url,
                message=f'ssh process returned {stream.return_code}'
            )
        return props

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
        # actually track the upload, i.e. the feeding of the stdin pipe
        # of the ssh-process, and not just the feeding of the
        # queue.
        upload_queue = Queue(maxsize=2)

        cmd = _SshCat(to_url).get_cmd(
            # leave special exit code when writing fails, but not the
            # general SSH access
            "( mkdir -p '{fdir}' && cat > '{fpath}' ) || exit 244"
        )
        with run(cmd, NoCaptureGeneratorProtocol, stdin=upload_queue, timeout=timeout) as ssh:
            # file is open, we can start progress tracking
            progress_id = self._get_progress_id(source_name, to_url)
            self._progress_report_start(
                progress_id,
                ('Upload %s to %s', source_name, to_url),
                'uploading',
                expected_size,
            )
            upload_size = 0
            while True:
                chunk = src_fp.read(COPY_BUFSIZE)
                # Leave the write-loop at eof
                if chunk == b'':
                    break

                # If the ssh-subprocess exited, leave the write loop, the
                # result will be interpreted below
                if ssh.runner.process.poll() is not None:
                    break

                chunk_size = len(chunk)
                # compute hash simultaneously
                hasher.update(chunk)

                # we are just putting stuff in the queue, and rely on
                # its maxsize to cause it to block the next call to
                # have the progress reports be anyhow valid
                try:
                    upload_queue.put(chunk, timeout=timeout)
                except Full:
                    raise TimeoutError

                self._progress_report_update(
                    progress_id, ('Uploaded chunk',), chunk_size)
                upload_size += chunk_size

            # we're done, close queue
            try:
                upload_queue.put(None, timeout=timeout)
            except Full:
                # Everything is done. If we leave the context the subprocess
                # will be treated as specified in the context initialization,
                # either wait for it, terminate, or kill it.
                pass

        # At this point the subprocess has terminated by itself or was killed.
        if ssh.return_code == 244:
            raise UrlOperationsResourceUnknown(to_url)
        elif ssh.return_code != 0:
            raise UrlOperationsRemoteError(
                to_url,
                message=f'ssh exited with return value: {ssh.return_code}')

        assert ssh.return_code == 0, f"Unexpected ssh return value: {ssh.return_code}"
        return {
            **hasher.get_hexdigest(),
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

    def get_cmd(self, payload_cmd: str) -> list[str]:
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
        return cmd
