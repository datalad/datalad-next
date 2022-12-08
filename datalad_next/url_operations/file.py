"""Handler for operations, such as "download", on file:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Dict
from urllib import (
    request,
    parse,
)

from datalad_next.utils.consts import COPY_BUFSIZE

from . import (
    UrlOperations,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)

lgr = logging.getLogger('datalad.ext.next.file_url_operations')


__all__ = ['FileUrlOperations']


class FileUrlOperations(UrlOperations):
    """Handler for operations on `file://` URLs

    Access to local data via file-scheme URLs is supported with the
    same API and feature set as other URL-schemes (simultaneous
    content hashing and progress reporting.
    """
    def _file_url_to_path(self, url):
        assert url.startswith('file://')
        parsed = parse.urlparse(url)
        path = request.url2pathname(parsed.path)
        return Path(path)

    def sniff(self,
              url: str,
              *,
              credential: str | None = None,
              timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.sniff`
        for parameter documentation and exception behavior.

        Raises
        ------
        UrlOperationsResourceUnknown
          For access targets found absent.
        """
        # filter out internals
        return {
            k: v for k, v in self._sniff(url, credential).items()
            if not k.startswith('_')
        }

    def _sniff(self, url: str, credential: str | None = None) -> Dict:
        # turn url into a native path
        from_path = self._file_url_to_path(url)
        # if anything went wrong with the conversion, or we lack
        # permissions: die here
        try:
            size = from_path.stat().st_size
        except FileNotFoundError as e:
            raise UrlOperationsResourceUnknown(url) from e
        return {
            'content-length': size,
            '_path': from_path,
        }

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
        """Copy a file:// URL target to a local path

        See :meth:`datalad_next.url_operations.UrlOperations.download`
        for parameter documentation and exception behavior.

        Raises
        ------
        UrlOperationsResourceUnknown
          For download targets found absent.
        """
        dst_fp = None
        try:
            props = self._sniff(from_url, credential=credential)
            from_path = props['_path']
            expected_size = props['content-length']
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')

            with from_path.open('rb') as src_fp:
                props.update(self._copyfp(
                    src_fp,
                    dst_fp,
                    expected_size,
                    hash,
                    start_log=('Download %s to %s', from_url, to_path),
                    update_log=('Downloaded chunk',),
                    finish_log=('Finished download',),
                    progress_label='downloading',
                ))
                return props
        except PermissionError:
            # would be a local issue, pass-through
            raise
        except UrlOperationsResourceUnknown:
            # would come from sniff(), pass_through
            raise
        except Exception as e:
            # wrap this into the datalad-standard, but keep the
            # original exception linked
            raise UrlOperationsRemoteError(from_url, message=str(e)) from e
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()

    def upload(self,
               from_path: Path | None,
               to_url: str,
               *,
               credential: str | None = None,
               hash: list[str] | None = None,
               timeout: float | None = None) -> Dict:
        """Copy a local file to a file:// URL target

        Any missing parent directories of the URL target are created as
        necessary.

        See :meth:`datalad_next.url_operations.UrlOperations.upload`
        for parameter documentation and exception behavior.

        Raises
        ------
        FileNotFoundError
          If the source file cannot be found.
        """
        # get the size, or die if inaccessible
        props = {}
        if from_path:
            expected_size = from_path.stat().st_size
            props['content-length'] = expected_size
        else:
            expected_size = None
        to_path = self._file_url_to_path(to_url)
        # create parent dir(s) as necessary
        to_path.parent.mkdir(exist_ok=True, parents=True)
        src_fp = None
        try:
            src_fp = sys.stdin.buffer if from_path is None \
                else open(from_path, 'rb')
            with to_path.open('wb') as dst_fp:
                props.update(self._copyfp(
                    src_fp,
                    dst_fp,
                    expected_size,
                    hash,
                    start_log=('Upload %s to %s', from_path, to_url),
                    update_log=('Uploaded chunk',),
                    finish_log=('Finished upload',),
                    progress_label='uploading',
                ))
                return props
        except FileNotFoundError as e:
            raise UrlOperationsResourceUnknown(url) from e
        except Exception as e:
            # wrap this into the datalad-standard, but keep the
            # original exception linked
            raise UrlOperationsRemoteError(from_url, message=str(e)) from e
        finally:
            if src_fp and from_path is not None:
                src_fp.close()

    def delete(self,
               url: str,
               *,
               credential: str | None = None,
               timeout: float | None = None) -> Dict:
        """Delete the target of a file:// URL

        The target can be a file or a directory. If it is a directory, it has
        to be empty.

        See :meth:`datalad_next.url_operations.UrlOperations.delete`
        for parameter documentation and exception behavior.

        Raises
        ------
        UrlOperationsResourceUnknown
          For deletion targets found absent.
        """
        path = self._file_url_to_path(url)
        try:
            path.unlink()
        except FileNotFoundError as e:
            raise UrlOperationsResourceUnknown(url) from e
        except IsADirectoryError:
            try:
                path.rmdir()
            except Exception as e:
                raise UrlOperationsRemoteError(url, message=str(e)) from e
        except Exception as e:
            # wrap this into the datalad-standard, but keep the
            # original exception linked
            raise UrlOperationsRemoteError(url, message=str(e)) from e

    def _copyfp(self,
                src_fp: file,
                dst_fp: file,
                expected_size: int,
                hash: list[str] | None,
                start_log: tuple,
                update_log: tuple,
                finish_log: tuple,
                progress_label: str,
    ) -> dict:
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(id(src_fp), id(src_fp))

        # Localize variable access to minimize overhead
        src_fp_read = src_fp.read
        dst_fp_write = dst_fp.write

        props = {}
        self._progress_report_start(
            progress_id, start_log, progress_label, expected_size)
        copy_size = 0
        try:
            while True:
                chunk = src_fp_read(COPY_BUFSIZE)
                if not chunk:
                    break
                dst_fp_write(chunk)
                chunk_size = len(chunk)
                self._progress_report_update(
                    progress_id, update_log, chunk_size)
                # compute hash simultaneously
                for h in hasher:
                    h.update(chunk)
                copy_size += chunk_size
            props.update(self._get_hash_report(hash, hasher))
            # return how much was copied. we could compare with
            # `expected_size` and error on mismatch, but not all
            # sources can provide that (e.g. stdin)
            props['content-length'] = copy_size
            return props
        finally:
            self._progress_report_stop(progress_id, finish_log)
