"""Handler for operations, such as "download", on file:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
try:
    from shutil import COPY_BUFSIZE
except ImportError:  # pragma: no cover
    # too old
    from datalad_next.utils import on_windows
    # from PY3.10
    COPY_BUFSIZE = 1024 * 1024 if on_windows else 64 * 1024
import sys
from typing import Dict
from urllib import (
    request,
    parse,
)

from datalad_next.exceptions import UrlTargetNotFound

from . import UrlOperations

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

    def sniff(self, url: str, *, credential: str = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.sniff`
        for parameter documentation.

        Raises
        ------
        UrlTargetNotFound
          Raises `UrlTargetNotFound` for download targets found absent.
        """
        # filter out internals
        return {
            k: v for k, v in self._sniff(url, credential).items()
            if not k.startswith('_')
        }

    def _sniff(self, url: str, credential: str = None) -> Dict:
        # turn url into a native path
        from_path = self._file_url_to_path(url)
        # if anything went wrong with the conversion, or we lack
        # permissions: die here
        try:
            size = from_path.stat().st_size
        except FileNotFoundError as e:
            raise UrlTargetNotFound(url) from e
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
                 credential: str = None,
                 hash: str = None) -> Dict:
        """Copy a local file to another location

        See :meth:`datalad_next.url_operations.UrlOperations.download`
        for parameter documentation.

        Raises
        ------
        UrlTargetNotFound
          Raises `UrlTargetNotFound` for download targets found absent.
        """
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)

        dst_fp = None
        try:
            props = self._sniff(from_url, credential=credential)
            from_path = props['_path']
            expected_size = props['content-length']
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')
            self._progress_report_start(
                progress_id, from_url, to_path, expected_size)

            with from_path.open('rb') as src_fp:
                # Localize variable access to minimize overhead
                src_fp_read = src_fp.read
                dst_fp_write = dst_fp.write
                while True:
                    chunk = src_fp_read(COPY_BUFSIZE)
                    if not chunk:
                        break
                    dst_fp_write(chunk)
                    self._progress_report_update(progress_id, len(chunk))
                    # compute hash simultaneously
                    for h in hasher:
                        h.update(chunk)
            props.update(self._get_hash_report(hash, hasher))
            return props
        except Exception as e:
            # wrap this into the datalad-standard, but keep the
            # original exception linked
            raise UrlTargetNotFound(msg=str(e)) from e
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            self._progress_report_stop(progress_id)
