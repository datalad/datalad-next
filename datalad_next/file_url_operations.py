"""Handler for operations, such as "download", on file:// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
from shutil import COPY_BUFSIZE
import sys
from typing import Dict
from urllib import (
    request,
    parse,
)

from datalad.support.exceptions import DownloadError

from .url_operations import UrlOperations

lgr = logging.getLogger('datalad.ext.next.file_url_operations')


__all__ = ['FileUrlOperations']


class FileUrlOperations(UrlOperations):
    def _file_url_to_path(self, url):
        assert url.startswith('file://')
        parsed = parse.urlparse(url)
        path = request.url2pathname(parsed.path)
        return Path(path)

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
        try:
            # turn url into a native path
            from_path = self._file_url_to_path(from_url)
            # if anything went wrong with the conversion, or we lack
            # permissions: die here
            expected_size = from_path.stat().st_size
            props = {}
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
            raise DownloadError(msg=str(e)) from e
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            self._progress_report_stop(progress_id)
