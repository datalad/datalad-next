"""Handler for interfacing with FSSPEC for URL operations"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Dict

from fsspec.core import url_to_fs

from datalad_next.utils.consts import COPY_BUFSIZE

from . import (
    UrlOperations,
)

lgr = logging.getLogger('datalad.ext.next.url_operations.fsspec')


class FsspecUrlOperations(UrlOperations):
    """
    """
    def _sniff(self,
               url: str,
               *,
               credential: str | None = None,
               timeout: float | None = None) -> Dict:
        fs, urlpath = url_to_fs(url)
        return {
            '_fsspec_fs': fs,
            '_urlpath': urlpath,
            'content-length': fs.size(urlpath),
        }

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 credential: str | None = None,
                 hash: list[str] | None = None,
                 timeout: float | None = None) -> Dict:
        # this is pretty much shutil.copyfileobj() with the necessary
        # wrapping to perform hashing and progress reporting
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)

        dst_fp = None

        try:
            props = self._sniff(
                from_url,
            )
            fs = props.pop('_fsspec_fs')
            urlpath = props.pop('_urlpath')
            # we cannot always have an expected size
            expected_size = props.get('content-length')
            dst_fp = sys.stdout.buffer if to_path is None \
                else open(to_path, 'wb')
            # download can start
            self._progress_report_start(
                progress_id,
                ('Download %s to %s', from_url, to_path),
                'downloading',
                expected_size,
            )
            with fs.open(urlpath) as src_fp:
                # not every file abstraction supports all features
                # switch by capabilities
                if hasattr(src_fp, '__next__'):
                    # iterate full-auto if we can
                    self._download_via_iterable(
                        src_fp, dst_fp,
                        hasher, progress_id)
                elif expected_size is not None:
                    # read chunks until target size, if we know the size
                    # (e.g. the Tar filesystem would simply read beyond
                    # file boundaries otherwise
                    self._download_chunks_to_maxsize(
                        src_fp, dst_fp, expected_size,
                        hasher, progress_id)
                else:
                    # this needs a fallback that simply calls read()
                    raise NotImplementedError

            props.update(self._get_hash_report(hash, hasher))
            return props
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            self._progress_report_stop(progress_id, ('Finished download',))

    def _download_via_iterable(self, src_fp, dst_fp, hasher, progress_id):
        # Localize variable access to minimize overhead
        dst_fp_write = dst_fp.write
        for chunk in src_fp:
            # write data
            dst_fp_write(chunk)
            # compute hash simultaneously
            for h in hasher:
                h.update(chunk)
            self._progress_report_update(
                progress_id, ('Downloaded chunk',), len(chunk))

    def _download_chunks_to_maxsize(self, src_fp, dst_fp, size_to_copy,
                                    hasher, progress_id):
        # Localize variable access to minimize overhead
        src_fp_read = src_fp.read
        dst_fp_write = dst_fp.write
        while True:
            # make sure to not read beyond the target size
            # some archive filesystem abstractions do not necessarily
            # stop at the end of an archive member otherwise
            chunk = src_fp_read(min(COPY_BUFSIZE, size_to_copy))
            if not chunk:
                break
            # write data
            dst_fp_write(chunk)
            chunk_size = len(chunk)
            # compute hash simultaneously
            for h in hasher:
                h.update(chunk)
            self._progress_report_update(
                progress_id, ('Downloaded chunk',), chunk_size)
            size_to_copy -= chunk_size
