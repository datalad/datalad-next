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

import datalad
from datalad.log import log_progress
from datalad.support.exceptions import DownloadError

lgr = logging.getLogger('datalad.ext.next.file_url_operations')


__all__ = ['FileUrlOperations']


# TODO make abstract base class for File|HttpUrlOperations
class FileUrlOperations:
    def __init__(self, cfg=None):
        self._cfg = cfg or datalad.cfg

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
        # TODO deduplicate with
        # HttpUrlOperations._stream_download_from_request()
        _hasher = self._get_hasher(hash)

        progress_id = f'download_{from_url}_{to_path}'

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
            log_progress(
                lgr.info,
                progress_id,
                'Download %s to %s', from_url, to_path,
                unit=' Bytes',
                label='Downloading',
                total=expected_size,
                noninteractive_level=logging.DEBUG,
            )
            with from_path.open('rb') as src_fp:
                # Localize variable access to minimize overhead
                src_fp_read = src_fp.read
                dst_fp_write = dst_fp.write
                while True:
                    chunk = src_fp_read(COPY_BUFSIZE)
                    if not chunk:
                        break
                    dst_fp_write(chunk)
                    log_progress(
                        lgr.info, progress_id,
                        'Downloaded chunk',
                        update=len(chunk),
                        increment=True,
                        noninteractive_level=logging.DEBUG,
                    )
                    # compute hash simultaneously
                    for h in _hasher:
                        h.update(chunk)
            if hash:
                props.update(dict(zip(hash, [h.hexdigest() for h in _hasher])))
            return props
        except Exception as e:
            # wrap this into the datalad-standard, but keep the
            # original exception linked
            raise DownloadError(msg=str(e)) from e
        finally:
            if dst_fp and to_path is not None:
                dst_fp.close()
            log_progress(
                lgr.info, progress_id, 'Finished download',
                noninteractive_level=logging.DEBUG,
            )

    def _get_hasher(self, hash: list[str]) -> list:
        if not hash:
            return []

        import hashlib
        # yes, this will crash, if an invalid hash algorithm name
        # is given
        _hasher = []
        for h in hash:
            hr = getattr(hashlib, h.lower(), None)
            if hr is None:
                raise ValueError(f'unsupported hash algorithm {h}')
            _hasher.append(hr())
        return _hasher
