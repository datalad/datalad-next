"""Abstract base class for URL operation handlers"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import datalad
from datalad.log import log_progress

lgr = logging.getLogger('datalad.ext.next.url_operations')


class UrlOperations:
    def __init__(self, cfg=None):
        self._cfg = cfg or datalad.cfg

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 credential: str = None,
                 hash: str = None) -> Dict:
        """Download from a URL to a local file or stream to stdout"""
        raise NotImplementedError

    def _get_progress_id(self, from_url, to_path):
        return f'download_{from_url}_{to_path}'

    def _progress_report_start(self, pid, from_url, to_path, expected_size):
        log_progress(
            lgr.info,
            pid,
            'Download %s to %s', from_url, to_path,
            unit=' Bytes',
            label='Downloading',
            total=expected_size,
            noninteractive_level=logging.DEBUG,
        )

    def _progress_report_update(self, pid, increment):
        log_progress(
            lgr.info, pid,
            'Downloaded chunk',
            update=increment,
            increment=True,
            noninteractive_level=logging.DEBUG,
        )

    def _progress_report_stop(self, pid):
        log_progress(
            lgr.info, pid, 'Finished download',
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

    def _get_hash_report(self, hash_names: list[str], hashers: list) -> Dict:
        if not hash_names:
            return {}
        else:
            return dict(zip(hash_names, [h.hexdigest() for h in hashers]))
