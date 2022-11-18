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
    """Abstraction for operations on URLs

    Support for specific URL schemes can be implemented via sub-classes.
    Such classes must comply with the following conditions:

    - Any configuration look-up must be performed with the `self._cfg`
      member, which is guaranteed to be a `ConfigManager` instance.

    - When downloads are to be supported, implement the `download()` method
      and comply with the behavior described in its documentation.

    This class provides a range of helper methods to aid computation of
    hashes and progress reporting.
    """
    def __init__(self, cfg=None):
        """
        Parameters
        ----------
        cfg: ConfigManager, optional
          A config manager instance that implementations will consult for
          any configuration items they may support.
        """
        self._cfg = cfg or datalad.cfg

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 credential: str = None,
                 hash: list[str] = None) -> Dict:
        """Download from a URL to a local file or stream to stdout

        Parameters
        ----------
        from_url: str
          Valid URL with any scheme supported by a particular implementation.
        to_path: Path or None
          A local platform-native path or `None`. If `None` the downloaded
          data is written to `stdout`, otherwise it is written to a file
          at the given path. The path is assumed to not exist. Any existing
          file will be oberwritten.
        credential: str, optional
          The name of a dedicated credential to be used for authentication
          in order to perform the download. Particular implementations may
          or may not require or support authentication. They also may or
          may not support automatic credential lookup.
        hash: list(algorithm_names), optional
          If given, must be a list of hash algorithm names supported by the
          `hashlib` module. A corresponding hash will be computed simultaenous
          to the download (without reading the data twice), and included
          in the return value.

        Returns
        -------
        dict
          A mapping of property names to values for the completed download.
          If `hash` algorithm names are provided, a corresponding key for
          each algorithm is included in this mapping, with the hexdigest
          of the corresponding checksum as the value.

        Raises
        ------
        DownloadError
          This exception is raised on any download-related error, with
          a summary of the underlying issues as its message. It carry
          a status code (e.g. HTTP status code) as its `status` property.
          Any underlying exception must be linked via the `__cause__`
          property (e.g. `raise DownloadError(...) from ...`).
        """
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
