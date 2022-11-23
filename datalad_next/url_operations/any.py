"""Meta URL handler with automatic scheme-based switching of implementations"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict
from urllib import (
    request,
    parse,
)

from datalad.support.exceptions import DownloadError

from .http import HttpUrlOperations
from .file import FileUrlOperations
from .ssh import SshUrlOperations
from . import UrlOperations

lgr = logging.getLogger('datalad.ext.next.any_url_operations')


__all__ = ['AnyUrlOperations']

# define handlers for each supported URL scheme
# extensions could patch their's in
# TODO support proper entrypoint mechanism
_urlscheme_handlers = dict(
    http=HttpUrlOperations,
    https=HttpUrlOperations,
    file=FileUrlOperations,
    ssh=SshUrlOperations,
)


class AnyUrlOperations(UrlOperations):
    """Handler for operations on any supported URLs

    The methods inspect the scheme of a given URL and call the corresponding
    methods for the `UrlOperations` implementation for that URL scheme.

    Parameter identity and semantics are unchanged with respect to the
    underlying implementations. See their documentation for details.

    An instance retains and reuses URL scheme handler instances for subsequent
    operations, such that held connections or cached credentials can be reused
    efficiently.
    """
    def __init__(self, cfg=None):
        super().__init__(cfg=cfg)
        # cache of already used handlers
        self._url_handlers = dict()

    def _get_handler(self, url: str) -> UrlOperations:
        scheme = self._get_url_scheme(url)
        try:
            # reuse existing handler, they might already have an idea on
            # authentication etc. from a previously processed URL
            url_handler = (
                self._url_handlers[scheme]
                if scheme in self._url_handlers
                else _urlscheme_handlers[scheme](cfg=self.cfg)
            )
        except KeyError:
            raise ValueError(f'unsupported URL scheme {scheme!r}')
        self._url_handlers[scheme] = url_handler
        return url_handler

    def _get_url_scheme(self, url) -> str:
        return url.split('://')[0]

    def is_supported_url(self, url) -> bool:
        # we require that any URL has a scheme
        scheme = self._get_url_scheme(url)
        return scheme in _urlscheme_handlers.keys()

    def sniff(self, url: str, *, credential: str = None) -> Dict:
        """Call `*UrlOperations.sniff()` for the respective URL scheme"""
        return self._get_handler(url).sniff(url, credential=credential)

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 credential: str = None,
                 hash: str = None) -> Dict:
        """Call `*UrlOperations.download()` for the respective URL scheme"""
        return self._get_handler(from_url).download(
            from_url, to_path, credential=credential, hash=hash)
