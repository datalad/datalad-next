"""Meta URL handler with automatic scheme-based switching of implementations"""

# allow for |-type UnionType declarations
from __future__ import annotations

from importlib import import_module
import logging
from pathlib import Path
import re
from typing import Dict
from urllib import (
    request,
    parse,
)

from .http import HttpUrlOperations
from .file import FileUrlOperations
from .ssh import SshUrlOperations
from . import UrlOperations

lgr = logging.getLogger('datalad.ext.next.url_operations.any')


__all__ = ['AnyUrlOperations']

# define handlers for each supported URL pattern
# the key in this dict is a regex match expression.
# the value is a tuple of containing module, and name of the
# class providing the handler
# extensions could patch their's in
# TODO support proper entrypoint mechanism
_url_handlers = dict(
    http=('datalad_next.url_operations.http', 'HttpUrlOperations'),
    file=('datalad_next.url_operations.file', 'FileUrlOperations'),
    ssh=('datalad_next.url_operations.ssh', 'SshUrlOperations'),
)


class AnyUrlOperations(UrlOperations):
    """Handler for operations on any supported URLs

    The methods inspect a given URL and call the corresponding
    methods for the `UrlOperations` implementation that matches the URL best.
    The "best match" is the match expression of a registered URL handler
    that yields the longest match against the given URL.

    Parameter identity and semantics are unchanged with respect to the
    underlying implementations. See their documentation for details.

    An instance retains and reuses URL scheme handler instances for subsequent
    operations, such that held connections or cached credentials can be reused
    efficiently.
    """
    def __init__(self, cfg=None):
        super().__init__(cfg=cfg)
        self._url_handlers = {
            re.compile(k): v for k, v in _url_handlers.items()
        }
        # cache of already used handlers
        self._url_handler_cache = dict()

    def _get_handler(self, url: str) -> UrlOperations:
        # match URL against all registered handlers and get the one with the
        # longest (AKA best) match
        longest_match = 0
        best_match = None
        for r in self._url_handlers:
            m = r.match(url)
            if not m:
                continue
            length = m.end() - m.start()
            if length > longest_match:
                best_match = r
                longest_match = length

        if best_match is None:
            raise ValueError(f'unsupported URL {url!r}')

        # reuse existing handler, they might already have an idea on
        # authentication etc. from a previously processed URL
        if best_match in self._url_handler_cache:
            return self._url_handler_cache[best_match]

        # we need to import the handler
        try:
            mod, cls = self._url_handlers[best_match]
            module = import_module(mod, package='datalad')
            handler_cls = getattr(module, cls)
            url_handler = handler_cls(cfg=self.cfg)
        except Exception as e:
            raise ValueError(
                'Cannot create URL handler instance for '
                f'{best_match.pattern!r} from {self._url_handlers[best_match]}') from e

        self._url_handler_cache[best_match] = url_handler
        return url_handler

    def is_supported_url(self, url) -> bool:
        return any(r.match(url) for r in self._url_handlers)

    def sniff(self,
              url: str,
              *,
              credential: str | None = None,
              timeout: float | None = None) -> Dict:
        """Call `*UrlOperations.sniff()` for the respective URL scheme"""
        return self._get_handler(url).sniff(
            url, credential=credential, timeout=timeout)

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 credential: str | None = None,
                 hash: list[str] | None = None,
                 timeout: float | None = None) -> Dict:
        """Call `*UrlOperations.download()` for the respective URL scheme"""
        return self._get_handler(from_url).download(
            from_url, to_path, credential=credential, hash=hash,
            timeout=timeout)

    def upload(self,
               from_path: Path | None,
               to_url: str,
               *,
               credential: str | None = None,
               hash: list[str] | None = None,
               timeout: float | None = None) -> Dict:
        """Call `*UrlOperations.upload()` for the respective URL scheme"""
        return self._get_handler(to_url).upload(
            from_path, to_url, credential=credential, hash=hash,
            timeout=timeout)

    def delete(self,
               url: str,
               *,
               credential: str | None = None,
               timeout: float | None = None) -> Dict:
        """Call `*UrlOperations.delete()` for the respective URL scheme"""
        return self._get_handler(url).delete(
            url, credential=credential, timeout=timeout)
