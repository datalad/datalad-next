"""Meta URL handler with automatic scheme-based switching of implementations"""

# allow for |-type UnionType declarations
from __future__ import annotations

from importlib import import_module
import json
import logging
from pathlib import Path
import re
from typing import Dict

from datalad_next.config import ConfigManager
from datalad_next.exceptions import CapturedException

from . import UrlOperations

lgr = logging.getLogger('datalad.ext.next.url_operations.any')


__all__ = ['AnyUrlOperations']

# define handlers for each supported URL pattern
# FORMAT OF HANDLER REGISTRY (dict)
# - key: regex match expression to be apply on a URL (to test whether a
#   particular handler should be used for a given URL)
# - value: tuple (handler specification, see below)
# FORMAT OF HANDLER SPECIFICATION
# - tuple of min-length 1
# - item1: str, handler class to import
#   e.g., package.module.class
# - item2: dict, optional, kwargs to pass to the handler constructor

# TODO support proper entrypoint mechanism
# It is best to only record handlers here for which there is no alternative,
# because the best handler is determined based on this information
# and only this handler is imported. If that fails, there is no fallback.
# Handlers that may or may not work under given conditions should only
# be added via external logic after they have been found to be "working"
# on a given installation.
_url_handlers = {
    'http': ('datalad_next.url_operations.http.HttpUrlOperations',),
    'file': ('datalad_next.url_operations.file.FileUrlOperations',),
    'ssh': ('datalad_next.url_operations.ssh.SshUrlOperations',),
}


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
    def __init__(self, cfg: ConfigManager | None = None):
        """
        Parameters
        ----------
        cfg: ConfigManager, optional
          A config manager instance that is consulted for any configuration
          filesystem configuration individual handlers may support.
        """
        super().__init__(cfg=cfg)
        self._load_handler_registery()
        # cache of already used handlers
        self._url_handler_cache = dict()

    def _load_handler_registery(self):
        # update with handlers from config
        # https://github.com/datalad/datalad-next/issues/217
        cfgh = {}
        for citem in self.cfg.keys():
            if not citem.startswith('datalad.url-handler.'):
                # none of our business
                continue
            # the match expression is right in the item key
            # (all but the first two and the last segment)
            citem_l = citem.split('.')
            match = '.'.join(citem_l[2:-1])
            prop = citem_l[-1]
            value = self.cfg[citem]
            if prop != 'class':
                try:
                    value = json.loads(value)
                except Exception as e:
                    ce = CapturedException(e)
                    lgr.debug(
                        'Ignoring invalid URL handler configuration '
                        'for %r(%s): %r [%s]',
                        match, prop, value, ce)
                    continue
            hc = cfgh.get(match, {})
            hc[prop] = value
            cfgh[match] = hc
        # merge all specs
        uh = dict(_url_handlers)
        for match, spec in cfgh.items():
            try:
                uh[match] = (spec['class'], spec['kwargs'])
            except KeyError:
                try:
                    uh[match] = (spec['class'],)
                except Exception as e:
                    CapturedException(e)
                    lgr.debug(
                        'Ignoring incomplete URL handler specification '
                        'for %r: %r', match, spec)
        self._url_handlers = {}
        for k, v in uh.items():
            # compile matches to finalize
            lgr.log(8, 'Add URL handler for %r: %r', k, v)
            self._url_handlers[re.compile(k)] = v

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
            handler_spec = self._url_handlers[best_match]
            # split the import declaration into units
            toimport = handler_spec[0].split('.')
            # the handler class is the last unit
            cls = toimport[-1]
            # the rest is the module
            mod = '.'.join(toimport[:-1])
            module = import_module(mod, package='datalad')
            handler_cls = getattr(module, cls)
            handler_kwargs = handler_spec[1] if len(handler_spec) > 1 else {}
            url_handler = handler_cls(cfg=self.cfg, **handler_kwargs)
        except Exception as e:
            raise ValueError(
                'Cannot create URL handler instance for '
                f'{best_match.pattern!r} from {self._url_handlers[best_match]}') from e

        self._url_handler_cache[best_match] = url_handler
        return url_handler

    def is_supported_url(self, url) -> bool:
        return any(r.match(url) for r in self._url_handlers)

    def stat(self,
             url: str,
             *,
             credential: str | None = None,
             timeout: float | None = None) -> Dict:
        """Call `*UrlOperations.stat()` for the respective URL scheme"""
        return self._get_handler(url).stat(
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
