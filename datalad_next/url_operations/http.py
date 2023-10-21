"""Handler for operations, such as "download", on http(s):// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Dict
import requests
from requests_toolbelt import user_agent
import www_authenticate

import datalad

from datalad_next.utils.requests_auth import DataladAuth
from . import (
    UrlOperations,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)

lgr = logging.getLogger('datalad.ext.next.url_operations.http')


__all__ = ['HttpUrlOperations']


class HttpUrlOperations(UrlOperations):
    """Handler for operations on `http(s)://` URLs

    This handler is built on the `requests` package. For authentication, it
    employes :class:`datalad_next.utils.requests_auth.DataladAuth`, an adaptor
    that consults the DataLad credential system in order to fulfill HTTP
    authentication challenges.
    """

    def __init__(self, cfg=None, headers: Dict | None = None):
        """
        Parameters
        ----------
        cfg: ConfigManager, optional
          A config manager instance that is consulted for any configuration
          filesystem configuration individual handlers may support.
        headers: dict, optional
          Additional or alternative headers to add to a request. The default
          headers contain a ``user-agent`` declaration. Any headers provided
          here override corresponding defaults.
        """
        super().__init__(cfg=cfg)
        self._headers = {
            'user-agent': user_agent('datalad', datalad.__version__),
        }
        if headers:
            self._headers.update(headers)

    def get_headers(self, headers: Dict | None = None) -> Dict:
        # start with the default
        hdrs = dict(self._headers)
        if headers is not None:
            hdrs.update(headers)
        return hdrs

    def stat(self,
             url: str,
             *,
             credential: str | None = None,
             timeout: float | None = None) -> Dict:
        """Gather information on a URL target, without downloading it

        See :meth:`datalad_next.url_operations.UrlOperations.stat`
        for parameter documentation and exception behavior.

        Raises
        ------
        UrlOperationsResourceUnknown
          For access targets found absent.
        """
        auth = DataladAuth(self.cfg, credential=credential)
        with requests.head(
                url,
                headers=self.get_headers(),
                auth=auth,
                # we want to match the `get` behavior explicitly
                # in order to arrive at the final URL after any
                # redirects that get would also end up with
                allow_redirects=True,
        ) as r:
            # fail visible for any non-OK outcome
            try:
                r.raise_for_status()
            except requests.exceptions.RequestException as e:
                # wrap this into the datalad-standard, but keep the
                # original exception linked
                if e.response.status_code == 404:
                    # special case reporting for a 404
                    raise UrlOperationsResourceUnknown(
                        url, status_code=e.response.status_code) from e
                else:
                    raise UrlOperationsRemoteError(
                        url, message=str(e), status_code=e.response.status_code
                        ) from e
            props = {
                # standardize on lower-case header keys.
                # also prefix anything other than 'content-length' to make
                # room for future standardizations
                k.lower() if k.lower() == 'content-length' else f'http-{k.lower()}':
                v
                for k, v in r.headers.items()
            }
            props['url'] = r.url
        auth.save_entered_credential(
            context=f"for accessing {url}"
        )
        if 'content-length' in props:
            # make an effort to return size in bytes as int
            try:
                props['content-length'] = int(props['content-length'])
            except (TypeError, ValueError):
                # but be reasonably robust against unexpected responses
                pass
        return props

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 *,
                 credential: str | None = None,
                 hash: list[str] | None = None,
                 timeout: float | None = None) -> Dict:
        """Download via HTTP GET request

        See :meth:`datalad_next.url_operations.UrlOperations.download`
        for parameter documentation and exception behavior.

        Raises
        ------
        UrlOperationsResourceUnknown
          For download targets found absent.
        """
        # a new manager per request
        # TODO optimize later to cache credentials per target
        # similar to requests_toolbelt.auth.handler.AuthHandler
        auth = DataladAuth(self.cfg, credential=credential)
        with requests.get(
                from_url,
                stream=True,
                headers=self.get_headers(),
                auth=auth,
        ) as r:
            # fail visible for any non-OK outcome
            try:
                r.raise_for_status()
            except requests.exceptions.RequestException as e:
                # wrap this into the datalad-standard, but keep the
                # original exception linked
                if e.response.status_code == 404:
                    # special case reporting for a 404
                    raise UrlOperationsResourceUnknown(
                        from_url, status_code=e.response.status_code) from e
                else:
                    raise UrlOperationsRemoteError(
                        from_url, message=str(e), status_code=e.response.status_code
                        ) from e

            download_props = self._stream_download_from_request(
                r, to_path, hash=hash)
        auth.save_entered_credential(
            context=f'download from {from_url}'
        )
        return download_props

    def probe_url(self, url, timeout=10.0, headers=None):
        """Probe a HTTP(S) URL for redirects and authentication needs

        This functions performs a HEAD request against the given URL,
        while waiting at most for the given timeout duration for
        a server response.

        Parameters
        ----------
        url: str
          URL to probe
        timeout: float, optional
          Maximum time to wait for a server response to the probe
        headers: dict, optional
          Any custom headers to use for the probe request. If none are
          provided, or the provided headers contain no 'user-agent'
          field, the default DataLad user agent is added automatically.

        Returns
        -------
        str or None, dict
          The first value is the URL against the final request was
          performed, after following any redirects and applying
          normalizations.

          The second value is a mapping with a particular set of
          properties inferred from probing the webserver. The following
          key-value pairs are supported:

          - 'is_redirect' (bool), True if any redirection occurred. This
            boolean property is a more accurate test than comparing
            input and output URL
          - 'status_code' (int), HTTP response code (of the final request
            in case of redirection).
          - 'auth' (dict), present if the final server response contained any
            'www-authenticate' headers, typically the case for 401 responses.
            The dict contains a mapping of server-reported authentication
            scheme names (e.g., 'basic', 'bearer') to their respective
            properties (dict). These can be any nature and number, depending
            on the respective authentication scheme. Most notably, they
            may contain a 'realm' property that can be used to determine
            suitable credentials for authentication.

        Raises
        ------
        requests.RequestException
          May raise any exception of the `requests` package, most notably
          `ConnectionError`, `Timeout`, `TooManyRedirects`, etc.
        """
        hdrs = self.get_headers()
        if headers is None:
            headers = hdrs
        elif 'user-agent' not in headers:
            headers.update(hdrs)

        props = {}
        req = requests.head(
            url,
            allow_redirects=True,
            timeout=timeout,
            headers=headers,
        )
        if 'www-authenticate' in req.headers:
            props['auth'] = www_authenticate.parse(
                req.headers['www-authenticate'])
        props['is_redirect'] = True if req.history else False
        props['status_code'] = req.status_code
        return req.url, props

    def _stream_download_from_request(
            self, r, to_path, hash: list[str] | None = None) -> Dict:
        from_url = r.url
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)
        # get download size, but not every server provides it
        try:
            # for compressed downloads the content length refers to the
            # compressed content
            expected_size = int(r.headers.get('content-length'))
        except (ValueError, TypeError):
            # some responses do not have a `content-length` header,
            # even though they HTTP200 and deliver the content.
            # example:
            # https://github.com/datalad/datalad-next/pull/365#issuecomment-1557114109
            expected_size = None
        self._progress_report_start(
            progress_id,
            ('Download %s to %s', from_url, to_path),
            'downloading',
            # can be None, and that is OK
            expected_size,
        )

        fp = None
        props = {}
        try:
            # we can only write to file-likes opened in bytes mode
            fp = sys.stdout.buffer if to_path is None else open(to_path, 'wb')
            # we need to track how much came down the pipe for progress
            # reporting
            downloaded_bytes = 0
            # TODO make chunksize a config item, 65536 is the default in
            # requests_toolbelt
            for chunk in r.raw.stream(amt=65536, decode_content=True):
                # update how much data was transferred from the remote server,
                # but we cannot use the size of the chunk for that,
                # because content might be downloaded with transparent
                # (de)compression. ask the download stream itself for its
                # "position"
                if expected_size:
                    tell = r.raw.tell()
                else:
                    tell = downloaded_bytes + len(chunk)
                self._progress_report_update(
                    progress_id,
                    ('Downloaded chunk',),
                    tell - downloaded_bytes,
                )
                fp.write(chunk)
                downloaded_bytes = tell
                # compute hash simultaneously
                hasher.update(chunk)
            props.update(hasher.get_hexdigest())
            return props
        finally:
            if fp and to_path is not None:
                fp.close()
            self._progress_report_stop(progress_id, ('Finished download',))
