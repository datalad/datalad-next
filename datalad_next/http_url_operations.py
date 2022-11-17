"""Handler for operations, such as "download", on http(s):// URLs"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Dict
import requests
from requests_toolbelt import user_agent
from requests_toolbelt.downloadutils.tee import tee as requests_tee
import www_authenticate

import datalad
from datalad.support.exceptions import DownloadError

from datalad_next.requests_auth import DataladAuth
from .url_operations import UrlOperations

lgr = logging.getLogger('datalad.ext.next.http_url_operations')


__all__ = ['HttpUrlOperations']


class HttpUrlOperations(UrlOperations):
    _headers = {
        'user-agent': user_agent('datalad', datalad.__version__),
    }

    def get_headers(self, headers: Dict = None) -> Dict:
        # start with the default
        hdrs = dict(HttpUrlOperations._headers)
        if headers is not None:
            hdrs.update(headers)
        return hdrs

    def download(self,
                 from_url: str,
                 to_path: Path | None,
                 credential: str = None,
                 hash: str = None) -> Dict:
        # a new manager per request
        # TODO optimize later to cache credentials per target
        # similar to requests_toolbelt.auth.handler.AuthHandler
        auth = DataladAuth(self._cfg, credential=credential)
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
                raise DownloadError(
                    msg=str(e), status=e.response.status_code) from e

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
            self, r, to_path, hash: list[str] = None) -> Dict:
        from_url = r.url
        hasher = self._get_hasher(hash)
        progress_id = self._get_progress_id(from_url, to_path)
        # get download size, but not every server provides it
        try:
            expected_size = int(r.headers.get('content-length'))
        except (ValueError, TypeError):
            expected_size = None
        self._progress_report_start(
            progress_id, from_url, to_path, expected_size)

        fp = None
        props = {}
        try:
            fp = sys.stdout.buffer if to_path is None else open(to_path, 'wb')
            # TODO make chunksize a config item
            for chunk in requests_tee(r, fp):
                self._progress_report_update(progress_id, len(chunk))
                # compute hash simultaneously
                for h in hasher:
                    h.update(chunk)
            props.update(self._get_hash_report(hash, hasher))
            return props
        finally:
            if fp and to_path is not None:
                fp.close()
            self._progress_report_stop(progress_id)
