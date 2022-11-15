# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from pathlib import Path
import sys
from typing import Dict
from urllib.parse import urlparse
import requests
from requests_toolbelt import user_agent
from requests_toolbelt.downloadutils.tee import tee as requests_tee
import www_authenticate

import datalad

from datalad_next.credman import CredentialManager

lgr = logging.getLogger('datalad.ext.next.http_support')


__all__ = ['probe_url', 'get_auth_realm', 'HttpOperations']


def probe_url(url, timeout=10.0, headers=None):
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
    # retire in favor of user_agent()
    from datalad.downloaders.http import DEFAULT_USER_AGENT
    hdrs = {'user-agent': DEFAULT_USER_AGENT}
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


def get_auth_realm(url, auth_info, scheme=None):
    """Determine an authentication realm identifier from a HTTP response.

    Examples
    --------
    Robustly determine a realm identifier for any URL::

       >>> url, props = probe_url('https://fz-juelich.sciebo.de/...')
       >>> get_auth_realm(url, props.get('auth'))
       'https://fz-juelich.sciebo.de/login'

    Parameters
    ----------
    url: str
      A URL as returned by `probe_url()`
    auth_info: dict
      A mapping of supported authentication schemes to the properties
      (i.e., a 'www-authenticate' response header), as returned by
      `probe_url()`'s 'auth' property.
    scheme: str, optional
      Which specific authentication to report a realm for, in case
      multiple are supported (such as 'basic', or 'token').
      If not given, the first (if any) reported authentication
      scheme is reported on.

    Returns
    -------
    str
      A server-specific realm identifier
    """
    if not auth_info:
        # no info from the server on what it needs
        # out best bet is the URL itself
        return url
    if scheme:
        auth_info = auth_info[scheme]
    else:
        scheme, auth_info = auth_info.copy().popitem()
    # take any, but be satisfied with none too
    realm = auth_info.get('realm') if auth_info else ''
    # a realm is supposed to indicate a validity scope of a credential
    # on a server. so we make sure to have the return realm identifier
    # actually indicate a server too, in order to make it suitable for
    # a global credential lookup
    if _is_valid_url(realm):
        # the realm is already a valid URL with a server specification.
        # we can simply relay it as such, following the admins' judgement
        return realm
    else:
        # the realm was just some kind of string. we prefix it with the
        # netloc of the given URL (ignoring its path) to achieve
        # the same server-specific realm semantics
        parsed = urlparse(url)
        return '{scheme}://{netloc}{slash}{realm}'.format(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            slash='' if realm.startswith('/') else'/',
            realm=realm,
        )


def _is_valid_url(url):
    try:
        parsed = urlparse(url)
        return all([parsed.scheme, parsed.netloc])
    except:
        return False


class HttpOperations:
    _headers = {
        'user-agent': user_agent('datalad', datalad.__version__),
    }

    def __init__(self, cfg):
        self._cfg = cfg

    def get_headers(self, headers: Dict = None) -> Dict:
        # start with the default
        hdrs = dict(HttpOperations._headers)
        if headers is not None:
            hdrs.update(headers)
        return hdrs

    def download(self, from_url: str, to_path: Path | None, credential: str = None):
        with requests.get(
                from_url,
                stream=True,
                headers=self.get_headers(),
                # a new manager per request
                # TODO optimize later to cache credentials per target
                # similar to requests_toolbelt.auth.handler.AuthHandler
                auth=DataladAuth(self._cfg, credential=credential),
        ) as r:
            r.raise_for_status()
            self._stream_download_from_request(r, to_path)

    def _stream_download_from_request(self, r, to_path):
        # TODO wrap in progress report
        fp = None
        try:
            fp = sys.stdout.buffer if to_path is None else open(to_path, 'wb')
            # TODO make chunksize a config item
            for chunk in requests_tee(r, fp):
                # TODO compute hash simultaneously
                pass
        finally:
            if fp and to_path is not None:
                fp.close()


class DataladAuth(requests.auth.AuthBase):
    _supported_auth_schemes = {
        'basic': 'user_password',
        'digest': 'user_password',
        'bearer': 'token',
    }

    def __init__(self, cfg: CredentialManager, credential: str = None):
        self._credman = CredentialManager(cfg)
        self._credential = credential

    def __call__(self, r):
        # TODO when using/reusing a credential, disable follow redirect
        # to prevent inappropriate leakage to other servers
        # TODO support being called from multiple threads
        #self.init_per_thread_state()

        # register hooks to be executed from a response to this
        # request is available
        # Redirect: reset credentials to avoid leakage to other server
        r.register_hook("response", self.handle_redirect)
        # 401 Unauthorized: look for a credential and try again
        r.register_hook("response", self.handle_401)
        return r

    def _get_credential(self, url, auth_schemes
                        ) -> tuple[str | None, str | None, Dict | None]:
        """Get a credential for access to `url` given server-supported schemes

        If a particular credential to use was given to the `DataladAuth`
        constructor it reported here.

        In all other situations a credential will be looked up, based on
        the access URL and the authentication schemes supported by the
        host. The authentication schemes will be tested in the order in
        which they are reported by the remote host.

        If no matching credential can be identified, a prompt to enter
        a credential is presented. The credential type will match, and be
        used with the first authentication scheme that is both reported
        by the host, and by this implementation.

        The methods returns a 3-tuple. The first element is an identifier
        for the authentication scheme ('basic', digest', etc.) to use
        with the credential. The second item is the name for the reported
        credential, and the third element is a dictionary with the
        credential properties and its secret. Any of the three items can be
        `None` if the respective information is not available.
        """
        if self._credential:
            cred = self._credman.get(name=self._credential)
            # this credential is scheme independent
            return None, self._credential, cred

        # no credential identified, find one
        for ascheme in auth_schemes:
            if ascheme not in DataladAuth._supported_auth_schemes:
                # nothing we can handle
                continue
            ctype = DataladAuth._supported_auth_schemes[ascheme]
            # get a realm ID for this authentication scheme
            realm = get_auth_realm(url, auth_schemes, scheme=ascheme)
            # ask for matching credentials
            creds = self._credman.query(
                _sortby='last-used',
                type=ctype,
                realm=realm,
            )
            if creds:
                # we have matches, go with the last used one
                name, cred = creds[0]
                return ascheme, name, cred

        # no success finding an existing credential, now ask, if possible
        # pick a scheme that is supported by the server and by us
        ascheme = [s for s in auth_schemes
                   if s in DataladAuth._supported_auth_schemes]
        if not ascheme:
            # f-string OK, only executed on failure
            lgr.debug(
                'Only unsupported HTTP auth schemes offered '
                f'{list(auth_schemes.keys())!r}')
        # go with the first supported scheme
        ascheme = ascheme[0]
        ctype = DataladAuth._supported_auth_schemes[ascheme]

        try:
            cred = self._credman.get(
                name=None,
                _prompt=f'Credential needed for access to {url}',
                _type_hint=ctype,
                type=ctype,
                # include the realm in the credential to avoid asking for it
                # interactively (it is a server-specified property
                # users would generally not know, if they do, they can use the
                # `credentials` command upfront.
                realm=get_auth_realm(url, auth_schemes)
            )
            return ascheme, None, cred
        except Exception as e:
            lgr.debug('Credential retrieval failed: %s', e)
            return ascheme, None, None

    def handle_401(self, r, **kwargs):
        if not 400 <= r.status_code < 500:
            # fast return if this is no error, see
            # https://github.com/psf/requests/issues/3772 for background
            return r
        if 'www-authenticate' not in r.headers:
            # no info on how to authenticate to react to, leave it as-is.
            # this also catches any non-401-like error code (e.g. 429).
            # doing this more loose check (rather then going for 401
            # specifically) enables to support services that send
            # www-authenticate with e.g. 403s
            return r
        # which auth schemes does the server support?
        auth_schemes = www_authenticate.parse(r.headers['www-authenticate'])
        ascheme, credname, cred = self._get_credential(r.url, auth_schemes)

        if cred is None or 'secret' not in cred:
            # we got nothing, leave things as they are
            return r

        # TODO add safety check. if a credential somehow contains
        # information on its scope (i.e. only for github.com)
        # prevent its use for other hosts -- maybe unless given explicitly.

        if ascheme is None:
            # if there is no authentication scheme identified, look at the
            # credential, if it knows
            ascheme = cred.get('http_auth_scheme')
            # if it does not, go with the first supported scheme that matches
            # the credential type, one is guaranteed to match
            ascheme = [
                c for c in auth_schemes
                if c in DataladAuth._supported_auth_schemes
                and cred.get('type') == DataladAuth._supported_auth_schemes[c]
            ][0]

        if ascheme == 'basic':
            return self._authenticated_rerequest(
                r,
                requests.auth.HTTPBasicAuth(cred['user'], cred['secret']),
                **kwargs)
        elif ascheme == 'digest':
            return self._authenticated_rerequest(
                r,
                requests.auth.HTTPDigestAuth(cred['user'], cred['secret']),
                **kwargs)
        elif ascheme == 'bearer':
            return self._authenticated_rerequest(
                r, HTTPBearerTokenAuth(cred['secret']), **kwargs)
        else:
            raise NotImplementedError(
                'Only unsupported HTTP auth schemes offered '
                f'{list(auth_schemes.keys())!r} need {ascheme!r}')

    def handle_redirect(self, r, **kwargs):
        if r.is_redirect and self._credential:
            og_p = urlparse(r.url)
            rd_p = urlparse(r.headers.get('location'), '')
            if og_p.netloc != rd_p.netloc or (
                    rd_p.scheme == 'http' and og_p.scheme == 'https'):
                lgr.debug(
                    'URL redirect, discarded given credential %r '
                    'to avoid leakage',
                    self._credential)
                self._credential = None

    def _authenticated_rerequest(
            self,
            response: requests.models.Response,
            auth: requests.auth.AuthBase,
            **kwargs
    ) -> requests.models.Response:
        """Helper to rerun a request, but with basic auth added"""
        prep = _get_renewed_request(response)
        auth(prep)
        _r = response.connection.send(prep, **kwargs)
        _r.history.append(response)
        _r.request = prep
        return _r


def _get_renewed_request(r: requests.models.Response
                         ) -> requests.models.PreparedRequest:
    """Helper. Logic taken from requests.auth.HTTPDigestAuth"""
    # Consume content and release the original connection
    # to allow our new request to reuse the same one.
    r.content
    r.close()
    prep = r.request.copy()
    requests.cookies.extract_cookies_to_jar(
        prep._cookies, r.request, r.raw)
    prep.prepare_cookies(prep._cookies)
    return prep


class HTTPBearerTokenAuth(requests.auth.AuthBase):
    """Attaches HTTP Bearer Token Authentication to the given Request object.
    """
    def __init__(self, token):
        super().__init__()
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f'Bearer {self.token}'
        return r
