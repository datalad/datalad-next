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
    realm = auth_info.get('realm', '')
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

    def download(self, from_url: str, to_path: Path):
        with requests.get(
                from_url,
                stream=True,
                headers=self.get_headers(),
                # a new manager per request
                # TODO optimize later to cache credentials per target
                # similar to requests_toolbelt.auth.handler.AuthHandler
                auth=DataladAuth(self._cfg),
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
    def __init__(self, cfg):
        self._credman = CredentialManager(cfg)

    def __call__(self, r):
        # TODO when using/reusing a credential, disable follow redirect
        # to prevent inappropriate leakage to other servers
        # TODO support being called from multiple threads
        #self.init_per_thread_state()

        # register hooks to be executed from a response to this
        # request is available
        # 401 Unauthorized: look for a credential and try again
        r.register_hook("response", self.handle_401)
        # Redirect: reset credentials to avoid leakage to other server
        r.register_hook("response", self.handle_redirect)
        return r

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
        # determine realm for credential lookup
        # TODO make conditional when a credential is already known
        realm = get_auth_realm(
            # we continue with the effective URL reported in the
            # response, i.e. after following all redirects. We use this
            # for credential lookup and reporting to avoid
            # misrepresenting the credential target to a user
            # (site could maliciously redirect to an entirely different
            # domain)
            r.url,
            auth_schemes,
            # we will get the realm for the first item in `auth_schemes`
            scheme=None,
        )
        # TODO make conditional when a credential is already known
        credname, cred = get_url_credential(
            # TODO support explicit name
            name=None,
            credman=self._credman,
            # TODO say something about auth-type?
            prompt=f'Credential needed for access to {r.url}',
            # use the real for lookup
            # TODO look for a matching auth_scheme property too
            query_props=dict(realm=realm),
            # TODO support something else than user/pass
            #prompt_credential_type='user_password',
        )
        if cred is None:
            # we got nothing, leave things as they are
            return r

        # TODO check what auth-scheme the credential can do and
        # select a matching one. If credential doesn't say, go
        # with first/any/all-one-by-one?
        # TODO check by credential type? Token for basic?
        if 'basic' in auth_schemes:
            return self._basic_auth_rerequest(
                r,
                cred['user'],
                cred['secret'],
                **kwargs)
        else:
            raise NotImplementedError('Unsupported HTTP auth scheme')

    def handle_redirect(self, r, **kwargs):
        if r.is_redirect:
            # TODO reset current credential
            pass

    def _basic_auth_rerequest(
            self,
            response: requests.models.Response,
            username: str,
            password: str,
            **kwargs
    ) -> requests.models.Response:
        """Helper to rerun a request, but with basic auth added"""
        auth = requests.auth.HTTPBasicAuth(username, password)
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


def get_url_credential(
    name: str | None,
    credman: CredentialManager,
    # Should say what kind of credential and what for
    prompt: str,
    # used to query for a credential, even when a name is given
    query_props: Dict = None,
    # type hint for credential manager to ask for the right
    # components
    prompt_credential_type: str = 'user_password',
) -> tuple[str | None, Dict | None]:

    cred = None
    if query_props:
        creds = credman.query(_sortby='last-used', **query_props)
        # TODO when a name is given, pick the one that matches the name
        # (if any does), when multiple results are returned
        if creds:
            name, cred = creds[0]

    if not cred:
        kwargs = dict(
            # name could be none
            name=name,
            _prompt=prompt,
            type=prompt_credential_type,
        )
        # check if we know the realm, if so include in the credential, if not
        # avoid asking for it interactively (it is a server-specified property
        # users would generally not know, if they do, they can use the
        # `credentials` command upfront.
        realm = query_props.get('realm')
        if realm:
            kwargs['realm'] = realm
        try:
            cred = credman.get(**kwargs)
        except Exception as e:
            lgr.debug('Credential retrieval failed: %s', e)

    return name, cred
