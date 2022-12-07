"""python-requests-compatible authentication handler using DataLad credentials
"""
# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from typing import Dict
from urllib.parse import urlparse
import requests
import www_authenticate

from datalad_next.utils import CredentialManager
from datalad_next.utils.http_helpers import get_auth_realm

lgr = logging.getLogger('datalad.ext.next.utils.requests_auth')


__all__ = ['DataladAuth', 'HTTPBearerTokenAuth']


class DataladAuth(requests.auth.AuthBase):
    """Requests-style authentication handler using DataLad credentials

    Similar to request_toolbelt's `AuthHandler`, this is a meta
    implementation that can be used with different actual authentication
    schemes. In contrast to `AuthHandler`, a credential can not only be
    specified directly, but credentials can be looked up based on the
    target URL and the server-supported authentication schemes.

    In addition to programmatic specification and automated lookup, manual
    credential entry using interactive prompts is also supported.

    At present, this implementation is not thread-safe.
    """
    _supported_auth_schemes = {
        'basic': 'user_password',
        'digest': 'user_password',
        'bearer': 'token',
    }

    def __init__(self, cfg: CredentialManager, credential: str = None):
        """
        Parameters
        ----------
        cfg: CredentialManager
          Credentials are looked up in this instance.
        credential: str, optional
          Name of a particular credential to be used for any operations.
        """
        self._credman = CredentialManager(cfg)
        self._credential = credential
        self._entered_credential = None

    def save_entered_credential(self, suggested_name: str = None,
                                context: str = None) -> Dict | None:
        """Utility method to save a pending credential in the store

        Pending credentials have been entered manually, and were subsequently
        used successfully for authentication.

        Saving a credential will prompt for entering a name to identify the
        credentials.
        """
        if self._entered_credential is None:
            # nothing to do
            return
        return self._credman.set(
            name=None,
            _lastused=True,
            _suggested_name=suggested_name,
            _context=context,
            **self._entered_credential
        )

    def __call__(self, r):
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
            self._entered_credential = cred
            return ascheme, None, cred
        except Exception as e:
            lgr.debug('Credential retrieval failed: %s', e)
            return ascheme, None, None

    def handle_401(self, r, **kwargs):
        """Callback that received any response to a request

        Any non-4xx response or a response lacking a 'www-authenticate'
        header is ignored.

        Server-provided 'www-authenticated' challenges are inspected, and
        corresponding credentials are looked-up (if needed) and subequently
        tried in a re-request to the original URL after performing any
        necessary actions to meet a given challenge. Such a re-request
        is then using the same connection as the original request.

        Particular challenges are implemented in dedicated classes, e.g.
        :class:`requests.auth.HTTPBasicAuth`.

        Credential look-up or entry is performed by
        :meth:`datalad_next.requests_auth.DataladAuth._get_credential`.
        """
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
        """Callback that received any response to a request

        Any non-redirect response is ignore.

        This callback drops an explicitly set credential whenever
        the redirect causes a non-encrypted connection to be used
        after the original request was encrypted, or when the `netloc`
        of the redirect differs from the original target.
        """
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
        """Helper to rerun a request, but with authentication added"""
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
