"""python-requests-compatible authentication handler using DataLad credentials
"""
# allow for |-type UnionType declarations
from __future__ import annotations

from functools import partial
import logging
from typing import Dict
from urllib.parse import urlparse
import requests
import www_authenticate

from datalad_next.config import ConfigManager
from datalad_next.credman import (
    CredentialManager,
    InvalidCredential,
)
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

    def __init__(self, cfg: ConfigManager, credential: str | None = None):
        """
        Parameters
        ----------
        cfg: ConfigManager
          Is passed to CredentialManager() as `cfg`-parameter.
        credential: str, optional
          Name of a particular credential to be used for any operations.
        """
        self._credman = CredentialManager(cfg)
        self._credential = credential

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

        Raises
        ------
        NoSuitableCredentialAvailable
          If no credential for the target URL could be determined/entered,
          or none of the determined/entered credentials led to successful
          authentication
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

        # assemble specification(s) to query for matching credentials,
        # one for each authentication scheme supported
        query_specs = [
            dict(
                # credential type
                type=self._supported_auth_schemes[ascheme],
                # a realm identifier for the specific scheme
                realm=get_auth_realm(r.url, auth_schemes, scheme=ascheme),
            )
            for ascheme in auth_schemes
            # ignore any scheme that we cannot handle anyways
            if ascheme in DataladAuth._supported_auth_schemes
        ]

        # a general realm identifier for prompting
        prompt_realm = get_auth_realm(r.url, auth_schemes)
        if prompt_realm.startswith(r.url):
            # remove redundancies in prompt when the realm identifier
            # starts with the access URL
            prompt_realm = prompt_realm[len(r.url):]

        return self._credman.call_with_credential(
            partial(
                self._authenticated_rerequest,
                response=r,
                supported_auth_schemes=auth_schemes,
                **kwargs,
            ),
            purpose=f'access to {r.url!r}',
            # pass any credential name given to the `DataladAuth` constructor
            name=self._credential,
            # provide a prompt, in case no credential is on record
            prompt=f'Credential needed for accessing {r.url} '
            f'(authentication realm {prompt_realm!r})',
            type_hint=self._get_preferred_credential_type(auth_schemes),
            # specification which credentials to query for,
            # only in effect when `name` is not None
            query_props=query_specs,
        )

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
            cred: Dict,
            *,
            supported_auth_schemes,
            response: requests.models.Response,
            **kwargs
    ) -> requests.models.Response:
        """Helper to rerun a request, but with authentication added"""
        # TODO add safety check. if a credential somehow contains
        # information on its scope (i.e. only for github.com)
        # prevent its use for other hosts -- maybe unless given explicitly.

        # we need to decide on an authentication to use (first?)
        # look at the credential, if it knows
        ascheme = cred.get('http_auth_scheme')
        if ascheme is not None and ascheme not in supported_auth_schemes:
            # reject, if the recorded scheme is not actually supported
            ascheme = None
        if ascheme is None:
            # if we have no scheme yet, go with the first supported scheme
            # that matches the credential type
            possible_aschemes = [
                c for c in supported_auth_schemes
                if c in DataladAuth._supported_auth_schemes and cred.get(
                    'type') == DataladAuth._supported_auth_schemes[c]
            ]
            ascheme = possible_aschemes[0] if possible_aschemes else None

        if ascheme == 'basic':
            auth = requests.auth.HTTPBasicAuth(cred['user'], cred['secret'])
            _r = self._handle_401_rerequest(response, auth, **kwargs)
        elif ascheme == 'digest':
            # we do something special here. DigestAuth needs a challenge
            # response. The challenge we already got, so let's init
            # auth with the original request (that already failed), and
            # then pass it the outcome immediately. It's implementation
            # we issue the challenge-response on its own, and we can
            # continue inspecting that outcome down below
            auth = requests.auth.HTTPDigestAuth(cred['user'], cred['secret'])
            auth(response.request)
            _r = auth.handle_401(response, **kwargs)
        elif ascheme == 'bearer':
            auth = HTTPBearerTokenAuth(cred['secret'])
            _r = self._handle_401_rerequest(response, auth, **kwargs)
        else:
            raise NotImplementedError(
                'Only unsupported HTTP auth schemes offered '
                f'{list(supported_auth_schemes.keys())!r}')

        # now inspect the outcome and fish out anything that looks like
        # failure due to an inadequate/insufficient/invalid credential
        if (
            # the focused 'unauthorized'
            _r.status_code == 401
            # but pragmatically anything error-ish that has auth-info in the
            # response
            or 400 <= _r.status_code < 500 and 'www-authenticate' in _r.headers
        ):
            raise InvalidCredential(
                f"HTTP{_r.status_code} {_r.reason}",
                _r.url,
                cred,
            )
        _r.history.append(response)
        return _r

    def _handle_401_rerequest(self, response, auth, **kwargs):
        # clone the previous request to renew it for another run ...
        prep = _get_renewed_request(response)
        # ... and equip with the auth info we want to provision (this time)
        auth(prep)
        # make the request
        _r = response.connection.send(prep, **kwargs)
        return _r

    def _get_preferred_credential_type(self, auth_schemes):
        # translate authentication schemes to credential types
        ctypes = set(
            self._supported_auth_schemes[s]
            for s in auth_schemes.keys()
            if s in self._supported_auth_schemes
        )
        if not ctypes:
            # protect against unsupported request
            return None

        # prefer a token, go with whatever we have otherwise
        return 'token' if 'token' in ctypes else ctypes.pop()


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
