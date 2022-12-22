"""Improved credential handling for ``create_sibling_<github-like>()``

This patch makes the storage of a newly entered credential conditional on a
successful authorization, in the spirit of `datalad/datalad#3126
<https://github.com/datalad/datalad/issues/3126>`__.

Moreover, stored credentials now contain a ``realm`` property that
identified the API endpoint. This makes it possible to identify
candidates of suitable credentials without having to specific
their name, similar to a request context url used by the old
providers setup.

This automatic realm-based credential lookup is now also implemented.
When no credential name is specified, the most recently used
credential matching the API realm will be used automatically.
If determined like this, it will be tested for successfull
authorization, and will then be stored again with an updated
``last-used`` timestamp.
"""

import logging
from urllib.parse import urlparse

from datalad.distributed.create_sibling_ghlike import _GitHubLike
from datalad.downloaders.http import DEFAULT_USER_AGENT
from datalad_next.exceptions import CapturedException
from datalad_next.utils import CredentialManager

# use same logger as -core
lgr = logging.getLogger('datalad.distributed.create_sibling_ghlike')


def _set_request_headers(self, credential_name, auth_info, require_token):
    credman = CredentialManager()
    from_query = False
    credential = None
    if not credential_name:
        # get the most recent credential by realm, because none was identified
        creds = credman.query(realm=self.api_url, _sortby='last-used')
        if creds:
            # found one, also assign the name to be able to update
            # it below
            credential_name, credential = creds[0]
            from_query = True
    if not credential_name:
        # if we have no name given, fall back on a generated one
        # that may exist from times before realms were recorded
        # properly, otherwise we would not be here
        credential_name = urlparse(self.api_url).netloc
    if not credential:
        # no credential yet
        try:
            credential = credman.get(
                credential_name,
                _prompt=auth_info,
                type='token',
                realm=self.api_url,
            )
            if credential is None or 'secret' not in credential:
                raise ValueError('No credential found')
        except Exception as e:
            CapturedException(e)
            lgr.debug('Token retrieval failed: %s', e)
            lgr.warning(
                'Cannot determine authorization token for %s', credential_name)
            if require_token:
                raise ValueError(
                    f'Authorization required for {self.fullname}, '
                    f'cannot find token for a credential {credential_name}.')
            else:
                lgr.warning("No token found for credential '%s'",
                            credential_name)
            credential = {}

    self.request_headers = {
        'user-agent': DEFAULT_USER_AGENT,
        'authorization':
        f'token {credential.get("secret", "NO-TOKEN-AVAILABLE")}',
    }

    if from_query or credential.pop('_edited', None):
        # if the credential was determined based on the api realm or edited,
        # test it so we know it (still) works before we save/update it
        try:
            self.authenticated_user
        except Exception as e:
            raise ValueError(
                f"Credential {credential_name!r} did not yield successful "
                "authorization") from e
        # this went well, store
        try:
            credman.set(
                credential_name,
                _lastused=True,
                **credential,
            )
        except Exception as e:
            # we do not want to crash for any failure to store a
            # credential
            lgr.warn(
                'Exception raised when storing credential %r %r: %s',
                credential_name,
                credential,
                CapturedException(e),
            )

# patch the core class
lgr.debug('Apply datalad-next patch to create_sibling_ghlike.py:_GitHubLike._set_request_headers')
_GitHubLike._set_request_headers = _set_request_headers

# update docs
_GitHubLike.create_sibling_params['credential']._doc = """\
name of the credential providing a personal access token
to be used for authorization. The token can be supplied via
configuration setting 'datalad.credential.<name>.secret', or
environment variable DATALAD_CREDENTIAL_<NAME>_SECRET, or will
be queried from the active credential store using the provided
name. If none is provided, the last-used token for the
API URL realm will be used. If no matching credential exists,
a credential named after the hostname part of the API URL is tried
as a last fallback."""

