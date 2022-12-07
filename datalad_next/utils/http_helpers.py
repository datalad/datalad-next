"""Small helpers for HTTP operations
"""
# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from urllib.parse import urlparse


lgr = logging.getLogger('datalad.ext.next.utils.http_helpers')


__all__ = ['get_auth_realm']


def get_auth_realm(url, auth_info, scheme=None):
    """Determine an authentication realm identifier from a HTTP response.

    Examples
    --------
    Robustly determine a realm identifier for any URL::

       > url, props = HttpUrlOperations().probe_url(
             'https://fz-juelich.sciebo.de/...')
       > get_auth_realm(url, props.get('auth'))
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
