import logging

lgr = logging.getLogger('datalad.utils')


def get_specialremote_param_dict(params):
    """
    Parameters
    ----------
    params : list

    Returns
    -------
    dict
    """
    return dict(p.split('=', maxsplit=1) for p in params)


def get_specialremote_credential_properties(params):
    """Determine properties of credentials special remote configuration

    The input is a parameterization as it would be given to
    `git annex initremote|enableremote <name> ...`, or as stored in
    `remote.log`. These parameters are inspected and a dictionary
    of credential properties, suitable for `CredentialManager.query()`
    is returned. This inspection may involve network activity, e.g.
    HTTP requests.

    Parameters
    ----------
    params : list or dict
      Either a list of strings of the format 'param=value', or a dictionary
      with parameter names as keys.

    Returns
    -------
    dict or None
      Credential property name-value mapping. This mapping can be passed to
      `CredentialManager.query()`. If no credential properties could be
      inferred, for example, because the special remote type is not recognized
      `None` is returned.
    """
    if isinstance(params, (list, tuple)):
        params = get_specialremote_param_dict(params)

    props = {}
    # no other way to do this specifically for each supported remote type
    remote_type = params.get('type')
    if remote_type == 'webdav':
        from datalad_next.http_support import (
            probe_url,
            get_auth_realm,
        )
        url = params.get('url')
        if not url:
            return
        url, urlprops = probe_url(url)
        realm = get_auth_realm(url, urlprops.get('auth'))
        if realm:
            props['realm'] = realm
    else:
        return

    return props or None


def update_specialremote_credential(
        srtype, credman, credname, credprops, credtype_hint=None,
        duplicate_hint=None):
    """
    Parameters
    ----------
    srtype: str
    credman: CredentialManager
    credname: str or Name
    credprops: dict
    """
    if not credname:
        # name could still be None, if this was entered
        # create a default name, and check if it has not been used
        credname = '{type}{udelim}{user}{delim}{realm}'.format(
            type=srtype,
            udelim='-' if 'user' in credprops else '',
            user=credprops.get('user', ''),
            delim='-' if 'realm' in credprops else '',
            realm=credprops.get('realm', ''),
        )
        if credman.get(
                name=credname,
                # give to make legacy credentials accessible
                _type_hint=credtype_hint):
            # this is already in use, do not override
            lgr.warning(
                'The entered credential will not be stored, '
                'a credential with the default name %r already exists.%s',
                credname, f' {duplicate_hint}' if duplicate_hint else '')
            return
    # we have used a credential, store it with updated usage info
    try:
        credman.set(credname, _lastused=True, **credprops)
    except Exception as e:
        # we do not want to crash for any failure to store a
        # credential
        lgr.warn(
            'Exception raised when storing credential %r %r: %s',
            credname, credprops, CapturedException(e),
        )
