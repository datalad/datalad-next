"""Credential support for ``AnnexRepo.enable_remote()`` and ``siblings enable``

Supported targets for automatic credential deployments are determined
by ``needs_specialremote_credential_envpatch()``. At the time of this
writing this includes the git-annex built-in remote types ``webdav``,
``s3``, and ``glacier``.

This patch also changes the function to raise its custom exception
with the context of an original underlying exception for better
error reporting.
"""
import logging
import os
import re
from unittest.mock import patch

from datalad.support.exceptions import (
    AccessDeniedError,
    AccessFailedError,
)
from datalad_next.exceptions import (
    CommandError,
)
from datalad_next.utils import (
    CredentialManager,
    ensure_list,
    get_specialremote_credential_envpatch,
    get_specialremote_param_dict,
    get_specialremote_credential_properties,
    needs_specialremote_credential_envpatch,
)
from datalad_next.utils.patch import apply_patch


# reuse logger from -core, despite the unconventional name
lgr = logging.getLogger('datalad.annex')


# This function is taken from datalad-core@2ed709613ecde8218a215dcb7d74b4a352825685
# datalad/support/annexrepo.py:AnnexRepo
def annexRepo__enable_remote(self, name, options=None, env=None):
    """Enables use of an existing special remote

    Parameters
    ----------
    name: str
        name, the special remote was created with
    options: list, optional
    """
    # MIH thinks there should be no `env` argument at all
    # https://github.com/datalad/datalad/issues/5162
    # if it would not be there, this whole dance is pretty much
    # obsolete
    env = env or self._git_runner.env

    # an enableremote can do pretty much anything, including a type change.
    # in order to be able to determine whether credentials *will* be needed,
    # we have to look ahead and form the special remote parameters that will
    # be there at the end -- more or less

    # pull info for present config
    sp_remotes = {v['name']: dict(v, uuid=k) for k, v in self.get_special_remotes().items()}
    remote_info = sp_remotes.get(name, {})
    # TODO if remote_info is empty, we can fail right here
    if options:
        # and now update with given params
        remote_info.update(get_specialremote_param_dict(options))

    # careful here, `siblings()` also calls this for regular remotes, check
    # for a known type
    if 'type' in remote_info \
            and needs_specialremote_credential_envpatch(remote_info['type']):
        # see if we can identify any matching credentials
        credprops = get_specialremote_credential_properties(remote_info)
        credman = None
        credspec = None
        if credprops:
            credman = CredentialManager(self.config)
            creds = credman.query(_sortby='last-used', **credprops)
            if creds:
                # found one
                credspec = creds[0]
        # TODO manual entry could be supported here too! (also see at the end)
        if env:
            env.copy()

        if credspec:
            credpatch = get_specialremote_credential_envpatch(
                remote_info['type'], credspec[1])
            if credpatch:
                if not env:
                    env = os.environ.copy()
                env.update(credpatch)

    try:
        with patch.object(self._git_runner, 'env', env):
            # TODO: outputs are nohow used/displayed. Eventually convert to
            # to a generator style yielding our "dict records"
            self.call_annex(['enableremote', name] + ensure_list(options))
    except CommandError as e:
        if re.match(r'.*StatusCodeException.*statusCode = 401', e.stderr):
            raise AccessDeniedError(e.stderr) from e
        elif 'FailedConnectionException' in e.stderr:
            raise AccessFailedError(e.stderr) from e
        else:
            raise e
    self.config.reload()
    # TODO when manual credential entry is supported,
    # implement store-after-success here


apply_patch(
    'datalad.support.annexrepo', 'AnnexRepo', 'enable_remote',
    annexRepo__enable_remote,
    msg='Apply datalad-next patch to annexrepo.py:AnnexRepo.enable_remote')
