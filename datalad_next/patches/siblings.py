"""Auto-deploy credentials when enabling special remotes

This is the companion of the ``annexRepo__enable_remote`` patch, and simply
removes the webdav-specific credential handling in ``siblings()``.
It is no longer needed, because credential deployment moved to a lower
layer, covering more special remote types.

Manual credential entry on ``enableremote`` is not implemented here, but easily
possible following the patterns from `datalad-annex::` and
``create_sibling_webdav()``
"""
import logging

from datalad_next.datasets import LegacyAnnexRepo as AnnexRepo
from datalad.support.exceptions import (
    AccessDeniedError,
    AccessFailedError,
    CapturedException,
)
from datalad.support.external_versions import external_versions
from . import apply_patch


# use same logger as -core
lgr = logging.getLogger('datalad.distribution.siblings')


# This function is taken from datalad-core@2ed709613ecde8218a215dcb7d74b4a352825685
# datalad/distribution/siblings.py
# Changes
# - removed credential lookup for webdav-remotes
# - exception logging via CapturedException
# - verification of the enabled special remote, as done by DataLad >= 1.7.3
def _enable_remote(ds, repo, name, res_kwargs, **unused_kwargs):
    result_props = dict(
        action='enable-sibling',
        path=ds.path,
        type='sibling',
        name=name,
        **res_kwargs)

    if not isinstance(repo, AnnexRepo):
        yield dict(
            result_props,
            status='impossible',
            message='cannot enable sibling of non-annex dataset')
        return

    if name is None:
        yield dict(
            result_props,
            status='error',
            message='require `name` of sibling to enable')
        return

    # get info on special remote
    sp_remotes = {
        v['name']: dict(v, uuid=k)
        for k, v in repo.get_special_remotes().items()
    }
    remote_info = sp_remotes.get(name, None)

    if remote_info is None:
        yield dict(
            result_props,
            status='impossible',
            message=("cannot enable sibling '%s', not known", name))
        return

    try:
        repo.enable_remote(name)
        result_props['status'] = 'ok'
    except (AccessDeniedError, AccessFailedError) as e:
        CapturedException(e)
        result_props['status'] = 'error'
        # TODO should use proper way of injecting exceptions in result records
        result_props['message'] = str(e)

    # TODO: remove the version check when datalad >= 1.7.3 is required
    if result_props['status'] == 'ok' \
            and external_versions['datalad'] >= '1.7.3':
        # git-annex >= 10.20261026 (CVE-2026-112818) does not use the special
        # remote until its configuration is verified: ask to confirm it if
        # there is an interactive session, and report how to verify it if not
        from datalad.support.exceptions import RemoteNotVerifiedError
        try:
            if not repo.verify_remote(name):
                result_props['status'] = 'impossible'
                result_props['message'] = (
                    "git-annex does not use sibling '%s', which was not "
                    "verified as safe", name)
        except RemoteNotVerifiedError as e:
            CapturedException(e)
            result_props['status'] = 'impossible'
            result_props['message'] = str(e)

    yield result_props


apply_patch(
    'datalad.distribution.siblings', None, '_enable_remote', _enable_remote)
