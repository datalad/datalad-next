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
from datalad_next.utils.patch import apply_patch


# use same logger as -core
lgr = logging.getLogger('datalad.distribution.siblings')


# This function is taken from datalad-core@2ed709613ecde8218a215dcb7d74b4a352825685
# datalad/distribution/siblings.py
# Changes
# - removed credential lookup for webdav-remotes
# - exception logging via CapturedException
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

    yield result_props


apply_patch(
    'datalad.distribution.siblings', None, '_enable_remote', _enable_remote)
