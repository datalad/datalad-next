import logging

from datalad.distribution import siblings as mod_siblings
from datalad.support.annexrepo import AnnexRepo
from datalad.support.exceptions import (
    AccessDeniedError,
    AccessFailedError,
    CapturedException,
)

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
    sp_remotes = {v['name']: dict(v, uuid=k) for k, v in repo.get_special_remotes().items()}
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


# apply patch
lgr.debug('Apply datalad-next patch to siblings.py:_enable_remote')
mod_siblings._enable_remote = _enable_remote
