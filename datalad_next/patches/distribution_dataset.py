"""``DatasetParameter`` support for ``resolve_path()``

This is the standard result of ``EnsureDataset``, which unlike
the datalad-core version actually carries a ``Dataset`` instance.

This patch ensure the traditional handling of "dataset instance from
a string-type parameter in this context.
"""

import logging

from datalad_next.utils.patch import apply_patch

# use same logger as -core, looks weird but is correct
lgr = logging.getLogger('datalad.dataset')


def resolve_path(path, ds=None, ds_resolved=None):
    if hasattr(ds, 'auto_instance_from_path'):
        # this instance came from datalad-next's EnsureDataset,
        # pretend that we resolved the dataset by hand
        return orig_resolve_path(
            ds=ds.auto_instance_from_path,
            ds_resolved=ds,
        )
    else:
        return orig_resolve_path(ds=ds, ds_resolved=ds_resolved)


# we need to preserve it as the workhorse, this patch only wraps around it
orig_resolve_path = apply_patch(
    'datalad.distribution.dataset', None, 'resolve_path',
    resolve_path,
    msg='Apply datalad-next patch to distribution.dataset:resolve_path')

# re-use docs
resolve_path.__doc__ = orig_resolve_path.__doc__
