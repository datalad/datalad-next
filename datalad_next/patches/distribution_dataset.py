import logging
from datalad.distribution import dataset as mod_distribution_dataset

# use same logger as -core, looks weird but is correct
lgr = logging.getLogger('datalad.dataset')

# we need to preserve it as the workhorse, this patch only wraps around it
orig_resolve_path = mod_distribution_dataset.resolve_path


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


# re-use docs
resolve_path.__doc__ = orig_resolve_path.__doc__


# apply patch
lgr.debug('Apply datalad-next patch to distribution.dataset:resolve_path')
mod_distribution_dataset.resolve_path = resolve_path
