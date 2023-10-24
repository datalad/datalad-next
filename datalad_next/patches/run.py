"""Enhance datalad-core's ``run()``

Portable path handling logic for run-records
--------------------------------------------

Placeholder substitutions to honor configuration defaults
---------------------------------------------------------

Previously, ``run()`` would not recognize configuration defaults for
placeholder substitution. This means that any placeholders globally declared in
``datalad.interface.common_cfg``, or via ``register_config()`` in DataLad
extensions would not be effective.

This patch makes run's ``format_command()`` helper include such defaults
explicitly, and thereby enable the global declaration of substitution defaults.

Moreoever a ``{python}`` placeholder is now defined via this mechanism, and
points to the value of ``sys.executable`` by default. This particular
placeholder was found to be valuable for improving the portability of
run-recording across (specific) Python versions, or across different (virtual)
environments. See https://github.com/datalad/datalad-container/issues/224 for
an example use case.

https://github.com/datalad/datalad/pull/7509
"""

from itertools import filterfalse
from os.path import lexists
from pathlib import (
    PurePath,
    PureWindowsPath,
    PurePosixPath,
)
import sys

from datalad.core.local.run import (
    GlobbedPaths,
    SequenceFormatter,
    normalize_command,
    quote_cmdlinearg,
    _create_record as _orig_create_record,
)
from datalad.distribution.dataset import Dataset
from datalad.local.rerun import get_run_info as _orig_get_run_info
from datalad.interface.common_cfg import definitions as cfg_defs
from datalad.support.constraints import EnsureStr
from datalad.support.extensions import register_config

from . import apply_patch


# Deals with https://github.com/datalad/datalad/issues/7512
def _create_record(run_info, sidecar_flag, ds):
    # convert any input/output specification to a POSIX path
    for k in ('inputs', 'outputs'):
        if k not in run_info:
            continue
        run_info[k] = [_get_posix_relpath_for_runrecord(p)
                       for p in run_info[k]]

    return _orig_create_record(run_info, sidecar_flag, ds)


def _get_posix_relpath_for_runrecord(path):
    p = PurePath(path)
    if p.is_absolute():
        # there is no point in converting an absolute path
        # to a different platform convention.
        # return as-is
        return path

    return str(PurePosixPath(p))


# Deals with https://github.com/datalad/datalad/issues/7512
def get_run_info(dset, message):
    msg, run_info = _orig_get_run_info(dset, message)
    if run_info is None:
        # nothing to process, return as-is
        return msg, run_info

    for k in ('inputs', 'outputs'):
        if k not in run_info:
            continue
        run_info[k] = [_get_platform_path_from_runrecord(p, dset)
                       for p in run_info[k]]
    return msg, run_info


def _get_platform_path_from_runrecord(path: str, ds: Dataset) -> PurePath:
    """Helper to standardize run_info path handling

    Previously, run-records would contain platform-paths (e.g., windows paths
    when added on windows, POSIX paths elsewhere). This made cross-platform
    rerun impossible out-of-the box, but it also means that such dataset are
    out there in unknown numbers.

    This helper inspects any input/output path reported by get_run_info()
    and tries to ensure that it matches platform conventions.

    Parameters
    ----------
    path: str
      A str-path from an input/output specification
    ds: Dataset
      This dataset's base path is used for existence testing for
      convention determination.

    Returns
    -------
    str
    """
    # we only need to act differently, when an incoming path is
    # windows. This is not possible to say with 100% confidence,
    # because a POSIX path can also contain a backslash. We support
    # a few standard cases where we CAN tell
    try:
        pathobj = None
        if '\\' not in path:
            # no windows pathsep, no problem
            pathobj = PurePosixPath(path)
        # let's assume it is windows for a moment
        elif lexists(str(ds.pathobj / PureWindowsPath(path))):
            # if there is something on the filesystem for this path,
            # we can be reasonably sure that this is indeed a windows
            # path. This won't catch everything, but better than nothing
            pathobj = PureWindowsPath(path)
        else:
            # if we get here, we have no idea, and no means to verify
            # further hypotheses -- go with the POSIX assumption
            # and hope for the best
            pathobj = PurePosixPath(path)
        assert pathobj is not None
    except Exception:
        return path

    # we report in platform-conventions
    return str(PurePath(pathobj))


# This function is taken from datalad-core@a96c51c0b2794b2a2b4432ec7bd51f260cb91a37
# datalad/core/local/run.py
# The change has been proposed in https://github.com/datalad/datalad/pull/7509
def format_command(dset, command, **kwds):
    """Plug in placeholders in `command`.

    Parameters
    ----------
    dset : Dataset
    command : str or list

    `kwds` is passed to the `format` call. `inputs` and `outputs` are converted
    to GlobbedPaths if necessary.

    Returns
    -------
    formatted command (str)
    """
    command = normalize_command(command)
    sfmt = SequenceFormatter()
    cprefix = 'datalad.run.substitutions.'

    def not_subst(x):
        return not x.startswith(cprefix)

    for k in set(filterfalse(not_subst, cfg_defs.keys())).union(
            filterfalse(not_subst, dset.config.keys())):
        v = dset.config.get(
            k,
            # pull a default from the config definitions
            # if we have no value, but a key
            cfg_defs.get(k, {}).get('default', None))
        sub_key = k.replace(cprefix, "")
        if sub_key not in kwds:
            kwds[sub_key] = v

    for name in ["inputs", "outputs"]:
        io_val = kwds.pop(name, None)
        if not isinstance(io_val, GlobbedPaths):
            io_val = GlobbedPaths(io_val, pwd=kwds.get("pwd"))
        kwds[name] = list(map(quote_cmdlinearg, io_val.expand(dot=False)))
    return sfmt.format(command, **kwds)


apply_patch(
    'datalad.core.local.run', None, 'format_command', format_command)
apply_patch(
    'datalad.core.local.run', None, '_create_record', _create_record)
apply_patch(
    'datalad.local.rerun', None, 'get_run_info', get_run_info)

register_config(
    'datalad.run.substitutions.python',
    'Substitution for {python} placeholder',
    description='Path to a Python interpreter executable',
    type=EnsureStr(),
    default=sys.executable,
    dialog='question',
)
