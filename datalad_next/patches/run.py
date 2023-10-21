"""Enhance ``run()`` placeholder substitutions to honor configuration defaults

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
import sys

from datalad.core.local.run import (
    GlobbedPaths,
    SequenceFormatter,
    normalize_command,
    quote_cmdlinearg,
)
from datalad.interface.common_cfg import definitions as cfg_defs
from datalad.support.constraints import EnsureStr
from datalad.support.extensions import register_config

from . import apply_patch


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
register_config(
    'datalad.run.substitutions.python',
    'Substitution for {python} placeholder',
    description='Path to a Python interpreter executable',
    type=EnsureStr(),
    default=sys.executable,
    dialog='question',
)
