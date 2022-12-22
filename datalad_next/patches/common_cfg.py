"""Change the default of ``datalad.annex.retry`` to ``1``

This prevents unconditional retries, and thereby improves the legibility
of errors (now only one error instead of three identical errors).

This change does not override user-settings, only the default.
"""

from datalad.support.extensions import has_config

if has_config('datalad.annex.retry'):
    from datalad.interface.common_cfg import definitions
    retrycfg = definitions['datalad.annex.retry']
    retrycfg['default'] = 1
