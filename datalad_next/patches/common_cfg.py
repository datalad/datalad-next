from datalad.support.extensions import has_config

if has_config('datalad.annex.retry'):
    from datalad.interface.common_cfg import definitions
    retrycfg = definitions['datalad.annex.retry']
    retrycfg['default'] = 1
