"""Make `register_config()/has_config()` use `ImplementationDefault` instance

The original implementation use a structure from
`datalad.interface.common_cfg`.  The `defaults` instance of
`ImplementationDefault` from `datalad_next.config` also contains this
information, and consolidates it into a new structure and API. This patch
ensures that extensions registering their configuration items using this legacy
API, also feed this `defaults` instance.
"""

from datalad_next.patches import apply_patch

from datalad_next.config import (
    defaults,
    legacy_cfg,
    legacy_register_config,
)


def has_config(name: str):
    return name in defaults


def register_config(*args, **kwargs):
    legacy_register_config(defaults, *args, **kwargs)


apply_patch('datalad', None, 'cfg', legacy_cfg)
apply_patch('datalad.support.extensions', None, 'register_config',
            register_config)
apply_patch('datalad.support.extensions', None, 'has_config', has_config)
