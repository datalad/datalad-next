"""Make `register_config()/has_config()` use `ImplementationDefault` instance

The original implementation use a structure from
`datalad.interface.common_cfg`.  The `defaults` instance of
`ImplementationDefault` from `datalad_next.config` also contains this
information, and consolidates it into a new structure and API. This patch
ensures that extensions registering their configuration items using this legacy
API, also feeds this `defaults` instance.
"""

from datalad_next.patches import apply_patch

from datalad_next.config import (
    LegacyConfigManager,
    defaults,
    legacy_cfg,
    legacy_register_config,
)


def has_config(name: str):
    return name in defaults


def register_config(*args, **kwargs):
    legacy_register_config(defaults, *args, **kwargs)


# we have to inject the new class into a whole bunch of places, because
# it is imported very early
apply_patch('datalad.config', None, 'ConfigManager', LegacyConfigManager)
apply_patch('datalad.dataset.gitrepo', None, 'ConfigManager', LegacyConfigManager)

apply_patch('datalad', None, 'cfg', legacy_cfg)
apply_patch('datalad.distribution.dataset', None, 'cfg', legacy_cfg)
apply_patch('datalad.support.extensions', None, 'register_config',
            register_config)
apply_patch('datalad.support.extensions', None, 'has_config', has_config)
