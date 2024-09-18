"""Configuration query and manipulation

This modules provides the central ``ConfigManager`` class.

.. currentmodule:: datalad_next.config
.. autosummary::
   :toctree: generated

   ConfigManager
"""

__all__ = [
    'ConfigManager',
    'ConfigurationSource',
    'Environment',
    'ImplementationDefault',
    'MultiConfiguration',
    'defaults',
    'dialog',
    'legacy_register_config',
    'legacy_cfg',
]

# TODO: eventually replace with
# from .legacy import ConfigManager
from datalad.config import ConfigManager

from . import dialog
from .default import (
    ImplementationDefault,
    legacy_register_config,
)
from .default import (
    load_legacy_defaults as _load_legacy_defaults,
)
from .env import Environment
from .git import (
    GitConfig,
    LocalGitConfig,
    GlobalGitConfig,
    SystemGitConfig,
)
from .legacy import ConfigManager as LegacyConfigManager
from .multi import MultiConfiguration
from .source import ConfigurationSource

# instance for registering all defaults
defaults = ImplementationDefault()
# load up with legacy registrations for now
_load_legacy_defaults(defaults)

manager = MultiConfiguration({
    # order reflects precedence rule, first source with a
    # key takes precedence
    'environment': Environment(),
    'git-global': GlobalGitConfig(),
    'git-system': SystemGitConfig(),
    'defaults': defaults,
})

legacy_cfg = LegacyConfigManager(
    manager,
)
