"""Configuration query and manipulation

This modules provides the central ``ConfigManager`` class.

.. todo::

   Mention ``defaults``, ``manager``, and ``legacy_cfg``


Validation of configuration item values

There are two ways to do validation and type conversion.  on-access, or
on-load. Doing it on-load would allow to reject invalid configuration
immediately. But it might spend time on items that never get accessed.
On-access might waste cycles on repeated checks, and possible complain later
than useful. Here we nevertheless run a validator on-access in the default
implementation. Particular sources may want to override this, or ensure that
the stored value that is passed to a validator is already in the best possible
form to make re-validation the cheapest.

.. currentmodule:: datalad_next.config
.. autosummary::
   :toctree: generated

   ConfigManager
   LegacyConfigManager
   LegacyEnvironment
   GitConfig
   SystemGitConfig
   GlobalGitConfig
   LocalGitConfig
   GitEnvironment
   ImplementationDefault
   defaults
   dialog
   legacy_register_config
   legacy_cfg
"""

__all__ = [
    'ConfigManager',
    'LegacyConfigManager',
    'LegacyEnvironment',
    'GitConfig',
    'SystemGitConfig',
    'GlobalGitConfig',
    'LocalGitConfig',
    'GitEnvironment',
    'ImplementationDefault',
    'defaults',
    'dialog',
    'legacy_register_config',
    'legacy_cfg',
]

# TODO: eventually replace with
# from .legacy import ConfigManager
from datalad.config import ConfigManager  # type: ignore

from . import dialog
from .default import (
    ImplementationDefault,
    legacy_register_config,
)
from .default import (
    load_legacy_defaults as _load_legacy_defaults,
)
from .env import LegacyEnvironment
from .git import (
    GitConfig,
    GlobalGitConfig,
    LocalGitConfig,
    SystemGitConfig,
)
from .gitenv import GitEnvironment
from .legacy import ConfigManager as LegacyConfigManager
from .manager import ConfigManager as NextGenConfigManager

# instance for registering all defaults
defaults = ImplementationDefault()
# load up with legacy registrations for now
_load_legacy_defaults(defaults)

# order reflects precedence rule, first source with a key takes precedence
manager = NextGenConfigManager(defaults=defaults)
legacy_cfg = LegacyConfigManager()

ConfigManager.__doc__ = """\
Do not use anymore

.. deprecated:: 1.6

   The use of this class is discouraged. It is a legacy import from the
   ``datalad`` package, and a near drop-in replacement is provided with
   :class:`LegacyConfigManager`. Moreover, a :class:`LegacyConfigManager`-based
   instance of a global configuration manager is available as a
   :obj:`datalad_next.config.legacy_cfg` object in this module.

   New implementation are encourage to use the
   :obj:`datalad_next.config.manager` object (and instance of
   :class:`MultiConfiguration`) to query and manipulate configuration items.
"""
