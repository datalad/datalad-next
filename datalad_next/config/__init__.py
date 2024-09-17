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
]

# TODO: eventually replace with
# from .legacy import ConfigManager
from datalad.config import ConfigManager

from . import dialog
from .default import (
    ImplementationDefault,
)
from .env import Environment
from .legacy import ConfigManager as LegacyConfigManager
from .multi import MultiConfiguration
from .source import ConfigurationSource

# instance for registering all defaults
defaults = ImplementationDefault()

manager = MultiConfiguration({
    # order reflects precedence rule, first source with a
    # key takes precedence
    'environment': Environment(),
    'defaults': defaults,
})
)
