from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from datasalad.settings import (
    Settings,
    UnsetValue,
)

from datalad_next.config.default import ImplementationDefault
from datalad_next.config.env import LegacyEnvironment
from datalad_next.config.git import (
    GlobalGitConfig,
    SystemGitConfig,
)
from datalad_next.config.gitenv import GitEnvironment


class ConfigManager(Settings):
    def __init__(self, defaults: ImplementationDefault):
        super().__init__({
            # call this one 'command', because that is what Git calls the scope
            # of items pulled from the process environment
            'git-command': GitEnvironment(),
            'legacy-environment': LegacyEnvironment(),
            'git-global': GlobalGitConfig(),
            'git-system': SystemGitConfig(),
            'defaults': defaults,
        })

    @contextmanager
    def overrides(self, overrides: dict) -> Generator[ConfigManager]:
        """Context manager to temporarily set configuration overrides

        Internally, these overrides are posted to the 'git-command' scope,
        hence affect the process environment and newly spawn subprocesses.

        .. todo::

           ATM this implementation cannot handle multi-value settings.
           Neither as incoming overrides, nor as to-be-replaced existing
           items.
        """
        gitcmdsrc = self.sources['git-command']
        restore = {}
        # TODO: handle multivalue settings
        for k, v in overrides.items():
            restore[k] = gitcmdsrc.get(k, gitcmdsrc.item_type(UnsetValue))
            gitcmdsrc[k] = gitcmdsrc.item_type(v)
        try:
            yield self
        finally:
            for k, v in restore.items():
                if v.pristine_value is UnsetValue:
                    del gitcmdsrc[k]
                else:
                    gitcmdsrc[k] = v
