from datasalad.settings import Settings

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
