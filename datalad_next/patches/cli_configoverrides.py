from datalad.config import _update_from_env as _update_from_datalad_env
from datalad.cli.helpers import _parse_overrides_from_cmdline

from datalad_next.config.utils import (
    get_gitconfig_items_from_env,
    set_gitconfig_items_in_env,
)

from . import apply_patch


def parse_overrides_from_cmdline(cmdlineargs):
    # read from cmdlineargs first to error on any syntax issues
    # before any other processing
    cli_overrides = _parse_overrides_from_cmdline(cmdlineargs)

    # reuse datalad-core implementation of datalad-specific ENV parsing
    # for config items
    overrides = {}
    _update_from_datalad_env(overrides)

    # let CLI settings override any ENV -- in-line with the behavior of Git
    overrides.update(cli_overrides)

    # read any existing GIT_CONFIG ENV vars and superimpose our
    # overrides on them, repost in ENV using git-native approach.
    # This will apply the overrides to any git(-config) calls
    # in this process and any subprocess
    gc_overrides = get_gitconfig_items_from_env()
    gc_overrides.update(overrides)
    set_gitconfig_items_in_env(gc_overrides)

    # we do not actually disclose any of these overrides.
    # the CLI runs a `datalad.cfg.reload(force=True)`
    # immediately after executing this function and thereby
    # pulls in the overrides we just posted into the ENV
    # here. This change reduced the scope of
    # `datalad.cfg.overrides` to be mere instance overrides
    # and no longer process overrides. This rectifies the mismatch
    # between appearance and actual impact of this information
    # in the ConfigManager
    return {}


apply_patch(
    'datalad.cli.helpers', None, '_parse_overrides_from_cmdline',
    parse_overrides_from_cmdline,
    msg='Enable posting DataLad config overrides CLI/ENV as '
    'GIT_CONFIG items in process ENV',
)
