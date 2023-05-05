from datalad.config import (
    CommandError,
    DATASET_CONFIG_FILE,
    KillOutput,
    _update_from_env,
)
from datalad_next.config.utils import (
    get_gitconfig_items_from_env,
    set_gitconfig_items_in_env,
)
from . import apply_patch


# This method is taken from datalad-core@c7d31e1d865a0939b5013962a94edca6ead3d6b3
# datalad/config.py
# Changes (look for "PATCH" below)
# - read and post configuration overrides in the ENV as detailed
#   in Solution 2 at
#   https://github.com/datalad/datalad-next/issues/325#issuecomment-1528709961
def reload(self, force=False):
    """Reload all configuration items from the configured sources

    If `force` is False, all files configuration was previously read from
    are checked for differences in the modification times. If no difference
    is found for any file no reload is performed. This mechanism will not
    detect newly created global configuration files, use `force` in this case.
    """
    # PATCH
    # read any existing GIT_CONFIG ENV vars and superimpose this instances
    # overrides on them, repost in ENV using git-native approach.
    # This will apply the overrides to the respective git-config call outcomes
    # below
    gc_overrides = get_gitconfig_items_from_env()
    gc_overrides.update(self.overrides)
    set_gitconfig_items_in_env(gc_overrides)

    run_args = ['-z', '-l', '--show-origin']

    # update from desired config sources only
    # 2-step strategy:
    #   - load datalad dataset config from dataset
    #   - load git config from all supported by git sources
    # in doing so we always stay compatible with where Git gets its
    # config from, but also allow to override persistent information
    # from dataset locally or globally

    # figure out what needs to be reloaded at all
    to_run = {}
    # committed branch config
    # well, actually not necessarily committed

    if self._src_mode != 'local' and self._repo_pathobj:
        # we have to read the branch config from this existing repo
        if self._repo_dot_git == self._repo_pathobj:
            # this is a bare repo, we go with the default HEAD,
            # if it has a config
            try:
                # will blow if absent
                self._runner.run([
                    'git', 'cat-file', '-e', 'HEAD:.datalad/config'],
                    protocol=KillOutput)
                to_run['branch'] = run_args + [
                    '--blob', 'HEAD:.datalad/config']
            except CommandError:
                # all good, just no branch config
                pass
        else:
            # non-bare repo
            # we could use the same strategy as for bare repos, and rely
            # on the committed config, however, for now we must pay off
            # the sins of the past and work with the file at hand
            dataset_cfgfile = self._repo_pathobj / DATASET_CONFIG_FILE
            if dataset_cfgfile.exists() and (
                    force or self._need_reload(self._stores['branch'])):
                # we have the file and are forced or encourages to (re)load
                to_run['branch'] = run_args + [
                    '--file', str(dataset_cfgfile)]

    if self._src_mode != 'branch' and (
            force or self._need_reload(self._stores['git'])):
        to_run['git'] = run_args + ['--local'] \
            if self._src_mode == 'branch-local' \
            else run_args

    # reload everything that was found todo
    while to_run:
        store_id, runargs = to_run.popitem()
        self._stores[store_id] = self._reload(runargs)

    # always update the merged representation, even if we did not reload
    # anything from a file. ENV or overrides could change independently
    # start with the commit dataset config
    merged = self._stores['branch']['cfg'].copy()
    # local config always takes precedence
    merged.update(self._stores['git']['cfg'])
    # superimpose overrides
    # PATCH actually do not (re)apply overrides, this is now done at the top
    # of the this method
    merged.update(self.overrides)

    # override with environment variables, unless we only want to read the
    # dataset's commit config
    if self._src_mode != 'branch':
        _update_from_env(merged)
    self._merged_store = merged


apply_patch(
    'datalad.config', 'ConfigManager', 'reload',
    reload,
    msg='Wrap ConfigManager.reload() to post config overrides in ENV')
