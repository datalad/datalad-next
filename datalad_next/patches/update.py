"""Robustify ``update()`` target detection for adjusted mode datasets

The true cause of the problem is not well understood.
https://github.com/datalad/datalad/issues/7507 documents that it is not
easy to capture the breakage in a test.
"""

from . import apply_patch


# This function is taken from datalad-core@cdc0ceb30ae04265c5369186acf2ab2683a8ec96
# datalad/distribution/update.py
# The change has been proposed in https://github.com/datalad/datalad/pull/7522
def _choose_update_target(repo, branch, remote, cfg_remote):
    """Select a target to update `repo` from.

    Note: This function is not concerned with _how_ the update is done (e.g.,
    merge, reset, ...).

    Parameters
    ----------
    repo : Repo instance
    branch : str
        The current branch.
    remote : str
        The remote which updates are coming from.
    cfg_remote : str
        The configured upstream remote.

    Returns
    -------
    str (the target) or None if a choice wasn't made.
    """
    target = None
    if cfg_remote and remote == cfg_remote:
        # Use the configured cfg_remote branch as the target.
        #
        # In this scenario, it's tempting to use FETCH_HEAD as the target. For
        # a merge, that would be the equivalent of 'git pull REMOTE'. But doing
        # so would be problematic when the GitRepo.fetch() call was passed
        # all_=True. Given we can't use FETCH_HEAD, it's tempting to use the
        # branch.*.merge value, but that assumes a value for remote.*.fetch.
        target = repo.call_git_oneline(
            ["rev-parse", "--symbolic-full-name", "--abbrev-ref=strict",
             # THIS IS THE PATCH: prefix @{upstream} with the branch name
             # of the corresponding branch
             f"{repo.get_corresponding_branch(branch) or ''}" "@{upstream}"],
            read_only=True)
    elif branch:
        remote_branch = "{}/{}".format(remote, branch)
        if repo.commit_exists(remote_branch):
            target = remote_branch
    return target


apply_patch(
    'datalad.distribution.update', None, '_choose_update_target',
    _choose_update_target)
