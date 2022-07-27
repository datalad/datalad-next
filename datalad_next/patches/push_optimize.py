from itertools import chain
import logging
import re

import datalad.core.distributed.push as mod_push
from datalad.distribution.dataset import Dataset
from datalad.log import log_progress
from datalad.runner.exception import CommandError
from datalad.support.annexrepo import AnnexRepo
from datalad.utils import (
    ensure_list,
)


lgr = logging.getLogger('datalad.core.distributed.push')


def _push(dspath, content, target, data, force, jobs, res_kwargs, pbars,
          got_path_arg=False):
    force_git_push = force in ('all', 'gitpush')

    # nothing recursive in here, we only need a repo to work with
    ds = Dataset(dspath)
    repo = ds.repo

    res_kwargs.update(type='dataset', path=dspath)

    # content will be unique for every push (even on the same dataset)
    pbar_id = 'push-{}-{}'.format(target, id(content))
    # register for final orderly take down
    pbars[pbar_id] = ds
    log_progress(
        lgr.info, pbar_id,
        'Determine push target',
        unit=' Steps',
        label='Push',
        total=4,
    )

    #
    # First we must figure out where to push to, if needed
    #

    # will contain info to determine what refspecs need to be pushed
    # and to which remote, if none is given
    wannabe_gitpush = None
    # pristine input arg
    _target = target
    # verified or auto-detected target sibling name
    target, status, message, wannabe_gitpush = _get_push_target(repo, target)
    if target is None:
        yield dict(
            res_kwargs,
            status=status,
            message=message,
        )
        return

    log_progress(
        lgr.info, pbar_id, "Push refspecs",
        label="Push to '{}'".format(target), update=1, total=4)

    # cache repo type
    is_annex_repo = isinstance(ds.repo, AnnexRepo)
    # handling pure special remotes is a lot simpler
    target_is_git_remote = repo.config.get(
        f'remote.{target}.url', None) is not None
    # TODO would is be useful to also check whether the
    # target is set 'annex-ignore' right here?

    if target_is_git_remote:
        # branch and refspec only need handling for Git remotes
        refspecs2push = _get_refspecs2push(
            repo, is_annex_repo, target, target_arg=_target,
            wannabe_gitpush=wannabe_gitpush)

        if not refspecs2push:
            # nothing was set up for push, and we have no active branch
            # this is a weird one, let's confess and stop here
            # I don't think we need to support such a scenario
            yield dict(
                res_kwargs,
                status='impossible',
                message=
                'There is no active branch, cannot determine remote '
                'branch'
            )
            return

    #
    # We know where to push to, honor dependencies
    # XXX we could do this right after we know the value of `target`,
    # but this would mean we would also push to dependencies
    # even when no actual push to the primary target is needed
    #

    # list of remotes that are publication dependencies for the
    # target remote
    # multiple dependencies could come from multiple declarations
    # of such a config items, but each declaration could also
    # contain a white-space separated list of remote names
    # see https://github.com/datalad/datalad/issues/6867
    publish_depends = list(chain.from_iterable(
        d.split() for d in ensure_list(
            ds.config.get(
                f'remote.{target}.datalad-publish-depends',
                [],
                get_all=True))))
    if publish_depends:
        lgr.debug("Discovered publication dependencies for '%s': %s'",
                  target, publish_depends)

    # we know what to push and where, now dependency processing first
    for r in publish_depends:
        # simply make a call to this function again, all the same, but
        # target is different
        # TODO: what if a publication dependency doesn't have any of the
        # determined refspecs2push, yet. Should we not attempt to push them,
        # because the main target has it?
        yield from _push(
            dspath,
            content,
            # to this particular dependency
            r,
            data,
            force,
            jobs,
            res_kwargs.copy(),
            pbars,
            got_path_arg=got_path_arg,
        )

    # and lastly the primary push target

    # git-annex data copy
    #
    if is_annex_repo:
        if data != "nothing":
            log_progress(
                lgr.info, pbar_id, "Transfer data",
                label="Transfer data to '{}'".format(target), update=2, total=4)
            yield from mod_push._transfer_data(
                repo,
                ds,
                target,
                content,
                data,
                force,
                jobs,
                res_kwargs.copy(),
                got_path_arg=got_path_arg,
            )
        else:
            lgr.debug("Data transfer to '%s' disabled by argument", target)
    else:
        lgr.debug("No data transfer: %s is not a git annex repository", repo)

    if not target_is_git_remote or not refspecs2push:
        # there is nothing that we need to push or sync with on the git-side
        # of things with this remote
        lgr.debug('No git-remote or no refspecs found that need to be pushed')
        # TODO ensure progress bar is ended properly
        return

    log_progress(
        lgr.info, pbar_id, "Update availability information",
        label="Update availability for '{}'".format(target), update=3, total=4)

    # TODO fetch is only needed if anything was actually transferred. Collect this
    # info and make the following conditional on it

    # after file transfer the remote might have different commits to
    # the annex branch. They have to be merged locally, otherwise a
    # push of it further down will fail
    _sync_remote_annex_branch(repo, target, is_annex_repo)

    # and push all relevant branches, plus the git-annex branch to announce
    # local availability info too
    yield from mod_push._push_refspecs(
        repo,
        target,
        refspecs2push,
        force_git_push,
        res_kwargs.copy(),
    )


def _append_branch_to_refspec_if_needed(repo, refspecs, branch):
    # try to anticipate any flavor of an idea of a branch ending up in a
    # refspec
    looks_like_that_branch = re.compile(
        r'((^|.*:)refs/heads/|.*:|^){}$'.format(branch))
    if all(not looks_like_that_branch.match(r) for r in refspecs):
        refspecs.append(
            branch
            if repo.config.get('branch.{}.merge'.format(branch), None)
            else '{branch}:{branch}'.format(branch=branch)
        )


def _get_push_dryrun(repo, remote=None):
    """
    Returns
    -------
    list
      The result of the dry-run. Will be an empty list if the dry-run
      failed for any reason.
    """
    try:
        wannabe_gitpush = repo.push(remote=remote, git_options=['--dry-run'])
    except Exception as e:
        lgr.debug(
            'Dry-run push to %r remote failed, '
            'assume no configuration: %s',
            remote if remote else 'default',
            e)
        wannabe_gitpush = []
    return wannabe_gitpush


def _get_push_target(repo, target_arg):
    """
    Returns
    -------
    str or None, str, str or None, list or None
      Target label, if determined; status label; optional message;
      git-push-dryrun result for re-use or None, if no dry-run was
      attempted.
    """
    # verified or auto-detected
    target = None
    # for re-use
    wannabe_gitpush = None
    if not target_arg:
        # let Git figure out what needs doing
        # we will reuse the result further down again, so nothing is wasted
        wannabe_gitpush = _get_push_dryrun(repo)
        # we did not get an explicit push target, get it from Git
        target = set(p.get('remote', None) for p in wannabe_gitpush)
        # handle case where a pushinfo record did not have a 'remote'
        # property -- should not happen, but be robust
        target.discard(None)
        if not len(target):
            return (
                None,
                'impossible',
                'No push target given, and none could be '
                'auto-detected, please specify via --to',
                wannabe_gitpush,
            )
        elif len(target) > 1:
            # dunno if this can ever happen, but if it does, report
            # nicely
            return (
                None,
                'error',
                ('No push target given, '
                 'multiple candidates auto-detected: %s',
                 list(target)),
                wannabe_gitpush,
            )
        else:
            # can only be a single one at this point
            target = target.pop()

    if not target:
        if target_arg not in repo.get_remotes():
            return (
                None,
                'error',
                ("Unknown target sibling '%s'.", target_arg),
                wannabe_gitpush,
            )
        target = target_arg

    # we must have a valid target label now
    assert target

    return (target, 'ok', None, wannabe_gitpush)


def _get_refspecs2push(repo, is_annex_repo, target, target_arg=None,
                       wannabe_gitpush=None):
    """Determine which refspecs shall be pushed to target

    Parameters
    ----------
    repo: Repo
    target: str
      Pre-determined push target
    target_arg: str, optional
      Target level given to original push() call, if any.
    wannabe_gitpush: list, optional
      Any cashed git-push-dryrun results for `target`

    Returns
    -------
    list
      Refspec labels
    """
    # (possibly redo) a push attempt to figure out what needs pushing
    # do this on the main target only, and apply the result to all
    # dependencies
    if target_arg and wannabe_gitpush is None:
        # only do it when an explicit target was given, otherwise
        # we can reuse the result from the auto-probing above
        wannabe_gitpush = _get_push_dryrun(repo, remote=target)

    refspecs2push = [
        # if an upstream branch is set, go with it
        p['from_ref']
        if repo.config.get(
            # refs come in as refs/heads/<branchname>
            # need to cut the prefix
            'branch.{}.remote'.format(p['from_ref'][11:]),
            None) == target and repo.config.get(
                'branch.{}.merge'.format(p['from_ref'][11:]),
                None)
        # if not, define target refspec explicitly to avoid having to
        # set an upstream branch, which would happen implicitly from
        # a users POV, and may also be hard to decide when publication
        # dependencies are present
        else '{}:{}'.format(p['from_ref'], p['to_ref'])
        for p in wannabe_gitpush
        if 'uptodate' not in p['operations'] and (
            # cannot think of a scenario where we would want to push a
            # managed branch directly, instead of the corresponding branch
            'refs/heads/adjusted' not in p['from_ref'])
    ]

    active_branch = repo.get_active_branch()
    if active_branch and is_annex_repo:
        # we could face a managed branch, in which case we need to
        # determine the actual one and make sure it is sync'ed with the
        # managed one, and push that one instead. following methods can
        # be called unconditionally
        repo.localsync(managed_only=True)
        active_branch = repo.get_corresponding_branch(
            active_branch) or active_branch

    # make sure that we always push the active branch (the context for the
    # potential path arguments) and the annex branch -- because we claim
    # to know better than any git config
    must_have_branches = [active_branch] if active_branch else []
    if is_annex_repo:
        must_have_branches.append('git-annex')
    for branch in must_have_branches:
        # refspecs2push= (in-place modification inside)
        _append_branch_to_refspec_if_needed(repo, refspecs2push, branch)

    return refspecs2push


def _sync_remote_annex_branch(repo, target, is_annex_repo):
    """Fetch remote annex-branch and merge locally

    Useful to ensure a push to the target will not fail due to unmerged
    remote changes.

    Parameters
    ----------
    repo: Repo
    target: str
    is_annex_repo: bool
    """
    try:
        # fetch remote, let annex sync them locally, so that the push
        # later on works.
        # We have to fetch via the push url (if there is any),
        # not a pull url.
        # The latter might be dumb and without the execution of a
        # post-update hook we might not be able to retrieve the
        # server-side git-annex branch updates (and git-annex does
        # not trigger the hook on copy), but we know we have
        # full access via the push url -- we have just used it to copy.
        lgr.debug("Fetching 'git-annex' branch updates from '%s'", target)
        fetch_cmd = ['fetch', target, 'git-annex']
        pushurl = repo.config.get(
            'remote.{}.pushurl'.format(target), None)
        if pushurl:
            # for some reason overwriting remote.{target}.url
            # does not have any effect...
            fetch_cmd = [
                '-c',
                'url.{}.insteadof={}'.format(
                    pushurl,
                    repo.config.get(
                        'remote.{}.url'.format(target), None)
                )
            ] + fetch_cmd
            lgr.debug(
                "Sync local annex branch from pushurl after remote "
                'availability update.')
        repo.call_git(fetch_cmd)
        # If no CommandError was raised, it means that remote has git-annex
        # but local repo might not be an annex yet. Since there is nothing to "sync"
        # from us, we just skip localsync without mutating repo into an AnnexRepo
        if is_annex_repo:
            repo.localsync(target)
    except CommandError as e:
        # it is OK if the remote doesn't have a git-annex branch yet
        # (e.g. fresh repo)
        # TODO is this possible? we just copied? Maybe check if anything
        # was actually copied?
        if "fatal: couldn't find remote ref git-annex" not in e.stderr.lower():
            raise
        lgr.debug('Remote does not have a git-annex branch: %s', e)


lgr.debug("Patching datalad.core.distributed.push._push")
mod_push._push = _push
