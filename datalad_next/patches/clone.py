"""Take spaghetti ``clone_dataset()`` apart into more manageable units

No change in functionality otherwise.
"""

__docformat__ = 'restructuredtext'

import logging
from typing import (
    Dict,
)

from datalad.config import ConfigManager
from datalad.core.distributed.clone import (
    configure_origins,
    postclone_check_head,
    postclone_checkout_commit,
)
from datalad_next.commands import get_status_dict
from datalad_next.exceptions import CapturedException
from datalad_next.utils import (
    knows_annex,
    rmtree,
)
from datalad_next.utils.patch import apply_patch
from datalad_next.datasets import (
    LegacyAnnexRepo as AnnexRepo,
    Dataset,
)

from .clone_utils import (
    _check_autoenable_special_remotes,
    _get_remote,
    _format_clone_errors,
    _try_clone_candidates,
    _test_existing_clone_target,
    _generate_candidate_clone_sources,
)

lgr = logging.getLogger('datalad.core.distributed.clone')


# This function is taken from datalad-core@bacdc8e8f8c942649cba98b15b426670c564ed3f
# datalad/core/distributed/clone.py
# Changes
# - Refactored into smaller, more manageable units
def clone_dataset(
        srcs,
        destds,
        reckless=None,
        description=None,
        result_props=None,
        cfg=None,
        checkout_gitsha=None,
        clone_opts=None):
    # docs are assigned from original version below

    if not result_props:
        # in case the caller had no specific idea on how results should look
        # like, provide sensible defaults
        result_props = dict(
            action='install',
            logger=lgr,
            ds=destds,
        )
    else:
        result_props = result_props.copy()

    candidate_sources = _generate_candidate_clone_sources(
        destds, srcs, cfg)

    # important test!
    # based on this `rmtree` will happen below after failed clone
    dest_path_existed, stop_props = _test_existing_clone_target(
        destds, candidate_sources)
    if stop_props:
        # something happened that indicates we cannot continue
        # yield and return
        result_props.update(stop_props)
        yield get_status_dict(**result_props)
        return

    if reckless is None and cfg:
        # if reckless is not explicitly given, but we operate on a
        # superdataset, query whether it has been instructed to operate
        # in a reckless mode, and inherit it for the coming clone
        reckless = cfg.get('datalad.clone.reckless', None)

    last_candidate, error_msgs, stop_props = _try_clone_candidates(
        destds=destds,
        candidate_sources=candidate_sources,
        clone_opts=clone_opts or [],
        dest_path_existed=dest_path_existed,
    )
    if stop_props:
        # no luck, report and stop
        result_props.update(stop_props)
        yield get_status_dict(**result_props)
        return
    else:
        # we can record the last attempt as the candidate URL that gave
        # a successful clone
        result_props['source'] = last_candidate

    if not destds.is_installed():
        # we do not have a clone, stop, provide aggregate error message
        # covering all attempts
        yield get_status_dict(
            status='error',
            message=_format_clone_errors(
                destds, error_msgs, last_candidate['giturl']),
            **result_props)
        return

    #
    # At minimum all further processing is all candidate for extension
    # patching.  wrap the whole thing in try-except, catch any exceptions
    # report it as an error results `rmtree` any intermediate and return
    #
    try:
        yield from _post_gitclone_processing_(
            destds=destds,
            cfg=cfg,
            gitclonerec=last_candidate,
            reckless=reckless,
            checkout_gitsha=checkout_gitsha,
            description=description,
        )
    except Exception as e:
        ce = CapturedException(e)
        # the rational for turning any exception into an error result is that
        # we are hadly able to distinguish user-error from an other errors
        yield get_status_dict(
            status='error',
            # XXX A test in core insists on the wrong message type to be used
            #error_message=ce.message,
            message=ce.message,
            exception=ce,
            **result_props,
        )
        rmtree(destds.path, children_only=dest_path_existed)
        return

    # yield successful clone of the base dataset now, as any possible
    # subdataset clone down below will not alter the Git-state of the
    # parent
    yield get_status_dict(status='ok', **result_props)


def _post_gitclone_processing_(
        *,
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        reckless: None or str,
        checkout_gitsha: None or str,
        description: None or str,
):
    """Perform git-clone post-processing

    This is helper is called immediately after a Git clone was established.

    The properties of that clone are passed via `gitclonerec`.

    Yields
    ------
    DataLad result records
    """
    dest_repo = destds.repo
    remote = _get_remote(dest_repo)

    yield from _post_git_init_processing_(
        destds=destds,
        cfg=cfg,
        gitclonerec=gitclonerec,
        remote=remote,
        reckless=reckless,
    )

    if knows_annex(destds.path):
        # init annex when traces of a remote annex can be detected
        yield from _pre_annex_init_processing_(
            destds=destds,
            cfg=cfg,
            gitclonerec=gitclonerec,
            remote=remote,
            reckless=reckless,
        )
        dest_repo = _annex_init(
            destds=destds,
            cfg=cfg,
            gitclonerec=gitclonerec,
            remote=remote,
            description=description,
        )
        yield from _post_annex_init_processing_(
            destds=destds,
            cfg=cfg,
            gitclonerec=gitclonerec,
            remote=remote,
            reckless=reckless,
        )

    if checkout_gitsha and \
       dest_repo.get_hexsha(
            dest_repo.get_corresponding_branch()) != checkout_gitsha:
        try:
            postclone_checkout_commit(dest_repo, checkout_gitsha,
                                      remote=remote)
        except Exception:
            # We were supposed to clone a particular version but failed to.
            # This is particularly pointless in case of subdatasets and
            # potentially fatal with current implementation of recursion.
            # see gh-5387
            lgr.debug(
                "Failed to checkout %s, removing this clone attempt at %s",
                checkout_gitsha, destds.path)
            raise

    yield from _pre_final_processing_(
        destds=destds,
        cfg=cfg,
        gitclonerec=gitclonerec,
        remote=remote,
        reckless=reckless,
    )


def _post_git_init_processing_(
        *,
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        reckless: None or str,
):
    """Any post-git-init processing that need not be concerned with git-annex
    """
    if not gitclonerec.get("version"):
        postclone_check_head(destds, remote=remote)

    # act on --reckless=shared-...
    # must happen prior git-annex-init, where we can cheaply alter the repo
    # setup through safe re-init'ing
    if reckless and reckless.startswith('shared-'):
        lgr.debug(
            'Reinitializing %s to enable shared access permissions',
            destds)
        destds.repo.call_git(['init', '--shared={}'.format(reckless[7:])])

    # trick to have the function behave like a generator, even if it
    # (currently) doesn't actually yield anything.
    # but a patched version might want to...so for uniformity with
    # _post_annex_init_processing_() let's do this
    if False:
        yield


def _pre_annex_init_processing_(
        *,
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        reckless: None or str,
):
    """Pre-processing a to-be-initialized annex repository"""
    if reckless == 'auto':
        lgr.debug(
            "Instruct annex to hardlink content in %s from local "
            "sources, if possible (reckless)", destds.path)
        destds.config.set(
            'annex.hardlink', 'true', scope='local', reload=True)

    # trick to have the function behave like a generator, even if it
    # (currently) doesn't actually yield anything.
    if False:
        yield


def _annex_init(
        *,
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        description: None or str,
):
    """Initializing an annex repository"""
    lgr.debug("Initializing annex repo at %s", destds.path)
    # Note, that we cannot enforce annex-init via AnnexRepo().
    # If such an instance already exists, its __init__ will not be executed.
    # Therefore do quick test once we have an object and decide whether to call
    # its _init().
    #
    # Additionally, call init if we need to add a description (see #1403),
    # since AnnexRepo.__init__ can only do it with create=True
    repo = AnnexRepo(destds.path, init=True)
    if not repo.is_initialized() or description:
        repo._init(description=description)
    return repo


def _post_annex_init_processing_(
        *,
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        reckless: None or str,
):
    """Post-processing an annex repository"""
    # convenience aliases
    repo = destds.repo
    ds = destds

    if reckless == 'auto' or (reckless and reckless.startswith('shared-')):
        repo.call_annex(['untrust', 'here'])

    _check_autoenable_special_remotes(repo)

    # we have just cloned the repo, so it has a remote `remote`, configure any
    # reachable origin of origins
    yield from configure_origins(ds, ds, remote=remote)


def _pre_final_processing_(
        *,
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        reckless: None or str,
):
    """Any post-processing after Git and git-annex pieces are fully initialized
    """
    if reckless:
        # store the reckless setting in the dataset to make it
        # known to later clones of subdatasets via get()
        destds.config.set(
            'datalad.clone.reckless', reckless,
            scope='local',
            reload=True)
    else:
        # We would still want to reload configuration to ensure that any of the
        # above git invocations could have potentially changed the config
        # TODO: might no longer be necessary if 0.14.0 adds reloading upon
        # non-readonly commands invocation
        destds.config.reload()

    # trick to have the function behave like a generator, even if it
    # (currently) doesn't actually yield anything.
    if False:
        yield


# apply patch
orig_clone_dataset = apply_patch(
    'datalad.core.distributed.clone', None, 'clone_dataset', clone_dataset,
    msg='Apply datalad-next patch to clone.py:clone_dataset')
apply_patch(
    'datalad.core.distributed.clone', 'clone_dataset', '__doc__',
    orig_clone_dataset.__doc__,
    msg='Reuse docstring of original clone_dataset()')
