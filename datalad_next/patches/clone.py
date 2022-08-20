""""""

__docformat__ = 'restructuredtext'

import logging
from typing import (
    Dict,
)

from datalad.config import ConfigManager
from datalad.core.distributed import clone as mod_clone
from datalad.core.distributed.clone import (
    configure_origins,
    postclone_check_head,
    postclone_checkout_commit,
    postclone_preannex_cfg_ria,
    postclonecfg_ria,
)
from datalad.dochelpers import single_or_plural
from datalad.interface.results import get_status_dict
from datalad.support.annexrepo import AnnexRepo
from datalad.support.gitrepo import (
    GitRepo,
)
from datalad.cmd import (
    CommandError,
)
from datalad.support.exceptions import (
    CapturedException,
)
from datalad.support.network import (
    RI,
)
from datalad.utils import (
    Path,
    check_symlink_capability,
    ensure_bool,
    knows_annex,
    rmtree,
)
from datalad.distribution.dataset import (
    Dataset,
)

from .clone_utils import (
    _get_remote,
    _format_clone_errors,
    _try_clone,
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

    last_candidate, error_msgs, stop_props = _try_clone(
        destds,
        candidate_sources,
        clone_opts or [],
        dest_path_existed,
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

    # In case of RIA stores we need to prepare *before* annex is called at all
    if gitclonerec['type'] == 'ria':
        postclone_preannex_cfg_ria(destds, remote=remote)

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
    elif reckless == 'ephemeral':
        # In ephemeral clones we set annex.private=true. This would prevent the
        # location itself being recorded in uuid.log. With a private repo,
        # declaring dead (see below after annex-init) seems somewhat
        # superfluous, but on the other hand:
        # If an older annex that doesn't support private yet touches the
        # repo, the entire purpose of ephemeral would be sabotaged if we did
        # not declare dead in addition. Hence, keep it regardless of annex
        # version.
        destds.config.set('annex.private', 'true', scope='local')
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

    elif reckless == 'ephemeral':
        # with ephemeral we declare 'here' as 'dead' right away, whenever
        # we symlink the remote's annex, since availability from 'here' should
        # not be propagated for an ephemeral clone when we publish back to
        # the remote.
        # This will cause stuff like this for a locally present annexed file:
        # % git annex whereis d1
        # whereis d1 (0 copies) failed
        # BUT this works:
        # % git annex find . --not --in here
        # % git annex find . --in here
        # d1

        # we don't want annex copy-to <remote>
        ds.config.set(
            f'remote.{remote}.annex-ignore', 'true',
            scope='local')
        ds.repo.set_remote_dead('here')

        if check_symlink_capability(ds.repo.dot_git / 'dl_link_test',
                                    ds.repo.dot_git / 'dl_target_test'):
            # symlink the annex to avoid needless copies in an ephemeral clone
            annex_dir = ds.repo.dot_git / 'annex'
            origin_annex_url = ds.config.get(f"remote.{remote}.url", None)
            origin_git_path = None
            if origin_annex_url:
                try:
                    # Deal with file:// scheme URLs as well as plain paths.
                    # If origin isn't local, we have nothing to do.
                    origin_git_path = Path(RI(origin_annex_url).localpath)

                    # we are local; check for a bare repo first to not mess w/
                    # the path
                    if GitRepo(origin_git_path, create=False).bare:
                        # origin is a bare repo -> use path as is
                        pass
                    elif origin_git_path.name != '.git':
                        origin_git_path /= '.git'
                except ValueError as e:
                    CapturedException(e)
                    # Note, that accessing localpath on a non-local RI throws
                    # ValueError rather than resulting in an AttributeError.
                    # TODO: Warning level okay or is info level sufficient?
                    # Note, that setting annex-dead is independent of
                    # symlinking .git/annex. It might still make sense to
                    # have an ephemeral clone that doesn't propagate its avail.
                    # info. Therefore don't fail altogether.
                    lgr.warning("reckless=ephemeral mode: %s doesn't seem "
                                "local: %s\nno symlinks being used",
                                remote, origin_annex_url)
            if origin_git_path:
                # TODO make sure that we do not delete any unique data
                rmtree(str(annex_dir)) \
                    if not annex_dir.is_symlink() else annex_dir.unlink()
                annex_dir.symlink_to(origin_git_path / 'annex',
                                     target_is_directory=True)
        else:
            # TODO: What level? + note, that annex-dead is independent
            lgr.warning("reckless=ephemeral mode: Unable to create symlinks on "
                        "this file system.")

    srs = {True: [], False: []}  # special remotes by "autoenable" key
    remote_uuids = None  # might be necessary to discover known UUIDs

    repo_config = repo.config
    # Note: The purpose of this function is to inform the user. So if something
    # looks misconfigured, we'll warn and move on to the next item.
    for uuid, config in repo.get_special_remotes().items():
        sr_name = config.get('name', None)
        if sr_name is None:
            lgr.warning(
                'Ignoring special remote %s because it does not have a name. '
                'Known information: %s',
                uuid, config)
            continue
        sr_autoenable = config.get('autoenable', False)
        try:
            sr_autoenable = ensure_bool(sr_autoenable)
        except ValueError as e:
            CapturedException(e)
            lgr.warning(
                'Failed to process "autoenable" value %r for sibling %s in '
                'dataset %s as bool.'
                'You might need to enable it later manually and/or fix it up to'
                ' avoid this message in the future.',
                sr_autoenable, sr_name, ds.path)
            continue

        # If it looks like a type=git special remote, make sure we have up to
        # date information. See gh-2897.
        if sr_autoenable and repo_config.get("remote.{}.fetch".format(sr_name)):
            try:
                repo.fetch(remote=sr_name)
            except CommandError as exc:
                ce = CapturedException(exc)
                lgr.warning("Failed to fetch type=git special remote %s: %s",
                            sr_name, exc)

        # determine whether there is a registered remote with matching UUID
        if uuid:
            if remote_uuids is None:
                remote_uuids = {
                    # Check annex-config-uuid first. For sameas annex remotes,
                    # this will point to the UUID for the configuration (i.e.
                    # the key returned by get_special_remotes) rather than the
                    # shared UUID.
                    (repo_config.get('remote.%s.annex-config-uuid' % r) or
                     repo_config.get('remote.%s.annex-uuid' % r))
                    for r in repo.get_remotes()
                }
            if uuid not in remote_uuids:
                srs[sr_autoenable].append(sr_name)

    if srs[True]:
        lgr.debug(
            "configuration for %s %s added because of autoenable,"
            " but no UUIDs for them yet known for dataset %s",
            # since we are only at debug level, we could call things their
            # proper names
            single_or_plural("special remote",
                             "special remotes", len(srs[True]), True),
            ", ".join(srs[True]),
            ds.path
        )

    if srs[False]:
        # if has no auto-enable special remotes
        lgr.info(
            'access to %s %s not auto-enabled, enable with:\n'
            '\t\tdatalad siblings -d "%s" enable -s %s',
            # but since humans might read it, we better confuse them with our
            # own terms!
            single_or_plural("dataset sibling",
                             "dataset siblings", len(srs[False]), True),
            ", ".join(srs[False]),
            ds.path,
            srs[False][0] if len(srs[False]) == 1 else "SIBLING",
        )

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
    # perform any post-processing that needs to know details of the clone
    # source
    if gitclonerec['type'] == 'ria':
        yield from postclonecfg_ria(destds, gitclonerec,
                                    remote=remote)

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


# apply patch
lgr.debug('Apply datalad-next patch to clone.py:clone_dataset')
clone_dataset.__doc__ = mod_clone.clone_dataset.__doc__
mod_clone.clone_dataset = clone_dataset
