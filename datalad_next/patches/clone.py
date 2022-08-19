import logging
import re
from os.path import expanduser
from typing import (
    Dict,
    List,
    Tuple,
)

from datalad.config import ConfigManager
from datalad.core.distributed import clone as mod_clone
from datalad.core.distributed.clone import (
    _get_tracking_source,
    _map_urls,
    configure_origins,
    decode_source_spec,
    postclone_check_head,
    postclone_checkout_commit,
    postclone_preannex_cfg_ria,
    postclonecfg_ria,
)
from datalad.dochelpers import single_or_plural
from datalad.interface.results import get_status_dict
from datalad.log import log_progress
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
    get_local_file_url,
    is_url,
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
from datalad.distribution.utils import (
    _get_flexible_source_candidates,
)

__docformat__ = 'restructuredtext'

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
            destds,
            cfg,
            last_candidate,
            reckless,
            checkout_gitsha,
            description,
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


def _generate_candidate_clone_sources(
        destds: Dataset,
        srcs: List,
        cfg: ConfigManager or None) -> List:
    """Convert "raw" clone source specs to candidate URLs
    """
    # check for configured URL mappings, either in the given config manager
    # or in the one of the destination dataset, which is typically not existent
    # yet and the process config is then used effectively
    srcs = _map_urls(cfg or destds.config, srcs)

    # decode all source candidate specifications
    # use a given config or pass None to make it use the process config
    # manager. Theoretically, we could also do
    # `cfg or destds.config` as done above, but some tests patch
    # the process config manager
    candidate_sources = [decode_source_spec(s, cfg=cfg) for s in srcs]

    # now expand the candidate sources with additional variants of the decoded
    # giturl, while duplicating the other properties in the additional records
    # for simplicity. The hope is to overcome a few corner cases and be more
    # robust than git clone
    return [
        dict(props, giturl=s) for props in candidate_sources
        for s in _get_flexible_source_candidates(props['giturl'])
    ]


def _test_existing_clone_target(
        destds: Dataset,
        candidate_sources: List) -> Tuple:
    """Check if the clone target exists, inspect it, if so

    Returns
    -------
    (bool, dict or None)
      A flag whether the target exists, and either a dict with properties
      of a result that should be yielded before an immediate return, or
      None, if the processing can continue
    """
    # important test! based on this `rmtree` will happen below after
    # failed clone
    dest_path = destds.pathobj
    dest_path_existed = dest_path.exists()
    if dest_path_existed and any(dest_path.iterdir()):
        if destds.is_installed():
            # check if dest was cloned from the given source before
            # this is where we would have installed this from
            # this is where it was actually installed from
            track_name, track_url = _get_tracking_source(destds)
            try:
                # this will get us track_url in system native path conventions,
                # whenever it is a path (and not a URL)
                # this is needed to match it to any potentially incoming local
                # source path in the 'notneeded' test below
                track_path = str(Path(track_url))
            except Exception as e:
                CapturedException(e)
                # this should never happen, because Path() will let any non-path
                # stringification pass through unmodified, but we do not want any
                # potential crash due to pathlib behavior changes
                lgr.debug("Unexpected behavior of pathlib!")
                track_path = None
            for cand in candidate_sources:
                src = cand['giturl']
                if track_url == src \
                        or (not is_url(track_url)
                            and get_local_file_url(
                                track_url, compatibility='git') == src) \
                        or track_path == expanduser(src):
                    return dest_path_existed, dict(
                        status='notneeded',
                        message=("dataset %s was already cloned from '%s'",
                                 destds,
                                 src),
                    )
        # anything else is an error
        return dest_path_existed, dict(
            status='error',
            message='target path already exists and not empty, '
                    'refuse to clone into target path',
        )
    # found no reason to stop, i.e. empty target dir
    return dest_path_existed, None


def _try_clone(
        destds: Dataset,
        candidate_sources: List,
        clone_opts: List,
        dest_path_existed: bool) -> Tuple:
    """Iterate over candidate URLs and attempt a clone

    Returns
    -------
    (dict or None, dict, dict or None)
      The record of the last clone attempt, a mapping of candidate URLs
      to potential error messages they yielded, and either a dict with
      properties of a result that should be yielded before an immediate
      return, or None, if the processing can continue
    """
    error_msgs = dict()  # accumulate all error messages formatted per each url
    for cand in candidate_sources:
        log_progress(
            lgr.info,
            'cloneds',
            'Attempting to clone from %s to %s', cand['giturl'], destds.path,
            update=1,
            increment=True)

        if cand.get('version', None):
            opts = clone_opts + ["--branch=" + cand['version']]
        else:
            opts = clone_opts

        try:
            GitRepo.clone(
                path=destds.path,
                url=cand['giturl'],
                clone_options=opts,
                create=True)

        except CommandError as e:
            ce = CapturedException(e)
            e_stderr = e.stderr

            error_msgs[cand['giturl']] = e
            lgr.debug("Failed to clone from URL: %s (%s)",
                      cand['giturl'], ce)
            if destds.pathobj.exists():
                lgr.debug("Wiping out unsuccessful clone attempt at: %s",
                          destds.path)
                # We must not just rmtree since it might be curdir etc
                # we should remove all files/directories under it
                # TODO stringification can be removed once patlib compatible
                # or if PY35 is no longer supported
                rmtree(destds.path, children_only=dest_path_existed)

            if e_stderr and 'could not create work tree' in e_stderr.lower():
                # this cannot be fixed by trying another URL
                re_match = re.match(r".*fatal: (.*)$", e_stderr,
                                    flags=re.MULTILINE | re.DOTALL)
                # cancel progress bar
                log_progress(
                    lgr.info,
                    'cloneds',
                    'Completed clone attempts for %s', destds
                )
                return cand, error_msgs, dict(
                    status='error',
                    message=re_match.group(1).strip()
                    if re_match else "stderr: " + e_stderr,
                )
            # next candidate
            continue

        # do not bother with other sources if succeeded
        break

    log_progress(
        lgr.info,
        'cloneds',
        'Completed clone attempts for %s', destds
    )
    return cand, error_msgs, None


def _format_clone_errors(
        destds: Dataset,
        error_msgs: List,
        last_clone_url: str) -> Tuple:
    """Format all accumulated clone errors across candidates into one message

    Returns
    -------
    (str, list)
      Message body and string formating arguments for it.
    """
    if len(error_msgs):
        if all(not e.stdout and not e.stderr for e in error_msgs.values()):
            # there is nothing we can learn from the actual exception,
            # the exit code is uninformative, the command is predictable
            error_msg = "Failed to clone from all attempted sources: %s"
            error_args = list(error_msgs.keys())
        else:
            error_msg = "Failed to clone from any candidate source URL. " \
                        "Encountered errors per each url were:\n- %s"
            error_args = '\n- '.join(
                '{}\n  {}'.format(url, exc.to_str())
                for url, exc in error_msgs.items()
            )
    else:
        # yoh: Not sure if we ever get here but I felt that there could
        #      be a case when this might happen and original error would
        #      not be sufficient to troubleshoot what is going on.
        error_msg = "Awkward error -- we failed to clone properly. " \
                    "Although no errors were encountered, target " \
                    "dataset at %s seems to be not fully installed. " \
                    "The 'succesful' source was: %s"
        error_args = (destds.path, last_clone_url)
    return error_msg, error_args


def _get_remote(repo: GitRepo) -> str:
    """Return the name of the remote of a freshly clones repo

    Raises
    ------
    RuntimeError
      In case there is no remote, which should never happen.
    """
    remotes = repo.get_remotes(with_urls_only=True)
    nremotes = len(remotes)
    if nremotes == 1:
        remote = remotes[0]
        lgr.debug("Determined %s to be remote of %s", remote, repo)
    elif remotes > 1:
        lgr.warning(
            "Fresh clone %s unexpected has multiple remotes: %s. Using %s",
            repo.path, remotes, remotes[0])
        remote = remotes[0]
    else:
        raise RuntimeError("bug: fresh clone has zero remotes")
    return remote


# This function is taken from datalad-core@bacdc8e8f8c942649cba98b15b426670c564ed3f
# datalad/core/distributed/clone.py
# Changes
# -
def postclonecfg_annexdataset(ds, reckless, description=None, remote="origin"):
    """If ds "knows annex" -- annex init it, set into reckless etc

    Provides additional tune up to a possibly an annex repo, e.g.
    "enables" reckless mode, sets up description
    """
    # in any case check whether we need to annex-init the installed thing:
    if not knows_annex(ds.path):
        # not for us
        return

    # init annex when traces of a remote annex can be detected
    if reckless == 'auto':
        lgr.debug(
            "Instruct annex to hardlink content in %s from local "
            "sources, if possible (reckless)", ds.path)
        ds.config.set(
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
        ds.config.set('annex.private', 'true', scope='local')

    lgr.debug("Initializing annex repo at %s", ds.path)
    # Note, that we cannot enforce annex-init via AnnexRepo().
    # If such an instance already exists, its __init__ will not be executed.
    # Therefore do quick test once we have an object and decide whether to call
    # its _init().
    #
    # Additionally, call init if we need to add a description (see #1403),
    # since AnnexRepo.__init__ can only do it with create=True
    repo = AnnexRepo(ds.path, init=True)
    if not repo.is_initialized() or description:
        repo._init(description=description)
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


def _post_gitclone_processing_(
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
        destds,
        cfg,
        gitclonerec,
        remote,
        reckless,
    )

    # TODO dissolve into the pre-init, init, and post-init
    yield from postclonecfg_annexdataset(
        destds,
        reckless,
        description,
        remote=remote)

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

    yield from _post_annex_init_processing_(
        destds,
        cfg,
        gitclonerec,
        remote,
        reckless,
    )


def _post_git_init_processing_(
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        reckless: None or str,
):
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


def _post_annex_init_processing_(
        destds: Dataset,
        cfg: ConfigManager,
        gitclonerec: Dict,
        remote: str,
        reckless: None or str,
):
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
