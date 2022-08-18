import logging
import re
from os.path import expanduser
from collections import OrderedDict
from typing import List

from datalad.core.distributed import clone as mod_clone
from datalad.core.distributed.clone import (
    _get_tracking_source,
    _map_urls,
    decode_source_spec,
    postclone_check_head,
    postclone_checkout_commit,
    postclone_preannex_cfg_ria,
    postclonecfg_annexdataset,
    postclonecfg_ria,
)

from datalad.interface.results import get_status_dict
from datalad.log import log_progress
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
    get_local_file_url,
    is_url,
)
from datalad.utils import (
    Path,
    rmtree,
)
from datalad.distribution.utils import (
    _get_flexible_source_candidates,
)

__docformat__ = 'restructuredtext'

lgr = logging.getLogger('datalad.core.distributed.clone')


# This function is taken from datalad-core@bacdc8e8f8c942649cba98b15b426670c564ed3f
# datalad/core/distributed/clone.py
# Changes
# -
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

    dest_path = destds.pathobj

    candidate_sources = _generate_candidate_clone_sources(
        srcs, cfg or destds.config)

    # important test! based on this `rmtree` will happen below after failed clone
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
                # this should never happen, because Path() will let any non-path stringification
                # pass through unmodified, but we do not want any potential crash due to
                # pathlib behavior changes
                lgr.debug("Unexpected behavior of pathlib!")
                track_path = None
            for cand in candidate_sources:
                src = cand['giturl']
                if track_url == src \
                        or (not is_url(track_url)
                            and get_local_file_url(track_url, compatibility='git') == src) \
                        or track_path == expanduser(src):
                    yield get_status_dict(
                        status='notneeded',
                        message=("dataset %s was already cloned from '%s'",
                                 destds,
                                 src),
                        **result_props)
                    return
        # anything else is an error
        yield get_status_dict(
            status='error',
            message='target path already exists and not empty, refuse to clone into target path',
            **result_props)
        return

    log_progress(
        lgr.info,
        'cloneds',
        'Cloning dataset to %s', destds,
        total=len(candidate_sources),
        label='Clone attempt',
        unit=' Candidate locations',
    )
    clone_opts = clone_opts or []
    error_msgs = OrderedDict()  # accumulate all error messages formatted per each url
    for cand in candidate_sources:
        log_progress(
            lgr.info,
            'cloneds',
            'Attempting to clone from %s to %s', cand['giturl'], dest_path,
            update=1,
            increment=True)

        if cand.get('version', None):
            opts = clone_opts + ["--branch=" + cand['version']]
        else:
            opts = clone_opts

        try:
            # TODO for now GitRepo.clone() cannot handle Path instances, and PY35
            # doesn't make it happen seamlessly
            GitRepo.clone(
                path=str(dest_path),
                url=cand['giturl'],
                clone_options=opts,
                create=True)

        except CommandError as e:
            ce = CapturedException(e)
            e_stderr = e.stderr

            error_msgs[cand['giturl']] = e
            lgr.debug("Failed to clone from URL: %s (%s)",
                      cand['giturl'], ce)
            if dest_path.exists():
                lgr.debug("Wiping out unsuccessful clone attempt at: %s",
                          dest_path)
                # We must not just rmtree since it might be curdir etc
                # we should remove all files/directories under it
                # TODO stringification can be removed once patlib compatible
                # or if PY35 is no longer supported
                rmtree(str(dest_path), children_only=dest_path_existed)

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
                yield get_status_dict(
                    status='error',
                    message=re_match.group(1).strip()
                    if re_match else "stderr: " + e_stderr,
                    **result_props)
                return
            # next candidate
            continue

        result_props['source'] = cand
        # do not bother with other sources if succeeded
        break

    log_progress(
        lgr.info,
        'cloneds',
        'Completed clone attempts for %s', destds
    )

    if not destds.is_installed():
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
            error_args = (destds.path, cand['giturl'])
        yield get_status_dict(
            status='error',
            message=(error_msg, error_args),
            **result_props)
        return

    dest_repo = destds.repo

    remotes = dest_repo.get_remotes(with_urls_only=True)
    nremotes = len(remotes)
    if nremotes == 1:
        remote = remotes[0]
        lgr.debug("Determined %s to be remote of %s", remote, destds)
    elif remotes > 1:
        lgr.warning(
            "Fresh clone %s unexpected has multiple remotes: %s. Using %s",
            destds.path, remotes, remotes[0])
        remote = remotes[0]
    else:
        raise RuntimeError("bug: fresh clone has zero remotes")

    if not cand.get("version"):
        postclone_check_head(destds, remote=remote)

    if reckless is None and cfg:
        # if reckless is not explicitly given, but we operate on a
        # superdataset, query whether it has been instructed to operate
        # in a reckless mode, and inherit it for the coming clone
        reckless = cfg.get('datalad.clone.reckless', None)

    # act on --reckless=shared-...
    # must happen prior git-annex-init, where we can cheaply alter the repo
    # setup through safe re-init'ing
    if reckless and reckless.startswith('shared-'):
        lgr.debug('Reinitializing %s to enable shared access permissions', destds)
        destds.repo.call_git(['init', '--shared={}'.format(reckless[7:])])

    # In case of RIA stores we need to prepare *before* annex is called at all
    if result_props['source']['type'] == 'ria':
        postclone_preannex_cfg_ria(destds, remote=remote)

    yield from postclonecfg_annexdataset(
        destds,
        reckless,
        description,
        remote=remote)

    if checkout_gitsha and \
       dest_repo.get_hexsha(dest_repo.get_corresponding_branch()) != checkout_gitsha:
        try:
            postclone_checkout_commit(dest_repo, checkout_gitsha,
                                      remote=remote)
        except Exception as e:
            ce = CapturedException(e)
            yield get_status_dict(
                status='error',
                message=str(ce),
                exception=ce,
                **result_props,
            )

            # We were supposed to clone a particular version but failed to.
            # This is particularly pointless in case of subdatasets and
            # potentially fatal with current implementation of recursion.
            # see gh-5387
            lgr.debug("Failed to checkout %s, removing this clone attempt at %s", checkout_gitsha, dest_path)
            # TODO stringification can be removed once pathlib compatible
            # or if PY35 is no longer supported
            rmtree(str(dest_path), children_only=dest_path_existed)
            return

    # perform any post-processing that needs to know details of the clone
    # source
    if result_props['source']['type'] == 'ria':
        yield from postclonecfg_ria(destds, result_props['source'],
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

    # yield successful clone of the base dataset now, as any possible
    # subdataset clone down below will not alter the Git-state of the
    # parent
    yield get_status_dict(status='ok', **result_props)


def _generate_candidate_clone_sources(srcs: List, cfg) -> List:
    """Convert "raw" clone source specs to candidate URLs
    """
    # check for configured URL mappings, either in the given config manager
    # or in the one of the destination dataset, which is typically not existent
    # yet and the process config manager is then used effectively
    srcs = _map_urls(cfg, srcs)

    # decode all source candidate specifications
    candidate_sources = [decode_source_spec(s, cfg=cfg) for s in srcs]

    # now expand the candidate sources with additional variants of the decoded
    # giturl, while duplicating the other properties in the additional records
    # for simplicity. The hope is to overcome a few corner cases and be more
    # robust than git clone
    return [
        dict(props, giturl=s) for props in candidate_sources
        for s in _get_flexible_source_candidates(props['giturl'])
    ]



# apply patch
lgr.debug('Apply datalad-next patch to clone.py:clone_dataset')
clone_dataset.__doc__ = mod_clone.clone_dataset.__doc__
mod_clone.clone_dataset = clone_dataset
