"""Helpers used in the clone.py patch"""

__docformat__ = 'restructuredtext'

import logging
from pathlib import Path
import re
from os.path import expanduser
from typing import (
    Dict,
    List,
    Tuple,
)
from datalad.config import ConfigManager
from datalad.core.distributed.clone import (
    _get_tracking_source,
    _map_urls,
    decode_source_spec,
)
from datalad.dochelpers import single_or_plural
from datalad_next.datasets import (
    LegacyAnnexRepo as AnnexRepo,
    LegacyGitRepo as GitRepo,
)
from datalad_next.exceptions import (
    CapturedException,
    CommandError,
)
from datalad.support.network import (
    get_local_file_url,
    is_url,
)
from datalad.distribution.utils import _get_flexible_source_candidates
from datalad_next.utils import (
    ensure_bool,
    log_progress,
    rmtree,
)
from datalad_next.datasets import Dataset

__docformat__ = 'restructuredtext'

lgr = logging.getLogger('datalad.core.distributed.clone')


def _generate_candidate_clone_sources(
        destds: Dataset,
        srcs: List,
        cfg: ConfigManager or None) -> List:
    """Convert "raw" clone source specs to candidate URLs

    Returns
    -------
    Each item in the list is a dictionary with clone candidate properties.
    At minimum each dictionary contains a 'giturl' property, with a URL
    value suitable for passing to `git-clone`. Other properties are
    provided by `decode_source_spec()` and are documented there.
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


def _try_clone_candidates(
        *,
        destds: Dataset,
        candidate_sources: List,
        clone_opts: List,
        dest_path_existed: bool) -> Tuple:
    """Iterate over candidate URLs and attempt a clone

    Parameters
    ----------
    destds: Dataset
      The target dataset the clone should materialize at.
    candidate_sources: list
      Each value is a dict with properties, as returned by
      `_generate_candidate_clone_sources()`
    clone_opts: list
      Options to be passed on to `_try_clone_candidate()`
    dest_path_existed: bool
      Flag whether the target path existed before attempting a clone.

    Returns
    -------
    (dict or None, dict, dict or None)
      The candidate record of the last clone attempt,
      a mapping of candidate URLs to potential error messages they yielded,
      and either a dict with properties of a result that should be yielded
      before an immediate return, or None, if the processing can continue
    """
    log_progress(
        lgr.info,
        'cloneds',
        'Attempting a clone into %s', destds.path,
        unit=' candidates',
        label='Cloning',
        total=len(candidate_sources),
    )
    error_msgs = dict()  # accumulate all error messages formatted per each url
    for cand in candidate_sources:
        log_progress(
            lgr.info,
            'cloneds',
            'Attempting to clone from %s to %s', cand['giturl'], destds.path,
            update=1,
            increment=True)

        tried_url, error, fatal = _try_clone_candidate(
            destds=destds,
            cand=cand,
            clone_opts=clone_opts,
        )

        if error is not None:
            lgr.debug("Failed to clone from URL: %s (%s)",
                      tried_url, error)

            error_msgs[tried_url] = error

            # ready playing field for the next attempt
            if destds.pathobj.exists():
                lgr.debug("Wiping out unsuccessful clone attempt at: %s",
                          destds.path)
                # We must not just rmtree since it might be curdir etc
                # we should remove all files/directories under it
                # TODO stringification can be removed once patlib compatible
                # or if PY35 is no longer supported
                rmtree(destds.path, children_only=dest_path_existed)

        if fatal:
            # cancel progress bar
            log_progress(
                lgr.info,
                'cloneds',
                'Completed clone attempts for %s', destds
            )
            return cand, error_msgs, fatal

        if error is None:
            # do not bother with other sources if succeeded
            break

    log_progress(
        lgr.info,
        'cloneds',
        'Completed clone attempts for %s', destds
    )
    return cand, error_msgs, None


def _try_clone_candidate(
        *,
        destds: Dataset,
        cand: Dict,
        clone_opts: List) -> Tuple:
    """Attempt a clone from a single candidate

    destds: Dataset
      The target dataset the clone should materialize at.
    candidate_sources: list
      Each value is a dict with properties, as returned by
      `_generate_candidate_clone_sources()`
    clone_opts: list
      Options to be passed on to `_try_clone_candidate()`

    Returns
    -------
    (str, str or None, dict or None)
      The first item is the effective URL a clone was attempted from.
      The second item is `None` if the clone was successful, or an
      error message, detailing the failure for the specific URL.
      If the third item is not `None`, it must be a result dict that
      should be yielded, and no further clone attempt (even when
      other candidates remain) will be attempted.
    """
    # right now, we only know git-clone based approaches
    return _try_git_clone_candidate(
        destds=destds,
        cand=cand,
        clone_opts=clone_opts,
    )


def _try_git_clone_candidate(
        *,
        destds: Dataset,
        cand: Dict,
        clone_opts: List) -> Tuple:
    """_try_clone_candidate() using `git-clone`

    Parameters and return value behavior is as described in
    `_try_clone_candidate()`.
    """
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

        # MIH thinks this should rather use any of ce's message generating
        # methods, but kept it to avoid behavior changes
        error_msg = e

        if e_stderr and 'could not create work tree' in e_stderr.lower():
            # this cannot be fixed by trying another URL
            re_match = re.match(r".*fatal: (.*)$", e_stderr,
                                flags=re.MULTILINE | re.DOTALL)
            # existential failure
            return cand['giturl'], error_msg, dict(
                status='error',
                message=re_match.group(1).strip()
                if re_match else "stderr: " + e_stderr,
            )

        # failure for this URL
        return cand['giturl'], error_msg, None

    # success
    return cand['giturl'], None, None


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


def _check_autoenable_special_remotes(repo: AnnexRepo):
    """Check and report on misconfigured/disfunctional special remotes
    """
    srs = {True: [], False: []}  # special remotes by "autoenable" key
    remote_uuids = None  # might be necessary to discover known UUIDs

    repo_config = repo.config
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
                'You might need to enable it later manually and/or fix it up '
                'to avoid this message in the future.',
                sr_autoenable, sr_name, repo.path)
            continue

        # If it looks like a type=git special remote, make sure we have up to
        # date information. See gh-2897.
        if sr_autoenable and repo_config.get(
                "remote.{}.fetch".format(sr_name)):
            try:
                repo.fetch(remote=sr_name)
            except CommandError as exc:
                ce = CapturedException(exc)
                lgr.warning("Failed to fetch type=git special remote %s: %s",
                            sr_name, ce)

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
            repo.path
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
            repo.path,
            srs[False][0] if len(srs[False]) == 1 else "SIBLING",
        )
