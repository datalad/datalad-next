import logging

from datalad.core.local.repo import repo_from_path
from datalad.support.gitrepo import GitRepo
import datalad.utils as ut


# reuse logger from -core, despite the unconventional name
lgr = logging.getLogger('datalad.gitrepo')


# mode identifiers used by Git (ls-files, ls-tree), mapped to
# type identifiers as used in command results
GIT_MODE_TYPE_MAP = {
    '100644': 'file',
    # we do not distinguish executables
    '100755': 'file',
    '040000': 'directory',
    '120000': 'symlink',
    '160000': 'dataset',
}


# this changes are taken from https://github.com/datalad/datalad/pull/6797
def gitRepo__diffstatus(self, fr, to, paths=None, untracked='all',
                       eval_submodule_state='full', _cache=None):
    """Like diff(), but reports the status of 'clean' content too.

    It supports an additional submodule evaluation state 'global'.
    If given, it will return a single 'modified'
    (vs. 'clean') state label for the entire repository, as soon as
    it can.
    """
    def _get_cache_key(label, paths, ref, untracked=None):
        return self.path, label, tuple(paths) if paths else None, \
            ref, untracked

    if _cache is None:
        _cache = {}

    if paths:
        # at this point we must normalize paths to the form that
        # Git would report them, to easy matching later on
        paths = map(ut.Path, paths)
        paths = [
            p.relative_to(self.pathobj) if p.is_absolute() else p
            for p in paths
        ]

    # TODO report more info from get_content_info() calls in return
    # value, those are cheap and possibly useful to a consumer
    # we need (at most) three calls to git
    if to is None:
        # everything we know about the worktree, including os.stat
        # for each file
        key = _get_cache_key('ci', paths, None, untracked)
        if key in _cache:
            to_state = _cache[key]
        else:
            to_state = self.get_content_info(
                paths=paths, ref=None, untracked=untracked)
            _cache[key] = to_state
        # we want Git to tell us what it considers modified and avoid
        # reimplementing logic ourselves
        key = _get_cache_key('mod', paths, None)
        if key in _cache:
            modified = _cache[key]
        else:
            modified = self._get_worktree_modifications(paths)
            _cache[key] = modified
    else:
        key = _get_cache_key('ci', paths, to)
        if key in _cache:
            to_state = _cache[key]
        else:
            to_state = self.get_content_info(paths=paths, ref=to)
            _cache[key] = to_state
        # we do not need worktree modification detection in this case
        modified = None
    # origin state
    key = _get_cache_key('ci', paths, fr)
    if key in _cache:
        from_state = _cache[key]
    else:
        if fr:
            from_state = self.get_content_info(paths=paths, ref=fr)
        else:
            # no ref means from nothing
            from_state = {}
        _cache[key] = from_state

    status = dict()
    # for all paths we know now or knew before
    for f in set(from_state).union(to_state):
        props = self._diffstatus_get_state_props(
            f,
            from_state.get(f),
            to_state.get(f),
            # are we comparing against a recorded commit or the worktree
            to is not None,
            # if we have worktree modification info, pass type(change)
            # report on, if there is any
            False if modified is None
            else modified.get(f, False),
            eval_submodule_state)
        # potential early exit in "global" eval mode
        if eval_submodule_state == 'global' and \
                props.get('state', None) not in ('clean', None):
            # any modification means globally 'modified'
            return 'modified'
        status[f] = props

    if to is not None or eval_submodule_state == 'no':
        # if we have `to` we are specifically comparing against
        # a recorded state, and this function only attempts
        # to label the state of a subdataset, not investigate
        # specifically what the changes in subdatasets are
        # this is done by a high-level command like rev-diff
        # so the comparison within this repo and the present
        # `state` label are all we need, and they are done already
        if eval_submodule_state == 'global':
            return 'clean'
        else:
            return status

    # loop over all subdatasets and look for additional modifications
    for f, st in status.items():
        f = str(f)
        if 'state' in st or not st['type'] == 'dataset':
            # no business here
            continue
        if not GitRepo.is_valid_repo(f):
            # submodule is not present, no chance for a conflict
            st['state'] = 'clean'
            continue
        # we have to recurse into the dataset and get its status
        subrepo = repo_from_path(f)
        # get the HEAD commit, or the one of the corresponding branch
        # only that one counts re super-sub relationship
        # save() syncs the corresponding branch each time
        subrepo_commit = subrepo.get_hexsha(subrepo.get_corresponding_branch())
        st['gitshasum'] = subrepo_commit
        # subdataset records must be labeled clean up to this point
        # test if current commit in subdataset deviates from what is
        # recorded in the dataset
        st['state'] = 'modified' \
            if st['prev_gitshasum'] != subrepo_commit \
            else 'clean'
        if eval_submodule_state == 'global' and st['state'] == 'modified':
            return 'modified'
        if eval_submodule_state == 'commit':
            continue
        # the recorded commit did not change, so we need to make
        # a more expensive traversal
        st['state'] = subrepo.diffstatus(
            # we can use 'HEAD' because we know that the commit
            # did not change. using 'HEAD' will facilitate
            # caching the result
            fr='HEAD',
            to=None,
            paths=None,
            untracked=untracked,
            eval_submodule_state='global',
            _cache=_cache) if st['state'] == 'clean' else 'modified'
        if eval_submodule_state == 'global' and st['state'] == 'modified':
            return 'modified'

    if eval_submodule_state == 'global':
        return 'clean'
    else:
        return status


def gitRepo___get_worktree_modifications(self, paths=None):
    """Report working tree modifications

    Parameters
    ----------
    paths : list or None
      If given, limits the query to the specified paths. To query all
      paths specify `None`, not an empty list.

    Returns
    -------
    dict
      Mapping of modified Paths to type labels from GIT_MODE_TYPE_MAP.
      Deleted paths have type `None` assigned.
    """
    # because of the way git considers smudge filters in modification
    # detection we have to consult two commands to get a full picture, see
    # https://github.com/datalad/datalad/issues/6791#issuecomment-1193145967

    # low-level code cannot handle pathobjs
    consider_paths = [str(p) for p in paths] if paths else None

    # first ask diff-files which can report typechanges. it gives a list with
    # interspersed diff info and filenames
    mod = list(self.call_git_items_(
        ['diff-files',
         # without this, diff-files would run a full status (recursively)
         # but we are at most interested in a subproject commit
         # change within the scope of this repo
         '--ignore-submodules=dirty',
         # hopefully making things faster by turning off features
         # we would not benefit from (at least for now)
         '--no-renames',
         '-z'],
        files=consider_paths, sep='\0', read_only=True))
    # convert into a mapping path to type
    modified = dict(zip(
        # paths are every other element, starting from the second
        mod[1::2],
        # mark `None` for deletions, and take mode reports otherwise
        # (for simplicity keep leading ':' in prev mode for now)
        (None if spec.endswith('D') else spec.split(' ', maxsplit=2)[:2]
         for spec in mod[::2])
    ))
    # `diff-files` cannot give us the full answer to "what is modified"
    # because it won't consider what smudge filters could do, for this
    # we need `ls-files --modified` to exclude any paths that are not
    # actually modified
    modified_files = set(
        p for p in self.call_git_items_(
            # we need not look for deleted files, diff-files did that
            ['ls-files', '-z', '-m'],
            files=consider_paths, sep='\0', read_only=True)
        # skip empty lines
        if p)
    modified = {
        # map to the current type, in case of a typechange
        # keep None for a deletion
        k: v if v is None else v[1]
        for k, v in modified.items()
        # a deletion
        if v is None
        # a typechange, strip the leading ":" for a valid comparison
        or v[0][1:] != v[1]
        # a plain modification after running possible smudge filters
        or k in modified_files
    }
    # convenience-map to type labels, leave raw mode if unrecognized
    # (which really should not happen)
    modified = {
        self.pathobj / ut.PurePosixPath(k):
        GIT_MODE_TYPE_MAP.get(v, v) for k, v in modified.items()
    }
    return modified


def gitRepo___diffstatus_get_state_props(self, f, from_state, to_state,
                                         against_commit,
                                         modified_in_worktree,
                                         eval_submodule_state):
    """Helper to determine diff properties for a single path

    Parameters
    ----------
    f : Path
    from_state : dict or None
    to_state : dict or None
    against_commit : bool
      Flag whether `to_state` reflects a commit or the worktree.
    modified_in_worktree : False or str
      False if there is no modification of `f` in the working tree,
      or a type label from GIT_MODE_TYPE_MAP indicating the type
      of the modified path `f`. This is ignored
      when `against_commit` is True.
    eval_submodule_state : {'commit', 'no', ...}
    """
    if against_commit:
        # we can ignore any worktree modification reported when
        # comparing against a commit
        modified_in_worktree = False

    # determine the state of `f` from from_state and to_state records, if
    # it can be determined conclusively from it. If not, it will
    # stay None for now
    state = self._diffstatus_get_state(
        f, from_state, to_state,
        modified_in_worktree,
        against_commit, eval_submodule_state,
    )

    # compile properties of the diff state
    # TODO sort out
    props = {}

    to_sha = to_state.get('gitshasum') if to_state else None
    from_sha = from_state.get('gitshasum') if from_state else None

    if state in ('clean', 'added', 'modified', None):
        # assign present gitsha to any record
        # state==None can only happen for subdatasets that
        # already existed, so also assign a sha for them
        if to_sha:
            # with a typechange there would be no gitsha
            # for the new content, despite a known modification
            props['gitshasum'] = to_sha
        if 'bytesize' in to_state:
            # if we got this cheap, report it
            props['bytesize'] = to_state['bytesize']
        elif state == 'clean' and 'bytesize' in from_state:
            # no change, we can take this old size info
            props['bytesize'] = from_state['bytesize']

    if state in ('clean', 'modified', 'deleted', None):
        # assign previous gitsha to any record
        # state==None can only happen for subdatasets that
        # already existed, so also assign a sha for them
        props['prev_gitshasum'] = from_sha

    # current type reporting
    if modified_in_worktree is not False:
        # we have a report of a modified type, include if
        # not None (i.e. vanished)
        if modified_in_worktree is not None:
            props['type'] = modified_in_worktree
    elif to_state and 'type' in to_state:
        props['type'] = to_state['type']

    if state == 'modified':
        # for modifications we want to report types for both states
        props['prev_type'] = from_state['type']

    if state == 'deleted':
        # report the type that was deleted
        props['type'] = from_state['type']

    if state:
        # only report a state if we could determine any
        # outside code tests for existence of the property
        # and not (always) for the value
        props['state'] = state
    return props


def gitRepo___diffstatus_get_state(
        self, f, from_state, to_state, modified_in_worktree,
        against_commit, eval_submodule_state):
    """Determine the state of `f` from from_state and to_state records

    Parameters
    ----------
    f: Path
    from_state: dict or None
    to_state: dict or None
    modified_in_worktree: bool
    against_commit: bool
    eval_submodule_state: str
      See _diffstatus_get_state_props() for info.

    Returns
    -------
    {'untracked', 'added', 'deleted', 'modified', 'clean', None}
        If the state cannot be determined conclusively from the state
        records, None is returned
    """
    if from_state is None:
        # this is new, or rather not known to the previous state
        return 'added' if to_state.get('gitshasum') else 'untracked'
    elif to_state is None or modified_in_worktree is None:
        # now state anymore or vanished from worktree
        return 'deleted'
    # from here we know that neither to_state nor from_state are None
    elif modified_in_worktree is False \
            and to_state.get('gitshasum') == from_state.get('gitshasum'):
        # something that is seemingly unmodified,
        if not against_commit:
            # but could also be an unstaged deletion!
            try:
                f.lstat()
            except FileNotFoundError:
                return 'deleted'

        if to_state['type'] == 'dataset':
            if against_commit or eval_submodule_state == 'commit':
                # we compare against a recorded state, just based on
                # the shas we can be confident, otherwise the state
                # of a subdataset isn't fully known yet, because
                # `modified_in_worktree` will only reflect changes
                # in the commit of a subdataset without looking into
                # it for uncommitted changes. Such tests are done
                # later and based on further conditionals for
                # performance reasons
                return 'clean'
        else:
            # no change in git record, and no change on disk
            # at this point we know that the reported object ids
            # for this file are identical in the to and from
            # records, and we already checked for deletions
            return 'clean'
    else:
        # change in git record or on disk, we already confirmed that it
        # is not a deletion.
        # for subdatasets leave the 'modified' judgement to the caller
        # for supporting corner cases, such as adjusted branch
        # which require inspection of a subdataset,
        # but if we have the working tree type not match the one
        # on git-record, we know it is modified
        return 'modified' \
            if against_commit \
            or (modified_in_worktree is not False
                and to_state['type'] != modified_in_worktree) \
            or to_state['type'] != 'dataset' \
            else None


lgr.debug('Apply datalad-next patch to gitrepo.py:GitRepo.enable_remote')
GitRepo.diffstatus = gitRepo__diffstatus
lgr.debug(
    'Apply datalad-next patch to gitrepo.py:'
    'GitRepo._get_worktree_modifications')
GitRepo._get_worktree_modifications = gitRepo___get_worktree_modifications
lgr.debug('Apply datalad-next patch to gitrepo.py:GitRepo.enable_remote')
GitRepo._diffstatus_get_state_props = gitRepo___diffstatus_get_state_props
lgr.debug('Apply datalad-next patch to gitrepo.py:GitRepo.enable_remote')
GitRepo._diffstatus_get_state = gitRepo___diffstatus_get_state
