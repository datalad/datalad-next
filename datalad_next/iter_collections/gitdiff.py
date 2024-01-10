"""Report on the difference of two Git tree-ishes or the worktree

The main functionality is provided by the :func:`iter_gitdiff()` function.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import cached_property
import logging
from pathlib import (
    Path,
    PurePosixPath,
)
import subprocess
from typing import Generator

from datalad_next.runners import iter_subproc
from datalad_next.itertools import (
    decode_bytes,
    itemize,
)

from .gittree import (
    GitTreeItem,
    GitTreeItemType,
    _mode_type_map,
)

lgr = logging.getLogger('datalad.ext.next.iter_collections.gitdiff')


# TODO Could be `StrEnum`, came with PY3.11
class GitDiffStatus(Enum):
    """Enumeration of statuses for diff items
    """
    addition = 'A'
    copy = 'C'
    deletion = 'D'
    modification = 'M'
    rename = 'R'
    typechange = 'T'
    unmerged = 'U'
    unknown = 'X'


_diffstatus_map = {
    'A': GitDiffStatus.addition,
    'C': GitDiffStatus.copy,
    'D': GitDiffStatus.deletion,
    'M': GitDiffStatus.modification,
    'R': GitDiffStatus.rename,
    'T': GitDiffStatus.typechange,
    'U': GitDiffStatus.unmerged,
    'X': GitDiffStatus.unknown,
}


@dataclass
class GitDiffItem(GitTreeItem):
    """``GitTreeItem`` with "previous" property values given a state comparison
    """
    prev_name: str | None = None
    prev_gitsha: str | None = None
    prev_gittype: GitTreeItemType | None = None

    status: GitDiffStatus | None = None
    percentage: int | None = None
    """This is the percentage of similarity for copy-status and
    rename-status diff items, and the percentage of dissimilarity
    for modifications."""

    @cached_property
    def prev_path(self) -> PurePosixPath:
        """Returns the item ``prev_name`` as a ``PurePosixPath``
        instance"""
        if self.prev_name:
            return PurePosixPath(self.prev_name)


def iter_gitdiff(
    path: Path,
    from_treeish: str | None,
    to_treeish: str | None,
    *,
    recursive: str = 'repository',
    find_renames: int | None = None,
    find_copies: int | None = None,
) -> Generator[GitTreeItem, None, None]:
    """
    Notes on 'no' recursion mode

    When comparing to the worktree, ``git diff-index`` always reports on
    subdirectories. For homogeneity with the report on a committed tree,
    a non-recursive mode emulation is implemented. It compresses all reports
    from a direct subdirectory into a single report on that subdirectory.
    The ``gitsha`` of that directory item will always be ``None``. Moreover,
    no type or typechange inspection, or further filesystem queries are
    performed. Therefore, ``prev_gittype`` will always be ``None``, and
    any change other than the addition of the directory will be labeled
    as a ``GitDiffStatus.modification``.

    Parameters
    ----------
    path: Path
      Path of a directory in a Git repository to report on. This directory
      need not be the root directory of the repository, but must be part of
      the repository. If the directory is not the root directory of a
      non-bare repository, the iterator is constrained to items underneath
      that directory.
    recursive: {'repository', 'no'}, optional
      Behavior for recursion into subtrees. By default (``repository``),
      all tree within the repository underneath ``path``) are reported,
      but not tree within submodules. If ``no``, only direct children
      are reported on.

    Yields
    ------
    :class:`GitDiffItem`
      The ``name`` and ``prev_name`` attributes of an item are a ``str`` with
      the corresponding (relative) path, as reported by Git
      (in POSIX conventions).
    """
    # we force-convert to Path to give us the piece of mind we want.
    # The docs already ask for that, but it is easy to
    # forget/ignore and leads to non-obvious errors. Running this once is
    # a cheap safety net
    path = Path(path)

    # from   : to   : description
    # ---------------------------
    # HEAD   : None : compare to worktree, not with the index (diff-index)
    # HEAD~2 : HEAD : compare trees (diff-tree)
    # None   : HEAD~2 : compare tree with its parents (diff-tree)
    # None   : None : exception

    common_args = [
        '--no-rename-empty',
        # ignore changes above CWD
        '--relative',
        '--raw',
        '-z',
    ]
    if find_renames is not None:
        common_args.append(f'--find-renames={find_renames}%')
    if find_copies is not None:
        common_args.append(f'--find-copies={find_copies}%')
        # if someone wants to look for copies, we actually look
        # for copies. This is expensive, but IMHO is the one
        # thing that makes this useful
        # TODO possibly we only want to enable this when
        # find_copies==100 (exact copies), based on the assumption
        # that this is cheaper than reading all file content.
        # but if that is actually true remains to be tested
        common_args.append(f'--find-copies-harder')

    if from_treeish is None and to_treeish is None:
        raise ValueError(
            'either `from_treeish` or `to_treeish` must not be None')
    elif to_treeish is None:
        cmd = ['diff-index', *common_args, from_treeish]
    else:
        # diff NOT against the working tree
        cmd = ['diff-tree', *common_args]
        if recursive == 'repository':
            cmd.append('-r')
        if from_treeish is None:
            cmd.append(to_treeish)
        else:
            # two tree-ishes given
            cmd.extend((from_treeish, to_treeish))

    # when do we need to condense subdir reports into a single dir-report
    reported_dirs = set()
    _single_dir = (cmd[0] == 'diff-index') and recursive == 'no'
    # diff-tree reports the compared tree when no from is given, we need
    # to skip that output below
    skip_first = (cmd[0] == 'diff-tree') and from_treeish is None
    pending_props = None
    for line in _git_diff_something(path, cmd):
        if skip_first:
            # RAW output format starts with hash that is being compared
            # we ignore it
            skip_first = False
            continue
        if pending_props:
            pending_props.append(line)
            if pending_props[4][0] in ('C', 'R'):
                # for copies and renames we expect a second path
                continue
            item = _get_diff_item(pending_props, _single_dir, reported_dirs,
                                  from_treeish, path)
            if item:
                yield item
            pending_props = None
        elif line.startswith(':'):
            pending_props = line[1:].split(' ')
        else:  # pragma: no cover
            raise RuntimeError(
                'we should not get here, unexpected diff output')
    if pending_props:
        item = _get_diff_item(pending_props, _single_dir, reported_dirs,
                              from_treeish, path)
        if item:
            yield item


def _get_diff_item(
        spec: list,
        single_dir: bool,
        reported_dirs: set,
        from_treeish: str | None,
        cwd: Path,
) -> GitDiffItem:
    props: dict[str, str | int | GitTreeItemType] = {}
    props.update(
        (k, _mode_type_map.get(v, None))
        for k, v in (('prev_gittype', spec[0]),
                     ('gittype', spec[1]))
    )
    props.update(
        (k, None if v == (40 * '0') else v)
        for k, v in (('prev_gitsha', spec[2]),
                     ('gitsha', spec[3]))
    )
    status = spec[4]
    props['status'] = _diffstatus_map[status[0]]
    if len(status) > 1:
        props['percentage'] = int(status[1:])

    if status == 'A':
        # this is an addition, we want `name` in the right place
        props['name'] = spec[5]
    else:
        props['prev_name'] = spec[5]
        props['name'] = spec[6] if len(spec) > 6 else spec[5]

    if not single_dir:
        return GitDiffItem(**props)

    # we decide on mangling the actual report to be on the containing directory
    # only, or to withhold it entirely
    dname_l = (props['name'] or props['prev_name']).split('/', maxsplit=1)
    if len(dname_l) < 2:
        # nothing in a subdirectory
        return GitDiffItem(**props)
    dname = dname_l[0]
    if dname in reported_dirs:
        # nothing else todo, we already reported
        return

    reported_dirs.add(dname)
    return _mangle_item_for_singledir(props, dname, from_treeish, cwd)


def _mangle_item_for_singledir(props, dname, from_treeish, cwd):
    # at this point we have a change report on subdirectory content
    # we only get here when comparing `from_treeish` to the worktree.
    props['name'] = dname
    # non-committed change -> no SHA (this ignored the index,
    # like we do elsewhere too)
    props['gitsha'] = None
    props['gittype'] = GitTreeItemType.directory
    try:
        props['prev_gitsha'] = subprocess.run(
            ['git', 'rev-parse', '-q', f'{from_treeish}:./{dname}'],
            capture_output=True,
            check=True,
            cwd=cwd,
        ).stdout.decode('utf-8').rstrip()
        # if we get here, we know that the name was valid in
        # `from_treeish` too
        props['prev_name'] = dname
        # it would require more calls to figure out the mode and infer
        # a possible type change. For now, we do not go there
        props['prev_gittype'] = None
        props['status'] = GitDiffStatus.modification
    except subprocess.CalledProcessError:
        # the was nothing with this name in `from_treeish`, but now
        # it exists. We compare to the worktree, but not any untracked
        # content -- this means that we likely compare across multiple
        # states and the directory become tracked after `from_treeish`.
        # let's call it an addition
        props['prev_gitsha'] = None
        props['prev_gittype'] = None
        props['status'] = GitDiffStatus.addition

    return GitDiffItem(**props)


def _git_diff_something(path, args):
    with iter_subproc(
            [
                'git',
                # take whatever is coming in
                *args,
            ],
            cwd=path,
    ) as r:
        yield from decode_bytes(
            itemize(
                r,
                sep=b'\0',
                keep_ends=False,
            )
        )
