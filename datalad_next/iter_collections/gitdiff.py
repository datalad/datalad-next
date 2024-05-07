"""Report on the difference of two Git tree-ishes or tracked worktree content

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
from typing import Generator

from datalad_next.consts import PRE_INIT_COMMIT_SHA
from datalad_next.runners import (
    CommandError,
    iter_git_subproc,
)
from datalad_next.itertools import (
    decode_bytes,
    itemize,
)
from datalad_next.runners import (
    call_git,
    call_git_oneline,
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
    addition = 'addition'
    copy = 'copy'
    deletion = 'deletion'
    modification = 'modification'
    rename = 'rename'
    typechange = 'typechange'
    unmerged = 'unmerged'
    unknown = 'unknown'
    # this is a local addition and not defined by git
    # AKA "untracked"
    other = 'other'


_diffstatus_map = {
    'A': GitDiffStatus.addition,
    'C': GitDiffStatus.copy,
    'D': GitDiffStatus.deletion,
    'M': GitDiffStatus.modification,
    'R': GitDiffStatus.rename,
    'T': GitDiffStatus.typechange,
    'U': GitDiffStatus.unmerged,
    'X': GitDiffStatus.unknown,
    'O': GitDiffStatus.other,
}


# TODO Could be `StrEnum`, came with PY3.11
class GitContainerModificationType(Enum):
    new_commits = 'new commits'
    untracked_content = 'untracked content'
    modified_content = 'modified content'


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
    modification_types: tuple[GitContainerModificationType, ...] | None = None
    """Qualifiers for modification types of container-type
    items (directories, submodules)."""

    def __post_init__(self):
        if self.status == GitDiffStatus.addition and self.gitsha is None:
            self.add_modification_type(GitContainerModificationType.modified_content)

    @cached_property
    def prev_path(self) -> PurePosixPath | None:
        """Returns the item ``prev_name`` as a ``PurePosixPath``
        instance"""
        if self.prev_name is None:
            return None
        return PurePosixPath(self.prev_name)

    def add_modification_type(self, value: GitContainerModificationType):
        if self.modification_types is None:
            self.modification_types = (value,)
        else:
            self.modification_types = (*self.modification_types, value)


def iter_gitdiff(
    path: Path,
    from_treeish: str | None,
    to_treeish: str | None,
    *,
    recursive: str = 'repository',
    find_renames: int | None = None,
    find_copies: int | None = None,
    yield_tree_items: str | None = None,
    # TODO add documentation
    eval_submodule_state: str = 'full',
) -> Generator[GitDiffItem, None, None]:
    """Report differences between Git tree-ishes or tracked worktree content

    This function is a wrapper around the Git command ``diff-tree`` and
    ``diff-index``. Therefore most semantics also apply here.

    The main difference with respect to the Git commands are: 1) uniform
    support for non-recursive, single tree reporting (no subtrees); and
    2) support for submodule recursion.

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
    from_treeish: str or None
      Git "tree-ish" that defines the comparison reference. If ``None``,
      ``to_treeeish`` must not be ``None`` (see its documentation for
      details).
    to_treeish:
      Git "tree-ish" that defines the comparison target. If ``None``,
      ``from_treeish`` must not be ``None``, and that tree-ish will be
      compared against the worktree. (see its documentation for
      details). If ``from_treeish`` is ``None``, the given tree-ish is
      compared to its immediate parents (see ``git diff-tree`` documentation
      for details).
    recursive: {'repository', 'submodules', 'no'}, optional
      Behavior for recursion into subtrees. By default (``repository``),
      all trees within the repository underneath ``path``) are reported,
      but no tree within submodules. With ``submodules``, recursion includes
      any submodule that is present. If ``no``, only direct children
      are reported on.
    find_renames: int, optional
      If given, this defines the similarity threshold for detecting renames
      (see ``git diff-{index,tree} --find-renames``). By default, no rename
      detection is done and reported items never have the ``rename`` status.
      Instead, a renames would be reported as a deletion and an addition.
    find_copied: int, optional
      If given, this defines the similarity threshold for detecting copies
      (see ``git diff-{index,tree} --find-copies``). By default, no copy
      detection is done and reported items never have the ``copy`` status.
      Instead, a copy would be reported as addition.
      This option always implies the use of the ``--find-copies-harder``
      Git option that enables reporting of copy sources, even when they
      have not been modified in the same change. This is a very expensive
      operation for large projects, so use it with caution.
    yield_tree_items: {'submodules', 'directories', 'all', None}, optional
      Whether to yield an item on type of subtree that will also be recursed
      into. For example, a submodule item, when submodule recursion is
      enabled. When disabled, subtree items (directories, submodules)
      will still be reported whenever there is no recursion into them.
      For example, submodule items are reported when
      ``recursive='repository``, even when ``yield_tree_items=None``.

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

    # put most args in a container, we need to pass then around quite
    # a bit
    kwargs = dict(
        from_treeish=from_treeish,
        to_treeish=to_treeish,
        recursive=recursive,
        find_renames=find_renames,
        find_copies=find_copies,
        yield_tree_items=yield_tree_items,
        eval_submodule_state=eval_submodule_state,
    )

    cmd = _build_cmd(**kwargs)

    if cmd[0] == 'diff-index':
        # when we compare to the index, we need a refresh run to not have
        # something like plain mtime changes trigger modification reports
        # https://github.com/datalad/datalad-next/issues/639
        call_git([
            'update-index',
            # must come first, we recurse ourselves
            '--ignore-submodules',
            # we want to continue the refresh when the index need updating
            '-q',
            '--refresh',
        ])

    # when do we need to condense subdir reports into a single dir-report
    reported_dirs: set[str] = set()
    _single_dir = (cmd[0] == 'diff-index') and recursive == 'no'
    # diff-tree reports the compared tree when no from is given, we need
    # to skip that output below
    skip_first = (cmd[0] == 'diff-tree') and from_treeish is None
    pending_props = None
    for line in _git_diff_something(path, cmd):
        if skip_first:
            skip_first = False
            continue
        if pending_props:
            pending_props.append(line)
            if pending_props[4][0] in ('C', 'R'):
                # for copies and renames we expect a second path
                continue
            yield from _yield_diff_item(
                cwd=path,
                single_dir=_single_dir,
                spec=pending_props,
                reported_dirs=reported_dirs,
                **kwargs
            )
            pending_props = None
        elif line.startswith(':'):
            pending_props = line[1:].split(' ')
        else:  # pragma: no cover
            raise RuntimeError(
                'we should not get here, unexpected diff output')
    if pending_props:
        # flush
        yield from _yield_diff_item(
            cwd=path,
            single_dir=_single_dir,
            spec=pending_props,
            reported_dirs=reported_dirs,
            **kwargs
        )


def _build_cmd(
    *,
    from_treeish, to_treeish,
    recursive, yield_tree_items,
    find_renames, find_copies,
    eval_submodule_state,
) -> list[str]:
    # from   : to   : description
    # ---------------------------
    # HEAD   : None : compare to worktree, not with the index (diff-index)
    # HEAD~2 : HEAD : compare trees (diff-tree)
    # None   : HEAD~2 : compare tree with its parents (diff-tree)
    # None   : None : exception

    common_args: list[str] = [
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

    if eval_submodule_state == 'no':
        common_args.append('--ignore-submodules=all')
    elif eval_submodule_state == 'commit':
        common_args.append('--ignore-submodules=dirty')
    elif eval_submodule_state == 'full':
        common_args.append('--ignore-submodules=none')
    else:
        raise ValueError(
            f'unknown submodule evaluation mode {eval_submodule_state!r}')

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
            if yield_tree_items in ('all', 'directories'):
                cmd.append('-t')
        if from_treeish is None:
            cmd.append(to_treeish)
        else:
            # two tree-ishes given
            cmd.extend((from_treeish, to_treeish))

    # add disambiguation marker for pathspec.
    # even if we do not pass any, we get simpler error messages from Git
    cmd.append('--')
    return cmd


def _yield_diff_item(
        *,
        cwd: Path,
        recursive: str,
        from_treeish: str | None,
        to_treeish: str | None,
        spec: list,
        single_dir: bool,
        reported_dirs: set,
        yield_tree_items: bool,
        **kwargs
) -> Generator[GitDiffItem, None, None]:
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

    # at this point we know all about the item
    # conversion should be cheap, so let's do this here
    # and get a bit neater code for the rest of this function
    item = GitDiffItem(**props)

    if not single_dir:
        if item.gittype != GitTreeItemType.submodule:
            yield item
            return
        # this is about a present submodule
        if item.status == GitDiffStatus.modification:
            if item.gitsha is None:
                # in 'git diff-index' speak the submodule is "out-of-sync" with
                # the index: this happens when there are new commits
                item.add_modification_type(
                    GitContainerModificationType.new_commits)
            # TODO we cannot give details for other modification types.
            # depending on --ignore-submodules a range of situations
            # could be the case
            #else:
            #    # this modification means that "content" is modified
            #    item.add_modification_type(
            #        GitContainerModificationType.modified_content)
        if recursive != 'submodules' or yield_tree_items in (
                'all', 'submodules'):
            # we are instructed to yield it
            yield item
        if recursive == 'submodules':
            # I believe we need no protection against absent submodules.
            # The only way they can appear here is a reported modification.
            # The only modification that is possible with an absent submodule
            # is a deletion. And that would cause the item.gittype to be None
            # -- a condition that is caught above
            for i in iter_gitdiff(
                cwd / PurePosixPath(item.name),
                **dict(
                    kwargs,
                    # we never want to pass None here
                    # if `prev_gitsha` is None, it means that the
                    # submodule record is new, and we want its full
                    # content reported. Passing None, however,
                    # would only report the change to the current
                    # state.
                    from_treeish=item.prev_gitsha or PRE_INIT_COMMIT_SHA,
                    # when comparing the parent to the worktree, we
                    # also want to compare any children to the worktree
                    to_treeish=None if to_treeish is None else item.gitsha,
                )
            ):
                # prepend any item name with the parent items
                # name
                for attr in ('name', 'prev_name'):
                    val = getattr(i, attr)
                    if val is not None:
                        setattr(i, attr, f'{item.name}/{val}')
                yield i
        return

    name = props['name'] or props['prev_name']
    # we cannot have items that have no name whatsoever
    assert name is not None
    # we decide on mangling the actual report to be on the containing directory
    # only, or to withhold it entirely
    dname_l = name.split('/', maxsplit=1)
    if len(dname_l) < 2:
        # nothing in a subdirectory
        yield item
        return
    dname = dname_l[0]
    if dname in reported_dirs:
        # nothing else todo, we already reported
        return

    reported_dirs.add(dname)
    yield _mangle_item_for_singledir(item, dname, from_treeish, cwd)


def _mangle_item_for_singledir(item, dname, from_treeish, cwd):
    # at this point we have a change report on subdirectory content
    # we only get here when comparing `from_treeish` to the worktree.
    item.name = dname
    # non-committed change -> no SHA (this ignored the index,
    # like we do elsewhere too)
    item.gitsha = None
    item.gittype = GitTreeItemType.directory
    try:
        item.prev_gitsha = call_git_oneline(
            ['rev-parse', '-q', f'{from_treeish}:./{dname}'],
            cwd=cwd,
        )
        # if we get here, we know that the name was valid in
        # `from_treeish` too
        item.prev_name = dname
        # it would require more calls to figure out the mode and infer
        # a possible type change. For now, we do not go there
        item.prev_gittype = None
        item.status = GitDiffStatus.modification
    except CommandError:
        # the was nothing with this name in `from_treeish`, but now
        # it exists. We compare to the worktree, but not any untracked
        # content -- this means that we likely compare across multiple
        # states and the directory become tracked after `from_treeish`.
        # let's call it an addition
        item.prev_gitsha = None
        item.prev_gittype = None
        item.status = GitDiffStatus.addition

    return item


def _git_diff_something(path, args):
    with iter_git_subproc([*args], cwd=path) as r:
        yield from decode_bytes(
            itemize(
                r,
                sep=b'\0',
                keep_ends=False,
            )
        )
