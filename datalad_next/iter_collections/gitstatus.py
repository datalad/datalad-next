"""Report on the status of the worktree

The main functionality is provided by the :func:`iter_gitstatus` function.
"""
from __future__ import annotations

import logging
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import Generator

from .gitdiff import (
    GitDiffItem,
    GitDiffStatus,
    GitTreeItemType,
    iter_gitdiff,
)
from .gitworktree import (
    iter_gitworktree,
    lsfiles_untracked_args,
    _git_ls_files,
)

lgr = logging.getLogger('datalad.ext.next.iter_collections.gitstatus')


def iter_gitstatus(
    path: Path,
    *,
    untracked: str | None = 'all',
    recursive: str = 'repository',
    yield_tree_items: str | None = None,
) -> Generator[GitDiffItem, None, None]:
    """
    Parameters
    ----------
    path: Path
      Path of a directory in a Git repository to report on. This directory
      need not be the root directory of the repository, but must be part of
      the repository. If the directory is not the root directory of a
      non-bare repository, the iterator is constrained to items underneath
      that directory.
    untracked: {'all', 'whole-dir', 'no-empty-dir'} or None, optional
      If not ``None``, also reports on untracked work tree content.
      ``all`` reports on any untracked file; ``whole-dir`` yields a single
      report for a directory that is entirely untracked, and not individual
      untracked files in it; ``no-empty-dir`` skips any reports on
      untracked empty directories.
    recursive: {'repository', 'submodules', 'no'}, optional
      Behavior for recursion into subtrees. By default (``repository``),
      all trees within the repository underneath ``path``) are reported,
      but no tree within submodules. With ``submodules``, recursion includes
      any submodule that is present. If ``no``, only direct children
      are reported on.
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
    path = Path(path)

    if untracked is None:
        # we can delegate all of this
        yield from iter_gitdiff(
            path,
            from_treeish='HEAD',
            # to the worktree
            to_treeish=None,
            recursive=recursive,
            yield_tree_items=yield_tree_items,
        )
        return

    # limit to within-repo, at most
    recmode = 'repository' if recursive == 'submodules' else recursive

    # we always start with a repository-contrained diff against the worktree
    # tracked content
    for item in iter_gitdiff(
        path,
        from_treeish='HEAD',
        # to the worktree
        to_treeish=None,
        recursive=recmode,
        yield_tree_items=yield_tree_items,
    ):
        # TODO when recursive==submodules, do not yield present
        # items of present submodules unless yield_tree_items says so
        yield item

    # now untracked files of this repo
    assert untracked is not None
    yield from _yield_repo_untracked(path, untracked)

    if recursive != 'submodules':
        # all other modes of recursion have been dealt with
        return

    # at this point, we know we need to recurse into submodule, and we still
    # have to report on untracked files -> scan the worktree
    for item in iter_gitworktree(
        path,
        untracked=None,
        link_target=False,
        fp=False,
        # singledir mode has been ruled out above,
        # we need to find all submodules
        recursive='repository',
    ):
        if item.gittype != GitTreeItemType.submodule \
                or item.name == PurePosixPath('.'):
            # either this is no submodule, or a submodule that was found at
            # the root path -- which would indicate that the submodule
            # itself it not around, only its record in the parent
            continue
        for i in iter_gitstatus(
            # the .path of a GitTreeItem is always POSIX
            path=path / item.path,
            untracked=untracked,
            recursive='submodules',
            yield_tree_items=yield_tree_items,
        ):
            i.name = f'{item.name}/{i.name}'
            yield i


def _yield_repo_untracked(path, untracked):
    for uf in _git_ls_files(
        path,
        *lsfiles_untracked_args[untracked],
    ):
        yield GitDiffItem(
            name=uf,
            status=GitDiffStatus.other,
        )
