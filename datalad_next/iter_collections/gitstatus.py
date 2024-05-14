"""Report on the status of the worktree

The main functionality is provided by the :func:`iter_gitstatus` function.
"""
from __future__ import annotations

import logging
from pathlib import (
    Path,
    PurePath,
)
from typing import Generator

from datalad_next.consts import PRE_INIT_COMMIT_SHA
from datalad_next.runners import (
    CommandError,
    call_git_lines,
    iter_git_subproc,
)
from datalad_next.itertools import (
    decode_bytes,
    itemize,
)
from datalad_next.repo_utils import (
    get_worktree_head,
)

from .gitdiff import (
    GitDiffItem,
    GitDiffStatus,
    GitContainerModificationType,
    iter_gitdiff,
)
from .gitworktree import (
    GitTreeItem,
    GitTreeItemType,
    iter_gitworktree,
    iter_submodules,
    lsfiles_untracked_args,
    _git_ls_files,
)

lgr = logging.getLogger('datalad.ext.next.iter_collections.gitstatus')


def iter_gitstatus(
    path: Path,
    *,
    untracked: str | None = 'all',
    recursive: str = 'repository',
    eval_submodule_state: str = "full",
) -> Generator[GitDiffItem, None, None]:
    """
    Recursion mode 'no'

    This mode limits the reporting to immediate directory items of a given
    path. This mode is not necessarily faster than a 'repository' recursion.
    Its primary purpose is the ability to deliver a collapsed report in that
    subdirectories are treated similar to submodules -- as containers that
    maybe have modified or untracked content.

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
      untracked empty directories. Also see ``eval_submodule_state`` for
      how this parameter is applied in submodule recursion.
    recursive: {'no', 'repository', 'submodules', 'monolithic'}, optional
      Behavior for recursion into subtrees. By default (``repository``),
      all trees within the repository underneath ``path``) are reported,
      but no tree within submodules. With ``submodules``, recursion includes
      any submodule that is present. If ``no``, only direct children
      are reported on.
    eval_submodule_state: {"no", "commit", "full"}, optional
      If 'full' (default), the state of a submodule is evaluated by
      considering all modifications, with the treatment of untracked files
      determined by `untracked`. If 'commit', the modification check is
      restricted to comparing the submodule's "HEAD" commit to the one
      recorded in the superdataset. If 'no', the state of the subdataset is
      not evaluated. When a git-annex repository in adjusted mode is detected,
      the reference commit that the worktree is being compared to is the basis
      of the adjusted branch (i.e., the corresponding branch).

    Yields
    ------
    :class:`GitDiffItem`
      The ``name`` and ``prev_name`` attributes of an item are a ``str`` with
      the corresponding (relative) path, as reported by Git
      (in POSIX conventions).
    """
    path = Path(path)

    head, corresponding_head = get_worktree_head(path)
    if head is None:
        # no commit at all -> compare to an empty repo.
        head = PRE_INIT_COMMIT_SHA

    # TODO it would make sense to always (or optionally) compare against any
    # existing corresponding_head. This would make the status communicate
    # anything that has not made it into the corresponding branch yet

    common_args = dict(
        head=head,
        path=path,
        untracked=untracked,
        eval_submodule_state=eval_submodule_state,
    )

    if recursive == 'no':
        yield from _yield_dir_items(**common_args)
        return
    elif recursive == 'repository':
        yield from _yield_repo_items(**common_args)
    # TODO what we really want is a status that is not against a per-repository
    # HEAD, but against the commit that is recorded in the parent repository
    # TODO we need a name for that
    elif recursive in ('submodules', 'monolithic'):
        yield from _yield_hierarchy_items(
            recursion_mode=recursive,
            **common_args,
        )
    else:
        raise ValueError(f'unknown recursion type {recursive!r}')


#
# status generators for each mode
#

def _yield_dir_items(
    *,
    head: str | None,
    path: Path,
    untracked: str | None,
    eval_submodule_state: str,
):
    # potential container items in a directory that need content
    # investigation
    container_types = (
        GitTreeItemType.directory,
        GitTreeItemType.submodule,
    )
    if untracked == 'no':
        # no need to look at anything other than the diff report
        dir_items = {}
    else:
        # there is no recursion, avoid wasting cycles on listing individual
        # files in subdirectories
        untracked = 'whole-dir' if untracked == 'all' else untracked
        # gather all dierectory items upfront, we subtract the ones reported
        # modified later and lastly yield all untracked content from them
        dir_items = {
            str(item.name): item
            for item in iter_gitworktree(
                path,
                untracked=untracked,
                recursive='no',
            )
        }
    # diff constrained to direct children
    for item in iter_gitdiff(
        path,
        from_treeish=head,
        # to the worktree
        to_treeish=None,
        recursive='no',
        # TODO trim scope like in repo_items
        eval_submodule_state=eval_submodule_state,
    ):
        if item.status != GitDiffStatus.deletion \
                and item.gittype in container_types:
            if item.gittype == GitTreeItemType.submodule:
                # issue standard submodule container report
                _eval_submodule(path, item, eval_submodule_state)
            else:
                dir_path = path / item.path
                # this is on a directory. if it appears here, it has
                # modified content
                if dir_path.exists():
                    item.add_modification_type(
                        GitContainerModificationType.modified_content)
                    if untracked != 'no' \
                            and _path_has_untracked(path / item.path):
                        item.add_modification_type(
                            GitContainerModificationType.untracked_content)
                else:
                    # this directory is gone entirely
                    item.status = GitDiffStatus.deletion
                    item.modification_types = None
            # we dealt with this item completely
            dir_items.pop(item.name, None)
        if item.status:
            yield item

    if untracked == 'no':
        return

    # yield anything untracked, and inspect remaining containers
    for dir_item in dir_items.values():
        if dir_item.gitsha is None and dir_item.gittype is None:
            # this is untracked
            yield GitDiffItem(
                # for homgeneity for report a str-path no matter what
                name=str(dir_item.name),
                status=GitDiffStatus.other,
            )
        elif dir_item.gittype in container_types:
            # none of these containers has any modification other than
            # possibly untracked content
            item = GitDiffItem(
                # for homgeneity for report a str-path no matter what
                name=str(dir_item.name),
                # this submodule has not been detected as modified
                # per-commit, assign reported gitsha to pre and post
                # state
                gitsha=dir_item.gitsha,
                prev_gitsha=dir_item.gitsha,
                gittype=dir_item.gittype,
                # TODO others?
            )
            if item.gittype == GitTreeItemType.submodule:
                # issue standard submodule container report
                _eval_submodule(path, item, eval_submodule_state)
            else:
                # this is on a directory. if it appears here, it has
                # no modified content
                if _path_has_untracked(path / dir_item.path):
                    item.status = GitDiffStatus.modification
                    item.add_modification_type(
                        GitContainerModificationType.untracked_content)
            if item.status:
                yield item


def _yield_repo_items(
    *,
    head: str | None,
    path: Path,
    untracked: str | None,
    eval_submodule_state: str,
) -> Generator[GitDiffItem, None, None]:
    """Report status items for a single/whole repsoitory"""
    present_submodules = {
        # stringify name for speedy comparison
        # TODO double-check that comparisons are primarily with
        # GitDiffItem.name which is str
        str(item.name): item for item in iter_submodules(path)
    }
    # start with a repository-contrained diff against the worktree
    for item in iter_gitdiff(
        path,
        from_treeish=head,
        # to the worktree
        to_treeish=None,
        recursive='repository',
        # we should be able to go cheaper with the submodule evaluation here.
        # We need to redo some check for adjusted mode, and other cases anyways
        eval_submodule_state='commit'
        if eval_submodule_state == 'full' else eval_submodule_state,
    ):
        # immediately investigate any submodules that are already
        # reported modified by Git
        if item.gittype == GitTreeItemType.submodule:
            _eval_submodule(path, item, eval_submodule_state)
            # we dealt with this submodule
            present_submodules.pop(item.name, None)
        if item.status:
            yield item

    # we are not generating a recursive report for submodules, hence
    # we need to look at ALL submodules for untracked content
    # `or {}` for the case where we got no submodules, which happens
    # with `eval_submodule_state == 'no'`
    for subm_name, subm_item in (present_submodules or {}).items():
        # none of these submodules has any modification other than
        # possibly untracked content
        item = GitDiffItem(
            # for homgeneity for report a str-path no matter what
            name=str(subm_item.name),
            # this submodule has not been detected as modified
            # per-commit, assign reported gitsha to pre and post
            # state
            gitsha=subm_item.gitsha,
            prev_gitsha=subm_item.gitsha,
            gittype=subm_item.gittype,
            # TODO others?
        )
        # TODO possibly trim eval_submodule_state
        _eval_submodule(path, item, eval_submodule_state)
        if item.status:
            yield item

    if untracked == 'no':
        return

    # lastly untracked files of this repo
    yield from _yield_repo_untracked(path, untracked)


def _yield_hierarchy_items(
    *,
    head: str | None,
    path: Path,
    untracked: str | None,
    recursion_mode: str,
    eval_submodule_state: str,
) -> Generator[GitDiffItem, None, None]:
    for item in _yield_repo_items(
        head=head,
        path=path,
        untracked=untracked,
        # TODO do we need to adjust the eval mode here for the diff recmodes?
        eval_submodule_state=eval_submodule_state,
    ):
        # there is nothing else to do for any non-submodule item
        if item.gittype != GitTreeItemType.submodule:
            yield item
            continue

        # we get to see any submodule item passing through here, and can simply
        # call this function again for a subpath

        # submodule recursion
        # the .path of a GitTreeItem is always POSIX
        sm_path = path / item.path
        if recursion_mode == 'submodules':
            # in this mode, we run the submodule status against it own
            # worktree head
            sm_head, _ = get_worktree_head(sm_path)
            # because this need not cover all possible changes with respect
            # to the parent repository, we yield an item on the submodule
            # itself
            yield item
        elif recursion_mode == 'monolithic':
            # in this mode we determine the change of the submodule with
            # respect to the recorded state in the parent. This is either
            # the current gitsha, or (if git detected a committed
            # modification) the previous sha. This way, any further report
            # on changes a comprehensive from the point of view of the parent
            # repository, hence no submodule item is emitted
            sm_head = item.gitsha or item.prev_gitsha

            if GitContainerModificationType.new_commits in item.modification_types:
                # this is a submodule that has new commits compared to
                # its state in the parent dataset. We need to yield this
                # item, even if nothing else is modified, because otherwise
                # this (unsafed) changed would go unnoticed
                # https://github.com/datalad/datalad-next/issues/645
                yield item

        for i in _yield_hierarchy_items(
            head=sm_head,
            path=sm_path,
            untracked=untracked,
            # TODO here we could implement handling for a recursion-depth limit
            recursion_mode=recursion_mode,
            eval_submodule_state=eval_submodule_state,
        ):
            i.name = f'{item.name}/{i.name}'
            yield i


#
# Helpers
#


def _yield_repo_untracked(
        path: Path,
        untracked: str | None,
) -> Generator[GitDiffItem, None, None]:
    """Yield items on all untracked content in a repository"""
    if untracked is None:
        return
    for uf in _git_ls_files(
        path,
        *lsfiles_untracked_args[untracked],
    ):
        yield GitDiffItem(
            name=uf,
            status=GitDiffStatus.other,
            # it is cheap to discriminate between a directory and anything
            # else. So let's do that, but not spend the cost of deciding
            # between files and symlinks
            gittype=GitTreeItemType.directory if uf.endswith('/') else None
        )


def _path_has_untracked(path: Path) -> bool:
    """Recursively check for any untracked content (except empty dirs)"""
    if not path.exists():
        # cannot possibly have untracked
        return False
    for ut in _yield_repo_untracked(
        path,
        'no-empty-dir',
    ):
        # fast exit on the first detection
        return True
    # we need to find all submodules, regardless of mode.
    # untracked content can also be in a submodule underneath
    # a directory
    for subm in iter_submodules(path):
        if _path_has_untracked(path / subm.path):
            # fast exit on the first detection
            return True
    # only after we saw everything we can say there is nothing
    return False


def _get_submod_worktree_head(path: Path) -> tuple[bool, str | None, bool]:
    """Returns (submodule exists, SHA | None, adjusted)"""
    try:
        HEAD, corresponding_head = get_worktree_head(path)
    except ValueError:
        return False, None, False

    adjusted = corresponding_head is not None
    if adjusted:
        # this is a git-annex adjusted branch. do the comparison against
        # its basis. it is not meaningful to track the managed branch in
        # a superdataset
        HEAD = corresponding_head
    res = call_git_lines(
        ['rev-parse', '--path-format=relative', '--show-toplevel', HEAD],
        cwd=path,
    )
    assert len(res) == 2
    if res[0].startswith('..'):
        # this is not a report on a submodule at this location
        return False, None, adjusted
    else:
        return True, res[1], adjusted


def _eval_submodule(basepath, item, eval_mode) -> None:
    """In-place amend GitDiffItem submodule item

    It does nothing with ``eval_mode='no'``.
    """
    if eval_mode == 'no':
        return

    item_path = basepath / item.path

    # this is the cheapest test for the theoretical chance that a submodule
    # is present at `item_path`. This is beneficial even when we would only
    # run a single call to `git rev-parse`
    # https://github.com/datalad/datalad-next/issues/606
    if not (item_path / '.git').exists():
        return

    # get head commit, and whether a submodule is actually present,
    # and/or in adjusted mode
    subds_present, head_commit, adjusted = _get_submod_worktree_head(item_path)
    if not subds_present:
        return

    if adjusted:
        _eval_submodule_adjusted(item_path, item, head_commit, eval_mode)
    else:
        _eval_submodule_normal(item_path, item, head_commit, eval_mode)


def _eval_submodule_normal(item_path, item, head_commit, eval_mode) -> None:
    if eval_mode == 'full' and item.status is None or (
        item.modification_types
        and GitContainerModificationType.new_commits in item.modification_types
    ):
        # if new commits have been detected, the diff-implementation is
        # not able to report "modified content" at the same time, if it
        # exists. This requires a dedicated inspection, which conincidentally
        # is identical to the analysis of an adjusted mode submodule.
        return _eval_submodule_adjusted(
            item_path, item, head_commit, eval_mode)

    if item.gitsha != head_commit:
        item.status = GitDiffStatus.modification
        item.add_modification_type(GitContainerModificationType.new_commits)

    if eval_mode == 'commit':
        return

    # check for untracked content (recursively)
    if _path_has_untracked(item_path):
        item.status = GitDiffStatus.modification
        item.add_modification_type(
            GitContainerModificationType.untracked_content)


def _eval_submodule_adjusted(item_path, item, head_commit, eval_mode) -> None:
    # we cannot rely on the diff-report for a submodule in adjusted mode.
    # git would make the comparison to the adjusted branch HEAD alone.
    # this would almost always be invalid, because it is not meaningful to
    # track a commit in an adjusted branch (it goes away).
    #
    # instead, we need to:
    # - check for a change in the corresponding HEAD to the recorded commit
    #   in the parent repository, consider any change "new commits"
    # - check for a diff of the worktree to corresponding HEAD, consider
    #   any such diff a "modified content"
    # - and lastly check for untracked content

    # start with "no modification"
    item.status = None
    item.modification_types = None

    if item.prev_gitsha != head_commit:
        item.status = GitDiffStatus.modification
        item.add_modification_type(GitContainerModificationType.new_commits)

    if eval_mode == 'commit':
        return

    if any(
        i.status is not None
        for i in iter_gitdiff(
            item_path,
            from_treeish=head_commit,
            # worktree
            to_treeish=None,
            recursive='repository',
            find_renames=None,
            find_copies=None,
            eval_submodule_state='commit',
        )
    ):
        item.status = GitDiffStatus.modification
        item.add_modification_type(
            GitContainerModificationType.modified_content)

    # check for untracked content (recursively)
    if _path_has_untracked(item_path):
        item.status = GitDiffStatus.modification
        item.add_modification_type(
            GitContainerModificationType.untracked_content)
