"""Report on the content of a Git-annex repository worktree
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from more_itertools import intersperse
from pathlib import (
    Path,
    PurePath,
)
from typing import (
    Any,
    Generator,
)

from datalad_next.itertools import (
    itemize,
    load_json,
    route_in,
    route_out,
    StoreOnly,
)
from datalad_next.runners import iter_subproc

from .gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree
)


lgr = logging.getLogger('datalad.ext.next.iter_collections.annexworktree')


@dataclass
class AnnexWorktreeItem(GitWorktreeItem):
    annexkey: str | None = None
    annexsize: int | None = None
    # annex object path, relative to the item
    annexobjpath: PurePath | None = None

    @classmethod
    def from_gitworktreeitem(
        cls,
        item: GitWorktreeItem,
    ):
        return cls(**item.__dict__)


@dataclass
class AnnexWorktreeFileSystemItem(GitWorktreeFileSystemItem):
    annexkey: str | None = None
    annexsize: int | None = None
    # annex object path, relative to the item
    annexobjpath: PurePath | None = None


# TODO this iterator should get a filter mechanism to limit it to a single
# directory (non-recursive). This will be needed for gooey.
# unlike iter_gitworktree() we pay a larger dedicated per item cost.
# Given that the switch to iterative processing is also made for
# iter_gitworktree() we should provide the same filtering for that one
# too!
def iter_annexworktree(
        path: Path,
        *,
        untracked: str | None = 'all',
        link_target: bool = False,
        fp: bool = False,
) -> Generator[AnnexWorktreeItem | AnnexWorktreeFileSystemItem, None, None]:
    """Report work tree item of an annexed Git repository

    This iterator can be used to report on all tracked, and untracked
    non-annexed content and on the annexed content of the work tree of an
    annexed Git repository. This includes files that have been removed
    from the work tree (deleted), unless their removal has already been staged.

    For any tracked content, yielded items include type information, gitsha
    as last known to Git, and annex information, if the file is annexed. Annex
    information includes the key of the annexed item, the size of the annexed
    item in bytes, and the path where the content of an annexed item will be
    available, if it is present.

    This iterator is based on :func:`iter_gitworktree` and like this, any
    yielded item reflects the last committed or staged content, not the state
    of an unstaged modification in the work tree.

    When no reporting of link targets or file-objects are requested, items of
    type :class:`AnnexWorktreeItem` are yielded, otherwise
    :class:`AnnexWorktreeFileSystemItem` instances are yielded. In both cases,
    ``gitsha``, ``gittype``, ``annexkey``, ``annexsize``, and ``annnexobjpath``
    properties are provided. Either of ``gitsha`` and ``gittyoe`` being ``None``
    indicates untracked work tree content. Either of ``annexkey``, ``annexsize``,
    ``annexobjpath`` being ``None`` indicates non-annexed work tree content.

    .. note::
      The ``gitsha`` is not equivalent to a SHA1 hash of a file's content,
      but is the SHA-type blob identifier as reported and used by Git.

    .. note::
      Although ``annexobjpath`` is always set for annexed content, that does not
      imply that an object at this path actually exists. The latter will only
      be the case if the annexed content is present in the work tree, typically
      as a result of a `datalad get`- or `git annex get`-call.

    Parameters
    ----------
    path: Path
      Path of a directory in a Git repository to report on.
      Please see :func:`iter_gitworktree` for details.
    untracked: {'all', 'whole-dir', 'no-empty'} or `None`, optional
      Please see :func:`iter_gitworktree` for details.
    link_target: bool, optional
      If ``True`` and the item represents a symlink, the target of the symlink
      is stored in the ``link_target`` attribute of the item.
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content, if the file is either: non-annexed, or if
      the files is annexed and the content is locally available.
      This file handle will be closed automatically when the next item is
      yielded.

    Yields
    ------
    :class:`AnnexWorktreeItem` or `AnnexWorktreeFileSystemItem`
    """
    glsf = iter_gitworktree(
        path,
        untracked=untracked,
        link_target=False,
        fp=False,
    )

    git_fileinfo_store: list[Any] = list()
    # this is a technical helper that will just store a bunch of `None`s
    # for aligning item-results between git-ls-files and git-annex-find
    _annex_git_align: list[Any] = list()

    with \
            iter_subproc(
                # we get the annex key for any filename
                # (or empty if not annexed)
                ['git', '-C', str(path),
                 'annex', 'find', '--anything', '--format=${key}\n', '--batch'],
                # intersperse items with newlines to trigger a batch run
                # this avoids string operations to append newlines to items
                input=intersperse(
                    b'\n',
                    # use `GitWorktree*`-elements yielded by `iter_gitworktree`
                    # to create an `AnnexWorktreeItem` or
                    # `AnnexWorktreeFileSystemItem` object, which is stored in
                    # `git_fileinfo_store`. Yield a string representation of
                    # the path contained in the `GitWorktree*`-element yielded
                    # by `iter_gitworktree`
                    route_out(
                        glsf,
                        git_fileinfo_store,
                        lambda git_worktree_item: (
                            str(git_worktree_item.name).encode(),
                            git_worktree_item
                        )
                    )
                )
            ) as gaf, \
            iter_subproc(
                # get the key properties JSON-lines style
                ['git', '-C', str(path),
                 'annex', 'examinekey', '--json', '--batch'],
                # use only non-empty keys as input to `git annex examinekey`.
                input=intersperse(
                    # Add line ending to submit the key to batch processing in
                    # `git annex examinekey`.
                    b'\n',
                    route_out(
                        itemize(
                            gaf,
                            # git-annex changed its line-ending behavior, but we
                            # should be safe, because we declare a specific format
                            # for git-annex-find above
                            sep=b'\n',
                        ),
                        # we need this route-out solely for the purpose
                        # of maintaining a 1:1 relationship of items reported
                        # by git-ls-files and git-annex-find (merged again
                        # in the `route-in` that gives `results` below). The
                        # "store" here does not actually store anything other
                        # than`None`s (because the `key` --which is consumed by
                        # `git annex examinekey`-- is also present in the
                        # output of `git annex examinekey`).
                        _annex_git_align,
                        # do not process empty key lines. Non-empty key lines
                        # are processed, but nothing needs to be stored because the
                        # processing result includes the key itself.
                        lambda key: (key if key else StoreOnly, None)
                    )
                )
            ) as gek:

        results = route_in(
            # the following `route_in` yields processed keys for annexed
            # files and `StoreOnly` for non-annexed files. Its
            # cardinality is the same as the cardinality of
            # `iter_gitworktree`, i.e. it produces data for each element
            # yielded by `iter_gitworktree`.
            route_in(
                load_json(itemize(gek, sep=None)),
                _annex_git_align,
                # `processed` data is either `StoreOnly` or detailed
                # annex key information. we just return `process_data` as
                # result, because `join_annex_info` knows how to incorporate
                # it into an `AnnexWorktree*`-object.
                lambda processed_data, _: processed_data
            ),
            git_fileinfo_store,
            _join_annex_info,
        )

        # at this point, each item in `results` is a dict with a `git_item`
        # key that hold a `GitWorktreeItem` instance, plus additional annex
        # related keys added by join_annex_info() for annexed files
        if not fp:
            # life is simpler here, we do not need to open any files in the
            # annex, hence all processing can be based in the information
            # collected so far
            for res in results:
                yield _get_worktree_item(path, get_fs_info=link_target, **res)
            return

        # if we get here, this is about file pointers...
        # for any annexed file we need to open, we need to locate it in
        # the annex. we get `annexobjpath` in the results. this is
        # relative to `path`. We could not use the `link_target`, because
        # we might be in a managed branch without link.
        path = Path(path)
        for res in results:
            item = _get_worktree_item(path, get_fs_info=True, **res)
            annexobjpath = res.get('annexobjpath')
            if not annexobjpath:
                # this is not an annexed file
                yield item
                continue
            full_annexobjpath = path / annexobjpath
            if not full_annexobjpath.exists():
                # annexed object is not present
                yield item
                continue
            with (full_annexobjpath).open('rb') as active_fp:
                item.fp = active_fp
                yield item


def _get_worktree_item(
    base_path: Path,
    get_fs_info: bool,
    git_item: GitWorktreeItem,
    annexkey: str | None = None,
    annexsize: int | None = None,
    annexobjpath: str | None = None,
) -> AnnexWorktreeFileSystemItem | AnnexWorktreeItem:
    """Internal helper to get an item from ``_join_annex_info()`` output

    The assumption is that minimal investigations have been done
    until this helper is called. In particular, no file system inspects
    have been performed.

    Depending on whether a user requested file system information to be
    contained in the items (``get_fs_info``), either
    ``AnnexWorktreeFileSystemItem`` or ``AnnexWorktreeItem`` is returned.

    The main workhorse of this function if
    ``AnnexWorktreeFileSystemItem.from_path()``. Besides calling it,
    information is only taken from arguments and injected into the item
    instances.
    """
    # we did not do any filesystem inspection previously, so
    # do now when link_target is enabled
    item = AnnexWorktreeFileSystemItem.from_path(
        base_path / git_item.name,
        link_target=True,
    ) if get_fs_info else AnnexWorktreeItem.from_gitworktreeitem(git_item)
    # amend the AnnexWorktree* object with the available git info
    item.gitsha = git_item.gitsha
    item.gittype = git_item.gittype
    # amend the AnnexWorktree* object with the available annex info
    item.annexkey = annexkey
    item.annexsize = annexsize
    item.annexobjpath = annexobjpath
    return item


def _join_annex_info(
    processed_data,
    stored_data: GitWorktreeItem,
) -> dict:
    """Internal helper to join results from pipeline stages

    All that is happening here is that information from git and git-annex
    inquiries gets merged into a single result dict.
    """
    joined = dict(git_item=stored_data)
    if processed_data is StoreOnly:
        # this is a non-annexed item, nothing to join
        return joined
    else:
        # here processed data is a dict with properties from annex examinekey
        joined.update(
            annexkey=processed_data['key'],
            annexsize=int(processed_data['bytesize']),
            annexobjpath=PurePath(str(processed_data['objectpath'])),
        )
        return joined
