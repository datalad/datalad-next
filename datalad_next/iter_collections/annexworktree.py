"""Report on the content of a Git-annex repository worktree

The main functionality is provided by the :func:`iter_annexworktree()`
function.
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

from datalad_next.consts import on_windows
from datalad_next.itertools import (
    itemize,
    load_json,
    route_in,
    route_out,
    StoreOnly,
)
from datalad_next.repo_utils import has_initialized_annex
from datalad_next.runners import iter_git_subproc

from .gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree
)
from .utils import FileSystemItemType


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
    recursive: str = 'repository',
) -> Generator[AnnexWorktreeItem | AnnexWorktreeFileSystemItem, None, None]:
    """Companion to ``iter_gitworktree()`` for git-annex repositories

    This iterator wraps
    :func:`~datalad_next.iter_collections.gitworktree.iter_gitworktree`.
    For each item, it determines whether it is an annexed file. If so,
    it amends the yielded item with information on the respective
    annex key, the byte size of the key, and its (would-be) location
    in the repository's annex.

    The basic semantics of all arguments are identical to
    :func:`~datalad_next.iter_collections.gitworktree.iter_gitworktree`.
    Importantly, with ``fp=True``, an annex object is opened directly,
    if available. If not available, no attempt is made to open the associated
    symlink or pointer file.

    With ``link_target`` and ``fp`` disabled items of
    type :class:`AnnexWorktreeItem` are yielded, otherwise
    :class:`AnnexWorktreeFileSystemItem` instances are yielded. In both cases,
    ``annexkey``, ``annexsize``, and ``annnexobjpath`` properties are provided.

    .. note::
      Although ``annexobjpath`` is always set for annexed content, that does
      not imply that an object at this path actually exists. The latter will
      only be the case if the annexed content is present in the work tree,
      typically as a result of a `datalad get`- or `git annex get`-call.

    Parameters
    ----------
    path: Path
      Path of a directory in a git-annex repository to report on. This
      directory need not be the root directory of the repository, but
      must be part of the repository's work tree.
    untracked: {'all', 'whole-dir', 'no-empty-dir'} or None, optional
      If not ``None``, also reports on untracked work tree content.
      ``all`` reports on any untracked file; ``whole-dir`` yields a single
      report for a directory that is entirely untracked, and not individual
      untracked files in it; ``no-empty-dir`` skips any reports on
      untracked empty directories.
    link_target: bool, optional
      If ``True``, information matching a
      :class:`~datalad_next.iter_collections.utils.FileSystemItem`
      will be included for each yielded item, and the targets of
      any symlinks will be reported, too.
    fp: bool, optional
      If ``True``, information matching a
      :class:`~datalad_next.iter_collections.utils.FileSystemItem`
      will be included for each yielded item, but without a
      link target detection, unless ``link_target`` is given.
      Moreover, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded.
    recursive: {'repository', 'no'}, optional
      Pass on to
      :func:`~datalad_next.iter_collections.gitworktree.iter_gitworktree`,
      thereby determining which items this iterator will yield.

    Yields
    ------
    :class:`AnnexWorktreeItem` or :class:`AnnexWorktreeFileSystemItem`
      The ``name`` attribute of an item is a ``PurePath`` instance with
      the corresponding (relative) path, in platform conventions.
    """

    glsf = iter_gitworktree(
        path,
        untracked=untracked,
        link_target=False,
        fp=False,
        recursive=recursive,
    )

    if not has_initialized_annex(path):
        # this is not an annex repo.
        # we just yield the items from the gitworktree iterator.
        # we funnel them through the standard result item prep
        # function for type equality.
        # when a recursive-mode other than 'repository' will be
        # implemented, this implementation needs to be double-checked
        # to avoid decision making on submodules just based on
        # the nature of the toplevel repo.
        for item in glsf:
            yield _get_worktree_item(
                path, get_fs_info=link_target, git_item=item)
        return

    git_fileinfo_store: list[Any] = list()
    # this is a technical helper that will just store a bunch of `None`s
    # for aligning item-results between git-ls-files and git-annex-find
    _annex_git_align: list[Any] = list()

    with \
            iter_git_subproc(
                # we get the annex key for any filename
                # (or empty if not annexed)
                ['annex', 'find', '--anything', '--format=${key}\\n',
                 '--batch'],
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
                ),
                cwd=path,
            ) as gaf, \
            iter_git_subproc(
                # get the key properties JSON-lines style
                ['annex', 'examinekey', '--json', '--batch'],
                # use only non-empty keys as input to `git annex examinekey`.
                input=intersperse(
                    # Add line ending to submit the key to batch processing in
                    # `git annex examinekey`.
                    b'\n',
                    route_out(
                        itemize(
                            gaf,
                            # although we declare a specific key output format
                            # for the git-annex find call, versions of
                            # git-annex <10.20231129 on Windows will terminate
                            # lines with '\r\n' instead of '\n'. We therefore use
                            # `None` as separator, which enables `itemize()`
                            # to use either separator, i.e. '\r\n' or '\n'.
                            sep=None if on_windows else b'\n',
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
                        # are processed, but nothing needs to be stored because
                        # the processing result includes the key itself.
                        lambda key: (key if key else StoreOnly, None)
                    )
                ),
                cwd=path,
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
            try:
                item = _get_worktree_item(path, get_fs_info=True, **res)
            except FileNotFoundError:
                # there is nothing to open, yield non FS item
                item = _get_worktree_item(path, get_fs_info=False, **res)
                yield item
                continue

            # determine would file we would open
            fp_src = None
            if item.annexobjpath is not None:
                # this is an annexed file
                fp_src = item.annexobjpath
            elif item.type == FileSystemItemType.file \
                    and item.annexkey is None:
                # regular file (untracked or tracked)
                fp_src = item.name
            elif item.type == FileSystemItemType.symlink \
                    and item.annexkey is None:
                # regular symlink
                fp_src = item.name
            if fp_src is None:
                # nothing to open
                yield item
            else:
                fp_src_fullpath = path / fp_src
                if not fp_src_fullpath.exists():
                    # nothing there to open (would resolve through a symlink)
                    yield item
                else:
                    with fp_src_fullpath.open('rb') as active_fp:
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
