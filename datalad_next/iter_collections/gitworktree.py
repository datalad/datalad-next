"""Report on the content of a Git repository worktree

The main functionality is provided by the :func:`iter_gitworktree()` function.
"""
from __future__ import annotations

from dataclasses import dataclass
from itertools import chain
import logging
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
from typing import (
    Dict,
    Generator,
    Tuple,
)

from datalad_next.runners import iter_git_subproc
from datalad_next.itertools import (
    decode_bytes,
    itemize,
)
from datalad_next.gitpathspec import (
    GitPathSpec,
    GitPathSpecs,
)
from .utils import (
    FileSystemItem,
    FileSystemItemType,
)
from .gittree import (
    GitTreeItem,
    GitTreeItemType,
    _mode_type_map,
)

lgr = logging.getLogger('datalad.ext.next.iter_collections.gitworktree')


@dataclass
class GitWorktreeItem(GitTreeItem):
    name: PurePath


@dataclass
class GitWorktreeFileSystemItem(FileSystemItem):
    name: PurePath
    # gitsha is not the sha1 of the file content, but the output
    # of `git hash-object` which does something like
    # `printf "blob $(wc -c < "$file_name")\0$(cat "$file_name")" | sha1sum`
    gitsha: str | None = None
    gittype: GitTreeItemType | None = None

    @classmethod
    def from_worktreeitem(
        cls,
        basepath: Path,
        item: GitWorktreeItem,
        link_target: bool = True,
    ):
        """Create GitWorktreeFileSystemItem from corresponding GitWorktreeItem

        Parameters
        ----------
        basepath: Path
          Reference path to convert the ``GitWorktreeItem``'s path into a
          path on the file system.
        item: GitWorktreeItem
          Item to create matching ``GitWorktreeFileSystemItem`` for.
        link_target: bool
          Flag whether to read out a link-target for an item that is a symlink.
        """
        fsitem = GitWorktreeFileSystemItem.from_path(
            path=item.path(),
            link_target=link_target,
        )
        fsitem.name = item.name
        fsitem.gitsha = item.gitsha
        fsitem.gittype = item.gittype
        return fsitem


lsfiles_untracked_args = {
    'all':
    ('--exclude-standard', '--others'),
    'whole-dir':
    ('--exclude-standard', '--others', '--directory'),
    'no-empty-dir':
    ('--exclude-standard', '--others', '--directory', '--no-empty-directory'),
}


def iter_gitworktree(
    path: Path,
    *,
    untracked: str | None = 'all',
    link_target: bool = False,
    fp: bool = False,
    recursive: str = 'repository',
    pathspecs: list[str] | GitPathSpecs | None = None,
) -> Generator[GitWorktreeItem | GitWorktreeFileSystemItem, None, None]:
    """Uses ``git ls-files`` to report on a work tree of a Git repository

    This iterator can be used to report on all tracked, and untracked content
    of a Git repository's work tree. This includes files that have been removed
    from the work tree (deleted), unless their removal has already been staged.

    For any tracked content, yielded items include type information and gitsha
    as last known to Git. This means that such reports reflect the last
    committed or staged content, not the state of a potential unstaged
    modification in the work tree.

    When no reporting of link targets or file-objects are requested, items of
    type :class:`GitWorktreeItem` are yielded, otherwise
    :class:`GitWorktreeFileSystemItem` instances. In both cases, ``gitsha`` and
    ``gittype`` properties are provided. Either of them being ``None``
    indicates untracked work tree content.

    .. note::
      The ``gitsha`` is not equivalent to a SHA1 hash of a file's content,
      but is the SHA-type blob identifier as reported and used by Git.

    Parameters
    ----------
    path: Path
      Path of a directory in a Git repository to report on. This directory
      need not be the root directory of the repository, but must be part of
      the repository's work tree.
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
      Behavior for recursion into subdirectories of ``path``. By default
      (``repository``), all directories within the repository are reported.
      This possibly includes untracked ones (see ``untracked``), but not
      directories within submodules. If ``no``, only direct children
      of ``path`` are reported on. For any worktree items in subdirectories
      of ``path`` only a single record for the containing immediate
      subdirectory ``path`` is yielded. For example, with
      'path/subdir/file1' and 'path/subdir/file2' there will only be a
      single item with ``name='subdir'`` and ``type='directory'``.

    Yields
    ------
    :class:`GitWorktreeItem` or :class:`GitWorktreeFileSystemItem`
      The ``name`` attribute of an item is a ``PurePath`` instance with
      the corresponding (relative) path, in platform conventions.
    """
    # we force-convert to Path to prevent delayed crashing when reading from
    # the file system. The docs already ask for that, but it is easy to
    # forget/ignore and leads to non-obvious errors. Running this once is
    # a cheap safety net
    # https://github.com/datalad/datalad-next/issues/551
    path = Path(path)
    _pathspecs = GitPathSpecs(pathspecs)

    yield from _iter_gitworktree(
        path=path,
        untracked=untracked,
        # TODO factor out to a higler level -- reporting vs discovery
        link_target=link_target,
        # TODO factor out to a higler level -- reporting vs discovery
        fp=fp,
        recursive=recursive,
        pathspecs=_pathspecs,
    )


def _iter_gitworktree(
    path: Path,
    *,
    untracked: str | None,
    link_target: bool,
    fp: bool,
    recursive: str,
    pathspecs: GitPathSpecs,
) -> Generator[GitWorktreeItem | GitWorktreeFileSystemItem, None, None]:
    """Internal helper for iter_gitworktree() tp support recursion"""

    lsfiles_args = ['--stage', '--cached']
    if untracked:
        lsfiles_args.extend(lsfiles_untracked_args[untracked])

    if pathspecs:
        lsfiles_args.extend(pathspecs.arglist())

    # helper to handle multi-stage reports by ls-files
    pending_item: tuple[None | PurePosixPath, None | Dict[str, str]] = (None, None)

    reported_dirs: set[PurePosixPath] = set()
    _single_dir = recursive == 'no'

    # we add a "fake" `None` record at the end to avoid a special
    # case for submitting the last pending item after the loop.
    # otherwise the context manager handling of the file pointer
    # would lead to lots of code duplication
    for line in chain(_git_ls_files(path, *lsfiles_args), [None]):
        # a bit ugly, but we need to account for the `None` record
        # that signals the final loop iteration
        ipath, lsfiles_props = _lsfiles_line2props(line) \
            if line is not None else (None, None)

        # yield any pending item, if the current record is not an
        # addendum of it
        if ipath is None or pending_item[0] not in (None, ipath):
            if ipath is None and pending_item[0] is None:
                return
            assert pending_item[0] is not None
            # this is the last point where we can still withhold a report.
            # it is also the point where we can do this with minimal
            # impact on the rest of the logic.
            # so act on recursion setup now
            pending_item_path_parts = pending_item[0].parts
            if _single_dir and len(pending_item_path_parts) > 1:
                # this path is pointing inside a subdirectory of the
                # base directory -> ignore
                # we do reset pending_item here, although this would also
                # happen below -- it decomplexifies the conditionals
                dir_path = PurePosixPath(pending_item_path_parts[0])
                if dir_path in reported_dirs:
                    # we only yield each containing dir once, and only once
                    pending_item = (ipath, lsfiles_props)
                    continue
                item = _get_item(
                    path,
                    # the next two must be passed in order to get the
                    # full logic when to yield a GitWorktreeFileSystemItem
                    # (not just GitWorktreeItem)
                    link_target=link_target,
                    fp=fp,
                    # we know all props already
                    ipath=dir_path,
                    type=GitTreeItemType.directory,
                    gitsha=None,
                )
                yield item
                reported_dirs.add(dir_path)
                pending_item = (ipath, lsfiles_props)
                continue

            assert pending_item[0] is not None
            # report on a pending item, this is not a "higher-stage"
            # report by ls-files
            item = _get_item(
                path,
                link_target,
                fp,
                pending_item[0],
                pending_item[1]['mode'] if pending_item[1] else None,
                pending_item[1]['gitsha'] if pending_item[1] else None,
            )
            fp_src = _get_fp_src(fp, path, item)
            if fp_src is None:
                # nothing to open
                yield item
            else:
                with fp_src.open('rb') as active_fp:
                    item.fp = active_fp
                    yield item

        if ipath is None:
            # this is the trailing `None` record. we are done here
            break

        if lsfiles_props is None:
            # when no properties were produced, this is a
            # category "other" report (i.e., untracked content)
            # the path is always relative-POSIX
            pending_item = (ipath, None)
        else:
            pending_item = (ipath, lsfiles_props)
        # do not yield immediately, wait for a possible higher-stage
        # report in the next loop iteration


def iter_submodules(
    path: Path,
    *,
    pathspecs: list[str] | GitPathSpecs | None = None,
    match_containing: bool = False,
) -> Generator[GitTreeItem, None, None]:
    """Given a path, report all submodules of a repository worktree underneath

    With ``match_containing`` set to the default ``False``, this is merely a
    convenience wrapper around ``iter_gitworktree()`` that selectively reports
    on submodules. With ``match_containing=True`` and ``pathspecs`` given, the
    yielded items corresponding to submodules where the given ``pathsspecs``
    *could* match content. This includes submodules that are not available
    locally, because no actual matching of pathspecs to submodule content is
    performed -- only an evaluation of the submodule item itself.
    """
    _pathspecs = GitPathSpecs(pathspecs)
    if not _pathspecs:
        # force flag to be sensible to simplify internal logic
        match_containing = False

    for item in iter_gitworktree(
        path,
        untracked=None,
        link_target=False,
        fp=False,
        recursive='repository',
        # if we want to match submodules that contain pathspecs matches
        # we cannot give the job to Git, it won't report anything,
        # but we need to match manually below
        pathspecs=None if match_containing else _pathspecs,
    ):
        # exclude non-submodules, or a submodule that was found at
        # the root path -- which would indicate that the submodule
        # itself it not around, only its record in the parent
        if item.gittype != GitTreeItemType.submodule \
                or item.name == PurePath('.'):
            continue

        if not match_containing:
            yield item
            continue

        assert pathspecs is not None
        # does any pathspec match the "inside" of the current submodule's
        # path
        # we are using any() here to return as fast as possible.
        # theoretically, we could also port all of them and enhance
        # GitTreeItem to carry them outside, but we have no idea
        # about the outside use case here, and cannot assume the additional
        # cost is worth it
        if any(
            (ps if isinstance(ps, GitPathSpec)
             else GitPathSpec.from_pathspec_str(ps)).for_subdir(str(item.name))
            for ps in pathspecs
        ):
            yield item
            continue

        # no match
        continue


def _get_item(
    basepath: Path,
    link_target: bool,
    fp: bool,
    ipath: PurePosixPath,
    type: str | GitTreeItemType | None = None,
    gitsha: str | None = None,
) -> GitWorktreeItem | GitWorktreeFileSystemItem:
    if isinstance(type, str):
        type = _mode_type_map[type]
    item = None
    if link_target or fp:
        fullpath = basepath / ipath
        try:
            item = GitWorktreeFileSystemItem.from_path(
                fullpath,
                link_target=link_target,
            )
        except FileNotFoundError:
            pass
    if item is None:
        item = GitWorktreeItem(name=ipath)
    if type is not None:
        item.gittype = type
    if gitsha is not None:
        item.gitsha = gitsha
    # make sure the name/id is the path relative to the basepath
    item.name = PurePath(ipath)
    return item


def _lsfiles_line2props(
    line: str
) -> Tuple[PurePosixPath, Dict[str, str] | None]:
    items = line.split('\t', maxsplit=1)
    # check if we cannot possibly have a 'staged' report with mode and gitsha
    if len(items) < 2:
        # not known to Git, but Git always reports POSIX
        path = PurePosixPath(line)
        # early exist, we have nothing but the path (untracked)
        return path, None

    props = items[0].split(' ')
    if len(props) != 3:
        # not known to Git, but Git always reports POSIX
        path = PurePosixPath(line)
        # early exist, we have nothing but the path (untracked)
        return path, None

    # again Git reports always in POSIX
    path = PurePosixPath(items[1])
    return path, dict(
        gitsha=props[1],
        mode=props[0],
    )


def _git_ls_files(path, *args):
    with iter_git_subproc(
            [
                'ls-files',
                # we rely on zero-byte splitting below
                '-z',
                # otherwise take whatever is coming in
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


def _get_fp_src(
    fp: bool,
    basepath: Path,
    item: GitWorktreeItem | GitWorktreeFileSystemItem,
) -> Path | None:
    if not fp or isinstance(item, GitWorktreeItem):
        # no file pointer request, we are done
        return None

    # if we get here, this is about file pointers...
    fp_src = None
    if item.type in (FileSystemItemType.file,
                     FileSystemItemType.symlink):
        fp_src = item.name
    if fp_src is None:
        # nothing to open
        return None

    fp_src_fullpath = basepath / fp_src
    if not fp_src_fullpath.exists():
        # nothing there to open (would resolve through a symlink)
        return None

    return fp_src_fullpath
