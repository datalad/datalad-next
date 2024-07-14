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
    Iterator,
    Dict,
    Generator,
    Tuple,
)
from datasalad.itertools import (
    decode_bytes,
    itemize,
)

from datalad_next.runners import iter_git_subproc
from datalad_next.gitpathspec import GitPathSpecs
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
    ) -> "GitWorktreeFileSystemItem":
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
            path=basepath / item.path,
            link_target=link_target,
        )
        fsitem.name = item.name
        fsitem.gitsha = item.gitsha
        fsitem.gittype = item.gittype
        return fsitem


lsfiles_untracked_args = {
    None:
    ('--stage', '--cached'),
    'all':
    ('--stage', '--cached', '--exclude-standard', '--others'),
    'whole-dir':
    ('--stage', '--cached', '--exclude-standard', '--others', '--directory'),
    'no-empty-dir':
    ('--stage', '--cached', '--exclude-standard',
     '--others', '--directory', '--no-empty-directory'),
    'only':
    ('--exclude-standard', '--others'),
    'only-whole-dir':
    ('--exclude-standard', '--others', '--directory'),
    'only-no-empty-dir':
    ('--exclude-standard',
     '--others', '--directory', '--no-empty-directory'),
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
    untracked: {'all', 'whole-dir', 'no-empty-dir', 'only', 'only-whole-dir', 'only-no-empty-dir'} or None, optional
      If not ``None``, also reports on untracked work tree content.
      ``all`` reports on any untracked file; ``whole-dir`` yields a single
      report for a directory that is entirely untracked, and not individual
      untracked files in it; ``no-empty-dir`` skips any reports on
      untracked empty directories. The modes starting with 'only' offer the
      same untracked content reporting styles, but only untracked and no
      tracked content is reported. For example, 'only' is the corresponding
      mode to 'all' with no tracked content being reported.
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
    recursive: {'submodules', 'repository', 'no'}, optional
      Behavior for recursion into subdirectories of ``path``. By default
      (``repository``), all directories within the repository are reported.
      This possibly includes untracked ones (see ``untracked``), but not
      directories within submodules. With ``submodules``, the full worktree
      is reported on with recursion into submodules. With ``no``,
      only direct children of ``path`` are reported on.
      For any worktree items in subdirectories of ``path`` only a single
      record for the containing immediate subdirectory ``path`` is yielded.
      For example, with 'path/subdir/file1' and 'path/subdir/file2' there
      will only be a single item with ``name='subdir'`` and
      ``type='directory'``.
    pathspecs: optional
      Patterns used to limit results to particular paths. Any pathspecs
      supported by Git can be used and are passed to the underlying ``git
      ls-files`` queries. Pathspecs are also supported for recursive reporting
      on submodules. In such a case, the results match those of individual
      queries with analog pathspecs on the respective submodules (Git itself
      does not support pathspecs for submodule-recursive operations).  For
      example, a ``submodule`` recursion with a pathspec ``*.jpg`` will yield
      reports on all JPG files in all submodules, even though a submodule path
      itself does not match ``*.jpg``.  On the other hand, a pathspec
      ``submoddir/*.jpg`` will only report on JPG files in the submodule at
      ``submoddir/``, but on all JPG files in that submodule.
      As of version 1.5, the pathspec support for submodule recursion is
      preliminary and results should be carefully investigated.

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

    processed_submodules: set[PurePath] = set()

    # the helper takes care of talking to Git and doing recursion
    for item in _iter_gitworktree(
        path=path,
        untracked=untracked,
        # the helper cannot do submodule recursion, we do this outside,
        # so limit here
        recursive='repository' if recursive == 'submodules' else recursive,
        pathspecs=_pathspecs,
    ):
        # exclude non-submodules, or a submodule that was found at
        # the root path -- which would indicate that the submodule
        # itself it not around, only its record in the parent
        if recursive == 'submodules' \
                and item.gittype == GitTreeItemType.submodule \
                and not item.name == PurePath('.'):
            # mark as processed immediately, independent of whether anything
            # need to be reported
            processed_submodules.add(item.name)
            yield from _yield_from_submodule(
                basepath=path,
                subm=item,
                untracked=untracked,
                recursive=recursive,
                link_target=link_target,
                fp=fp,
                pathspecs=_pathspecs,
            )
            # nothing else to do here, the iter_gitworktree() called
            # dealt with this submodule completely
            continue

        # here we take care of the file system related information,
        # reading out symlinks and opening files
        if link_target or fp:
            # convert to FileSystemItem and read out any symlinks
            try:
                item = GitWorktreeFileSystemItem.from_worktreeitem(
                    path,
                    item,
                    link_target=link_target,
                )
            except FileNotFoundError:
                pass
        # try opening the file
        # _get_fp_src() returns None of there is nothing to open
        fp_src = _get_fp_src(fp, path, item)
        if fp_src is None:
            # nothing to open
            yield item
        else:
            with fp_src.open('rb') as active_fp:
                item.fp = active_fp
                yield item

    # we may need to loop over the (remaining) submodules for two reasons:
    # - with pathspecs there is a chance that a given pathspec set did not
    #   match a submodule (directly) that could have content that matches a
    #   pathspec
    # - when we are looking for untracked content only, the code above
    #   (by definition) will not have found the submodules (because they are
    #   unconditionally tracked)
    if recursive == 'submodules' and (
        (untracked and untracked.startswith('only')) or _pathspecs
    ):
        for subm in iter_submodules(
            path=path,
            pathspecs=_pathspecs,
            match_containing=True,
        ):
            if subm.name in processed_submodules:
                # we dealt with that above already
                continue
            yield from _yield_from_submodule(
                basepath=path,
                subm=subm,
                untracked=untracked,
                recursive=recursive,
                link_target=link_target,
                fp=fp,
                pathspecs=_pathspecs,
            )


def _yield_from_submodule(
    basepath: Path,
    subm: GitTreeItem,
    untracked: str | None,
    recursive: str,
    link_target: bool,
    fp: bool,
    pathspecs: GitPathSpecs,
) -> Generator[GitWorktreeItem | GitWorktreeFileSystemItem, None, None]:
    # GitTreeItem.name is a str in POSIX notation. Convert to proper type
    # to get a meaningful path on all platforms
    subm_name = PurePosixPath(subm.name)
    subm_path = basepath / subm_name
    if not subm_path.exists():
        # no point in trying to list a submodule that is not around
        return
    subm_pathspecs = pathspecs
    if pathspecs:
        # recode pathspecs to match the submodule scope
        try:
            subm_pathspecs = pathspecs.for_subdir(subm_name)
        except ValueError:
            # not a single pathspec could be translated, there is
            # no chance for a match, we can stop here
            return
    for item in iter_gitworktree(
        path=subm_path,
        untracked=untracked,
        link_target=link_target,
        fp=fp,
        recursive=recursive,
        pathspecs=subm_pathspecs,
    ):
        # recode path/name
        item.name = subm.name / item.name
        # clear any possibly cached path value
        try:
            # if there is a cache, it is an instance value
            # with the cached_property name
            del item.path
        except AttributeError:
            pass
        yield item


def _iter_gitworktree(
    path: Path,
    *,
    untracked: str | None,
    recursive: str,
    pathspecs: GitPathSpecs,
) -> Generator[GitWorktreeItem, None, None]:
    """Internal helper for iter_gitworktree() tp support recursion"""

    # perform an implicit test of whether the `untracked` mode is known
    lsfiles_args = list(lsfiles_untracked_args[untracked])

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
                pending_item[0],
                pending_item[1]['mode'] if pending_item[1] else None,
                pending_item[1]['gitsha'] if pending_item[1] else None,
            )
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
        if _pathspecs.any_match_subdir(PurePosixPath(item.name)):
            yield item
            continue

        # no match
        continue


def _get_item(
    basepath: Path,
    ipath: PurePosixPath,
    type: str | GitTreeItemType | None = None,
    gitsha: str | None = None,
) -> GitWorktreeItem:
    if isinstance(type, str):
        type = _mode_type_map[type]
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


def _git_ls_files(path: Path, *args) -> Iterator[str]:
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
        yield from itemize(
            decode_bytes(r, backslash_replace=True),
            sep='\0',
            keep_ends=False,
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
