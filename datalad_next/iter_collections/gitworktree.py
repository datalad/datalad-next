"""Report on the content of a Git repository worktree

The main functionality is provided by the :func:`iter_gitworktree()` function.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from itertools import chain
import logging
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
import re
from typing import (
    Dict,
    Generator,
    Tuple,
)

from datalad_next.runners import (
    DEVNULL,
    LineSplitter,
    ThreadedRunner,
    StdOutCaptureGeneratorProtocol,
)

from .utils import (
    FileSystemItem,
    FileSystemItemType,
    PathBasedItem,
)

lgr = logging.getLogger('datalad.ext.next.iter_collections.gitworktree')


# TODO Could be `StrEnum`, came with PY3.11
class GitTreeItemType(Enum):
    """Enumeration of item types of Git trees
    """
    file = 'file'
    executablefile = 'executablefile'
    symlink = 'symlink'
    directory = 'directory'
    submodule = 'submodule'


# TODO maybe establish GitTreeItem and derive from that
@dataclass
class GitWorktreeItem(PathBasedItem):
    name: PurePath
    # gitsha is not the sha1 of the file content, but the output
    # of `git hash-object` which does something like
    # `printf "blob $(wc -c < "$file_name")\0$(cat "$file_name")" | sha1sum`
    gitsha: str | None = None
    gittype: GitTreeItemType | None = None


@dataclass
class GitWorktreeFileSystemItem(FileSystemItem):
    # gitsha is not the sha1 of the file content, but the output
    # of `git hash-object` which does something like
    # `printf "blob $(wc -c < "$file_name")\0$(cat "$file_name")" | sha1sum`
    gitsha: str | None = None
    gittype: GitTreeItemType | None = None


# stolen from GitRepo.get_content_info()
_lsfiles_props_re = re.compile(
    r'(?P<mode>[0-9]+) (?P<gitsha>.*) (.*)\t(?P<fname>.*)$'
)

_mode_type_map = {
    '100644': GitTreeItemType.file,
    '100755': GitTreeItemType.executablefile,
    '040000': GitTreeItemType.directory,
    '120000': GitTreeItemType.symlink,
    '160000': GitTreeItemType.submodule,
}

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
    untracked: {'all', 'whole-dir', 'no-empty'} or None, optional
      If not ``None``, also reports on untracked work tree content.
      ``all`` reports on any untracked file; ``whole-dir`` yields a single
      report for a directory that is entirely untracked, and not individual
      untracked files in it; ``no-empty-dir`` skips any reports on
      untracked empty directories. Any untracked content is yielded as
      a ``PurePosixPath``.
    link_target: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded.
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded.

    Yields
    ------
    :class:`GitWorktreeItem` or `GitWorktreeFileSystemItem`
    """
    lsfiles_args = ['--stage', '--cached']
    if untracked:
        lsfiles_args.extend(lsfiles_untracked_args[untracked])

    # helper to handle multi-stage reports by ls-files
    pending_item = None

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
        if ipath is None or (
                pending_item is not None and pending_item[0] != ipath):
            # report on a pending item, this is not a "higher-stage"
            # report by ls-files
            item = _get_item(path, link_target, fp, *pending_item)
            if fp and item.type == FileSystemItemType.file:
                with (Path(path) / item.name).open('rb') as fp:
                    item.fp = fp
                    yield item
            else:
                yield item

        if ipath is None:
            # this is the trailing `None` record. we are done here
            break

        if lsfiles_props is None:
            # when no properties were produced, this is a
            # category "other" report (i.e., untracked content)
            # the path is always relative-POSIX
            pending_item = (ipath,)
        else:
            pending_item = (
                ipath,
                _mode_type_map[lsfiles_props['mode']],
                lsfiles_props['gitsha']
            )
        # do not yield immediately, wait for a possible higher-stage
        # report in the next loop iteration


def _get_item(
    basepath: Path,
    link_target: bool,
    fp: bool,
    ipath: PurePosixPath,
    type: GitTreeItemType | None = None,
    gitsha: str | None = None,
) -> GitWorktreeItem | GitWorktreeFileSystemItem:
    if link_target or fp:
        fullpath = basepath / ipath
        item = GitWorktreeFileSystemItem.from_path(
            fullpath,
            link_target=link_target,
        )
        if type is not None:
            item.gittype = type
        if gitsha is not None:
            item.gitsha = gitsha
    else:
        item = GitWorktreeItem(
            name=ipath,
            gittype=type,
            gitsha=gitsha,
        )
    # make sure the name/id is the path relative to the basepath
    item.name = PurePath(ipath)
    return item


def _lsfiles_line2props(
    line: str
) -> Tuple[PurePosixPath, Dict[str, str] | None]:
    props = _lsfiles_props_re.match(line)
    if not props:
        # Kludge: Filter out paths starting with .git/ to work around
        # an `ls-files -o` bug that was fixed in Git 2.25.
        #
        # TODO: Drop this condition when GIT_MIN_VERSION is at least
        # 2.25.
        if line.startswith(".git/"):  # pragma nocover
            lgr.debug("Filtering out .git/ file: %s", line)
            return
        # not known to Git, but Git always reports POSIX
        path = PurePosixPath(line)
        # early exist, we have nothing but the path (untracked)
        return path, None

    # again Git reports always in POSIX
    path = PurePosixPath(props.group('fname'))
    return path, dict(
        gitsha=props.group('gitsha'),
        mode=props.group('mode'),
    )


def _git_ls_files(path, *args):
    # we use a plain runner to avoid the overhead of a GitRepo instance
    runner = ThreadedRunner(
        cmd=[
            'git', 'ls-files',
            # we rely on zero-byte splitting below
            '-z',
            # otherwise take whatever is coming in
            *args,
        ],
        protocol_class=StdOutCaptureGeneratorProtocol,
        stdin=DEVNULL,
        # run in the directory we want info on
        cwd=path,
    )
    line_splitter = LineSplitter('\0', keep_ends=False)
    # for each command output chunk received by the runner
    for content in runner.run():
        # for each zerobyte-delimited "line" in the output
        for line in line_splitter.process(content.decode('utf-8')):
            yield line
