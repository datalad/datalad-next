"""Report on the content of a Git-annex repository worktree

The main functionality is provided by the :func:`iter_annexworktree()`
function.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
from typing import (
    Generator,
)

from datalad_next.runners import (
    DEVNULL,
    ThreadedRunner,
)

from .gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
)

lgr = logging.getLogger('datalad.ext.next.iter_collections.annexworktree')


# TODO Could be `StrEnum`, came with PY3.11
class AnnexTreeItemType(Enum):
    """Enumeration of item types of Git trees
    """
    file = 'file'
    executablefile = 'executablefile'
    symlink = 'symlink'
    directory = 'directory'
    submodule = 'submodule'


@dataclass
class AnnexWorktreeItem(GitWorktreeItem):
    annexkey: str | None = None
    annexsize: int | None = None
    annexhashdir_lower: str | None = None
    annexhashdir_mixed: str | None = None


@dataclass
class AnnexWorktreeFileSystemItem(GitWorktreeFileSystemItem):
    annexkey: str | None = None
    annexsize: int | None = None


def iter_annexworktree(
    path: Path,
    *,
    untracked: str | None = 'all',
    link_target: bool = False,
    fp: bool = False,
) -> Generator[AnnexWorktreeItem | AnnexWorktreeFileSystemItem, None, None]:
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
    :class:`AnnexWorktreeItem` or `AnnexWorktreeFileSystemItem`
    """
    annex = _get_annexinfo(path)

    from datalad_next.iter_collections.gitworktree import iter_gitworktree
    for item in iter_gitworktree(
        path=path,
        untracked=untracked,
        # we expect most link targets to actually be annexed files
        # we want to handle that differently
        link_target=False,
        # we need to account for annexed files and would rather open
        # their content in the annex, so we have to do this separately
        fp=False
    ):
        ainfo = annex.get(item.name, {})
        asize = ainfo.get('bytesize')
        if asize is not None:
            asize = int(asize)
        # TODO honor link target for annex (pointer) files
        yield AnnexWorktreeItem(
            name=item.name,
            gitsha=item.gitsha,
            gittype=item.gittype,
            annexkey=ainfo.get('key'),
            annexsize=asize,
        )


def _get_annexinfo(path):
    from datalad.support.annexrepo import GeneratorAnnexJsonNoStderrProtocol
    # we use a plain runner to avoid the overhead of a GitRepo instance
    runner = ThreadedRunner(
        cmd=[
            'git', 'annex', 'find',
            # we want everything regardless of local availability.
            # the faster `--anything` came only in 10.20230126
            #'--anything',
            '--include=*',
            '--json', '--json-error-messages',
            '.',
        ],
        protocol_class=GeneratorAnnexJsonNoStderrProtocol,
        stdin=DEVNULL,
        # run in the directory we want info on
        cwd=path,
    )

    map = {
        'bytesize': lambda x: x,
        'key': lambda x: x,
        # include the hashdirs, to enable a consumer to do a
        # "have-locally" check
        'hashdirlower': lambda x: PurePath(x),
        'hashdirmixed': lambda x: PurePath(x),
    }

    return {
        # git-annex reports the file path in POSIX conventions,
        # even on windows, but the hashdir report comes in
        # platform conventions.
        # we need to go to platform conventions to match the behavior of
        # iter_gitworktree()
        PurePath(PurePosixPath(r['file'])):
        {
            k: map[k](r[k]) for k in map.keys() if k in r
        }
        for r in runner.run()
    }
