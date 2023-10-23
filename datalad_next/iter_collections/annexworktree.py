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
from datalad_next.runners.batch import annexjson_batchcommand
from datalad_next.runners.run import run

from .gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree,
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
    annexobjpath: PurePath | None = None


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
      This flag behaves analog to that of ``iter_gitworktree()``. In addition,
      enabling link target reporting will provide the location
      of an annex object (annexed file content) via the
      ``AnnexWorktreeItem.annexobjpath`` property. Annex object path reporting
      is supported whether or not a particular key is locally present.
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded.

    Yields
    ------
    :class:`AnnexWorktreeItem` or `AnnexWorktreeFileSystemItem`
    """
    # get a lookup mapping platform-paths to annex key and bytesize
    # for any path connected to an annex key
    annex = _get_annexinfo(path)

    # we use git-annex-examinekey for annex object location reporting.
    # we cannot use git-annex-contentlocation, because it only reports on
    # present annex objects, and here we also need to report on would-be
    # locations
    with annexjson_batchcommand(
            ['git', 'annex', 'examinekey', '--json', '--batch'],
            cwd=path,
    ) as exkey:
        for item in iter_gitworktree(
            path=path,
            untracked=untracked,
            # we relay the link_target flag to ensure homogeneous behavior
            # with iter_gitworktree(), but will also do further processing
            # below
            link_target=False,
            # we need to account for annexed files and would rather open
            # their content in the annex, so we have to do this separately
            fp=False
        ):
            ainfo = annex.get(item.name, {})
            akey = ainfo.get('key')
            aobjpath = None
            if link_target and akey:
                keyprops = exkey(bytes(akey, encoding="utf-8") + b'\n')
                aobjpath = keyprops.get('objectpath')
                if aobjpath:
                    aobjpath = PurePath(aobjpath)

            asize = ainfo.get('bytesize')
            if asize is not None:
                asize = int(asize)
            yield AnnexWorktreeItem(
                name=item.name,
                gitsha=item.gitsha,
                gittype=item.gittype,
                annexkey=ainfo.get('key'),
                annexsize=asize,
                annexobjpath=aobjpath,
            )


def _get_annexinfo(path: Path) -> dict:
    from datalad.support.annexrepo import GeneratorAnnexJsonNoStderrProtocol

    map = {
        'bytesize': lambda x: x,
        'key': lambda x: x,
    }

    with run(
        [
            'git', 'annex', 'find',
            # we want everything regardless of local availability.
            # the faster `--anything` came only in 10.20230126
            #'--anything',
            '--include=*',
            '--json', '--json-error-messages',
            '.',
        ],
        protocol_class=GeneratorAnnexJsonNoStderrProtocol,
        # run in the directory we want info on
        cwd=path,
    ) as annexfind:
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
            for r in annexfind
        }
