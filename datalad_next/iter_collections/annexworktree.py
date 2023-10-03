"""Report on the content of a Git-annex repository worktree

The main functionality is provided by the :func:`iter_annexworktree()`
function.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from itertools import zip_longest
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
from typing import Generator

from datalad.support.annexrepo import GeneratorAnnexJsonNoStderrProtocol
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
    """ Iterate over the git annex worktree of a Git repository

    This iterator uses ``git ls-files``, ``git annex lookupkey``, and
    ``git annex examinekey`` to report on a git annex work tree of a Git repo.

    .. note::
      This is a POC implementation to demonstrate the concurrent use of
      run-context-managers and two batch-context-managers. Most parameters are
      ignored, the protocol that is used does not take care of pending results.

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

    # we use git-annex-examinekey for annex object location reporting.
    # we cannot use git-annex-contentlocation, because it only reports on
    # present annex objects, and here we also need to report on would-be
    # locations
    git_annex_find_cmd = [
        'git', 'annex', 'find', '--include=*',
        '--json', '--json-error-messages', '.'
    ]
    common_args = dict(cwd=path, terminate_time=3, kill_time=1)

    # We iterate over two generators, i.e. `git_ls_files` and `git_annex_find`,
    # at the same time, and associate entries that describe the same dataset
    # item by their "path"-property. Since we don't know the order in which
    # entries are yielded by the iterators, we have to save output from both
    # iterators until we get matching entries, i.e. entries with identical
    # "path"-property (the "path"-property is `item.name` if `item` is a result
    # of `git_ls_files`, and `item['file']`, if item is a result of
    # `git_annex_find`).
    gaf_store = dict()
    glf_store = dict()

    with \
            run(git_annex_find_cmd, protocol_class=GeneratorAnnexJsonNoStderrProtocol, **common_args) as git_annex_find, \
            annexjson_batchcommand(['git', 'annex', 'examinekey', '--json', '--batch'], **common_args) as examine_key:

        git_ls_files = iter_gitworktree(path=path, untracked=untracked)

        # "zip" the two iterators into 2-tuples, "shorter" iterators contribute
        # a `None`, when they are exhausted. We assume that the set of annexed
        # files is a subset of the files in git, i.e. the `git_annex_find`
        # generator yields less or equal results then the `git_ls_files`
        # generator.
        for gaf_item, glf_item in zip_longest(git_annex_find, git_ls_files):
            # Store both results (if they exist)
            if gaf_item:
                gaf_store[PurePath(PurePosixPath(gaf_item['file']))] = gaf_item
            glf_store[glf_item.name] = glf_item

            # Check the "path"-properties of all `git_ls_files`-items and
            # check for a matching path in `git_annex_find`-items. If a
            # matching pair exists, yield a result for an annexed file and
            # mark the pair for deletion.
            remove = []
            for path, glf_item in glf_store.items():
                gaf_item = gaf_store.get(path)
                if gaf_item:
                    remove.append(path)
                    key_properties = examine_key(gaf_item['key'].encode() + b'\n')
                    yield AnnexWorktreeItem(
                        name=glf_item.name,
                        gitsha=glf_item.gitsha,
                        gittype=glf_item.gittype,
                        annexkey=gaf_item['key'],
                        annexsize=int(gaf_item['bytesize']),
                        annexobjpath=PurePath(key_properties['objectpath']),
                    )

            # Delete marked pairs from both item-stores.
            for path in remove:
                del glf_store[path]
                del gaf_store[path]

        # Remaining git ls-files results are all unannexed, yield them.
        assert len(gaf_store) == 0
        for path, glf_item in glf_store.items():
            yield AnnexWorktreeItem(
                name=glf_item.name,
                gitsha=glf_item.gitsha,
                gittype=glf_item.gittype,
                annexkey=None,
                annexsize=None,
                annexobjpath=None
            )
