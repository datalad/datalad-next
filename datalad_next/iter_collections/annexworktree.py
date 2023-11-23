"""Report on the content of a Git-annex repository worktree

The main functionality is provided by the :func:`iter_annexworktree()`
function.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
from typing import Generator

from more_itertools import intersperse

from .gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree
)
from datalad_next.iterable_subprocess.iterable_subprocess import iterable_subprocess
from datalad_next.itertools import (
    itemize,
    join_with_list,
    load_json,
    route_in,
    route_out,
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

    glsf = iter_gitworktree(
        path,
        untracked=untracked,
        link_target=link_target,
        fp=fp
    )

    with \
            iterable_subprocess(
                # we get the annex key for any filename (or empty if not annexed)
                ['git', '-C', str(path), 'annex', 'find', '--anything', '--format=\${key}\n', '--batch'],
                # intersperse items with newlines to trigger a batch run
                # this avoids string operations to append newlines to items
                intersperse(
                    b'\n',
                    # store all output of the git ls-find in the gitfileinfo
                    # store
                    route_out(
                        glsf,
                        'git_fileinfo_store',
                        lambda data: (str(data.name).encode(), [data])
                    )
                ),
            ) as gaf, \
            iterable_subprocess(
                # get the key properties JSON-lines style
                ['git', '-C', str(path), 'annex', 'examinekey', '--json', '--batch'],
                # process all non-empty keys and store them in the key store,
                # skip processing of empty keys and store an ignored value in
                # the key store
                route_out(
                    itemize(gaf, sep=b'\n', keep_ends=True),
                    'key_store',
                    lambda data: (None, [None]) if data == b'\n' else (data, [data])
                )
            ) as gek:

        for item in route_in(
                route_in(
                    load_json(itemize(gek, sep=b'\n')),
                    'key_store',
                    join_with_list,
                ),
                'git_fileinfo_store',
                join_with_list,
        ):
            yield AnnexWorktreeItem(
                name=item[2].name,
                gitsha=item[2].gitsha,
                gittype=item[2].gittype,
                annexkey=item[1].decode().strip() if item[1] else None,
                annexsize=int(item[0]['bytesize']) if item[0] else None,
                annexobjpath=PurePath(PurePosixPath(str(item[0]['objectpath']))) if item[0] else None,
            )
