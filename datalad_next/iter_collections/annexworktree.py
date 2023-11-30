"""Report on the content of a Git-annex repository worktree
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from os import linesep
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
from typing import (
    Any,
    Generator,
)

from more_itertools import intersperse

from .gitworktree import (
    GitWorktreeItem,
    GitWorktreeFileSystemItem,
    iter_gitworktree
)
from datalad_next.itertools import (
    itemize,
    load_json,
    route_in,
    route_out,
    StoreOnly,
)
from datalad_next.runners import iter_subproc


lgr = logging.getLogger('datalad.ext.next.iter_collections.annexworktree')


linesep_bytes = linesep.encode()


@dataclass
class AnnexWorktreeItem(GitWorktreeItem):
    annexkey: str | None = None
    annexsize: int | None = None
    annexobjpath: PurePath | None = None


@dataclass
class AnnexWorktreeFileSystemItem(GitWorktreeFileSystemItem):
    annexkey: str | None = None
    annexsize: int | None = None
    annexobjpath: PurePath | None = None


def get_annex_item(data):
    if isinstance(data, GitWorktreeItem):
        return AnnexWorktreeItem(**data.__dict__)
    elif isinstance(data, GitWorktreeFileSystemItem):
        return AnnexWorktreeFileSystemItem(**data.__dict__)
    else:
        raise TypeError(
            'Expected GitWorktreeItem or '
            f'GitWorktreeFileSystemItem, got {type(data)}'
        )


def join_annex_info(processed_data,
                    stored_data: AnnexWorktreeItem | AnnexWorktreeFileSystemItem
                    ):
    if processed_data is StoreOnly:
        return stored_data
    else:
        if processed_data:
            stored_data.annexkey = processed_data['key']
            stored_data.annexsize = int(processed_data['bytesize'])
            stored_data.annexobjpath = PurePath(
                PurePosixPath(str(processed_data['objectpath']))
            )
        return stored_data


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

    git_fileinfo_store: list[Any] = list()
    key_store: list[Any] = list()

    with \
            iter_subproc(
                # we get the annex key for any filename (or empty if not annexed)
                ['git', '-C', str(path), 'annex', 'find', '--anything', '--format=\${key}\n', '--batch'],
                # intersperse items with newlines to trigger a batch run
                # this avoids string operations to append newlines to items
                input=intersperse(
                    b'\n',
                    # use `GitWorktree*`-elements yielded by `iter_gitworktree`
                    # to create an `AnnexWorktreeItem` or
                    # `AnnexWorktreeFileSystemItem` object, which is stored in
                    # `git_fileinfo_store`. Yield a string representation of the
                    # path contained in the `GitWorktree*`-element yielded by
                    # `iter_gitworktree`
                    route_out(
                        glsf,
                        git_fileinfo_store,
                        lambda git_worktree_item: (
                                str(git_worktree_item.name).encode(),
                                get_annex_item(git_worktree_item),
                        )
                    )
                ),
            ) as gaf, \
            iter_subproc(
                # get the key properties JSON-lines style
                ['git', '-C', str(path), 'annex', 'examinekey', '--json', '--batch'],
                # use only non-empty keys as input to `git annex examinekey`.
                input=route_out(
                    itemize(gaf, sep=linesep_bytes, keep_ends=True),
                    key_store,
                    # do not process empty key lines. Non-empty key lines
                    # are processed, but nothing needs to be stored because the
                    # processing result includes the key itself.
                    lambda key: (StoreOnly, None)
                                 if key == linesep_bytes
                                 else (key, None)
                )
            ) as gek:

        yield from route_in(
            # the following `route_in` yields processed keys for annexed
            # files and `StoreOnly` for non-annexed files. Its
            # cardinality is the same as the cardinality of
            # `iter_gitworktree`, i.e. it produces data for each element
            # yielded by `iter_gitworktree`.
            route_in(
                load_json(itemize(gek, sep=linesep_bytes)),
                key_store,
                # `processed` data is either `StoreOnly` or detailed
                # annex key information. we just return `process_data` as
                # result, because `join_annex_info` knows how to incorporate
                # it into an `AnnexWorktree*`-object.
                lambda processed_data, _: processed_data
            ),
            git_fileinfo_store,
            join_annex_info,
        )
