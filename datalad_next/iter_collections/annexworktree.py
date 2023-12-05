"""Report on the content of a Git-annex repository worktree
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
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
    FileSystemItemType,
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


def content_path(item: AnnexWorktreeFileSystemItem,
                 base_path: Path,
                 link_target: bool,
                 ) -> PurePath | None:

    if item.annexobjpath:
        return \
            item.annexobjpath \
            if (base_path / item.annexobjpath).exists() \
            else None

    # if this is not an annexed file, honor `link_target`-parameter.
    if item.type == FileSystemItemType.file:
        return item.name
    elif item.type == FileSystemItemType.symlink:
        return item.link_target if link_target is True else None
    return item.link_target if link_target else item.name


def get_annex_item(data, **kwargs):
    # Reuse the work done in `iter_gitworktree`.
    # TODO: this approach is a little wasteful because we have to pass `fp` and
    #  `link_target` into `iter_gitworktree` to get the proper
    #  `GitWorkTree*`-class. But that might lead `iter_gitworktree` to open a
    #  file for us (if `fp` is `True`), which we do not use.
    all_args = {
        **data.__dict__,
        **kwargs,
    }
    if isinstance(data, GitWorktreeItem):
        return AnnexWorktreeItem(**all_args)
    elif isinstance(data, GitWorktreeFileSystemItem):
        return AnnexWorktreeFileSystemItem(**all_args)
    else:
        raise TypeError(
            'Expected GitWorktreeItem or '
            f'GitWorktreeFileSystemItem, got {type(data)}'
        )


def join_annex_info(processed_data,
                    stored_data: GitWorktreeItem | GitWorktreeFileSystemItem,
                    ):
    if processed_data is StoreOnly:
        return get_annex_item(stored_data)
    else:
        return get_annex_item(
            stored_data,
            annexkey=processed_data['key'],
            annexsize=int(processed_data['bytesize']),
            annexobjpath=PurePath(
                PurePosixPath(str(processed_data['objectpath']))
            ),
        )


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
        # TODO: we have to pass `fp` and `link_target` to `iter_gitworktree`
        #  because we want to get the attributes from `GitWorktreeFileSystemItem`.
        #  We only get those if we specify `fp=True` or `link_target=True`.
        #  However, we cannot use the file pointer that we receive from
        #  `iter_gitworktree`, because they are closed when the next item is
        #  consumed from `iter_gitworktree`. This happens before we yield our
        #  results. Therefore, we have to open the file again. This is not ideal, but
        #  it works for now. A better solution might be, for example, to factor
        #  the worktree code out and provide object-factories for
        #  `GitWorktreeFileSystemItem` and for `AnnexWorktreeFileSystemItem`.
        link_target=link_target,
        fp=fp,
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
                                git_worktree_item
                        )
                    )
                )
            ) as gaf, \
            iter_subproc(
                # get the key properties JSON-lines style
                ['git', '-C', str(path), 'annex', 'examinekey', '--json', '--batch'],
                # use only non-empty keys as input to `git annex examinekey`.
                input=route_out(
                    itemize(gaf, sep=None, keep_ends=True),
                    key_store,
                    # do not process empty key lines. Non-empty key lines
                    # are processed, but nothing needs to be stored because the
                    # processing result includes the key itself.
                    lambda key: (StoreOnly, None)
                                 if key.strip() == b''
                                 else (key, None)
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

        if not fp:
            yield from results
        else:
            # Ensure that `path` is a `Path` object
            path = Path(path)
            for item in results:
                item_path = content_path(item, path, link_target)
                if item_path:
                    with (path / item_path).open('rb') as active_fp:
                        item.fp = active_fp
                        yield item
                else:
                    item.fp = None
                    yield item
