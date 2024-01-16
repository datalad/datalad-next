"""Common repository operations
"""
from __future__ import annotations

from pathlib import (
    Path,
    PurePath,
)
from typing import Generator

from datalad_next.exceptions import CapturedException
from datalad_next.iter_collections.gitworktree import (
    GitTreeItem,
    GitTreeItemType,
    iter_gitworktree,
)
from datalad_next.runners import (
    CommandError,
    call_git_lines,
)


def iter_submodules(
    path: Path,
) -> Generator[GitTreeItem, None, None]:
    """Given a path, report all submodules of a repository underneath it"""
    for item in iter_gitworktree(
        path,
        untracked=None,
        link_target=False,
        fp=False,
        recursive='repository',
    ):
        # exclude non-submodules, or a submodule that was found at
        # the root path -- which would indicate that the submodule
        # itself it not around, only its record in the parent
        if item.gittype == GitTreeItemType.submodule \
                and item.name != PurePath('.'):
            yield item


def get_worktree_head(
    path: Path,
) -> tuple[str | None, str | None]:
    try:
        HEAD = call_git_lines(
            # we add the pathspec disambiguator to get cleaner error messages
            # (and we only report the first item below, to take it off again)
            ['rev-parse', '-q', '--symbolic-full-name', 'HEAD', '--'],
            cwd=path,
        )[0]
    except (NotADirectoryError, FileNotFoundError) as e:
        raise ValueError('path not found') from e
    except CommandError as e:
        CapturedException(e)
        if 'fatal: not a git repository' in e.stderr:
            raise ValueError(f'no Git repository at {path!r}') from e
        elif 'fatal: bad revision' in e.stderr:
            return (None, None)
        else:
            # no idea reraise
            raise

    if HEAD.startswith('refs/heads/adjusted/'):
        # this is a git-annex adjusted branch. do the comparison against
        # its basis. it is not meaningful to track the managed branch in
        # a superdataset
        return (
            HEAD,
            # replace 'refs/heads' with 'refs/basis'
            f'refs/basis/{HEAD[11:]}',
        )
    else:
        return (HEAD, None)
