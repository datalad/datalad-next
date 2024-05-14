from __future__ import annotations

from pathlib import Path

from datalad_next.exceptions import CapturedException
from datalad_next.runners import (
    CommandError,
    call_git_lines,
)


def get_worktree_head(
    path: Path,
) -> tuple[str | None, str | None]:
    """Returns the symbolic name of the worktree `HEAD` at the given path

    Returns
    -------
    tuple
      The first item is the symbolic name of the worktree `HEAD`, or `None`
      if there is no commit.
      The second item is the symbolic name of the "corresponding branch" in
      an adjusted-mode git-annex repository, or `None`.
    """
    try:
        HEAD = call_git_lines(
            # we add the pathspec disambiguator to get cleaner error messages
            # (and we only report the first item below, to take it off again)
            ['rev-parse', '-q', '--symbolic-full-name', 'HEAD', '--'],
            cwd=path,
            # we are doing error message parsing below, fix the language
            # to avoid making it even more fragile
            force_c_locale=True,
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

