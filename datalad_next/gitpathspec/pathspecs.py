from __future__ import annotations

from collections.abc import Iterable
from itertools import chain
from pathlib import (
    PurePosixPath,
)

from .pathspec import GitPathSpec


class GitPathSpecs:
    """Convenience container for any number of pathspecs (or none)

    This class can facilitate implementing support for pathspec-constraints,
    including scenarios involving submodule recursion.

    >>> # can except a "default" argument for no pathspecs
    >>> ps = GitPathSpecs(None)
    >>> not ps
    True
    >>> ps.arglist()
    []
    >>> # deal with any number of pathspecs
    >>> ps = GitPathSpecs(['*.jpg', 'dir/*.png'])
    >>> ps.any_match_subdir(PurePosixPath('dummy'))
    True
    >>> ps.for_subdir(PurePosixPath('dir'))
    GitPathSpecs(['*.jpg', '*.png'])
    """
    def __init__(
        self,
        pathspecs: Iterable[str | GitPathSpec] | GitPathSpecs | None,
    ):
        """Pathspecs can be given as an iterable (string-form and/or
        ``GitPathSpec``), another ``GitPathSpecs`` instance, or ``None``.
        ``None``, or empty iterable indicate a 'no constraint' scenario,
        equivalent to a single ``':'`` pathspec.
        """
        self._pathspecs: list[GitPathSpec] | None = None
        if pathspecs is None:
            return
        elif isinstance(pathspecs, GitPathSpecs):
            self._pathspecs = list(pathspecs._pathspecs) \
                if pathspecs._pathspecs else None
        else:
            self._pathspecs = _normalize_gitpathspec(pathspecs)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}([{', '.join(repr(p) for p in self.arglist())}])"
    def __len__(self) -> int:
        return len(self._pathspecs) if self._pathspecs is not None else 0

    # TODO lru_cache decorator?
    # this would prevent repeated conversion cost for the usage pattern of
    # - test if we would have a match for a subdir
    # - run code with the matching pathspecs
    # without having to implement caching logic in client code
    def for_subdir(
        self,
        path: PurePosixPath,
    ) -> GitPathSpecs:
        if self._pathspecs is None:
            return self
        return GitPathSpecs(chain.from_iterable(
            ps.for_subdir(str(path))
            for ps in self._pathspecs
        ))

    def arglist(self) -> list[str]:
        """Convert pathspecs to a CLI argument list

        This list is suitable for use with any Git command that supports
        pathspecs, after a ``--`` (that disables the interpretation of further
        arguments as options).

        When no pathspecs are present an empty list is returned.
        """
        if self._pathspecs is None:
            return []
        return list(str(ps) for ps in self._pathspecs)


def _normalize_gitpathspec(
    specs: Iterable[str | GitPathSpec] | None,
) -> list[GitPathSpec] | None:
    """Normalize path specs to a plain list of GitPathSpec instances"""
    if not specs:
        return None
    else:
        return [
            ps if isinstance(ps, GitPathSpec)
            else GitPathSpec.from_pathspec_str(ps)
            for ps in specs
        ]
