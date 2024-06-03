from __future__ import annotations

from itertools import chain
from pathlib import (
    Path,
    PurePosixPath,
)

from .pathspec import GitPathSpec


class GitPathSpecs:
    def __init__(
        self,
        pathspecs: list[str | GitPathSpec] | GitPathSpecs | None,
    ):
        self._pathspecs: list[GitPathSpec] | None = None
        if isinstance(pathspecs, GitPathSpecs):
            self._pathspecs = pathspecs._pathspecs
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
        if self._pathspecs is None:
            return []
        return list(str(ps) for ps in self._pathspecs)


def _normalize_gitpathspec(
    specs: list[str | GitPathSpec] | None,
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
