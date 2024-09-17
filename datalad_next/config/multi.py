from __future__ import annotations

from itertools import chain
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Any,
)

if TYPE_CHECKING:
    from datalad_next.config import (
        ConfigurationItem,
        ConfigurationSource,
    )


class MultiConfiguration:
    """Query different sources of configuration settings

    This is query-centered. Manipulation is supported by
    by individual configuration source implementations.
    This separation is done for two reasons. 1) Query is
    a much more frequent operation than write, and
    2) consolidating different sources for read is sensible,
    and doable, while a uniform semantics and behavior for
    write are complicated due to the inherent differences
    across sources.
    """
    def __init__(
        self,
        sources: dict[str, ConfigurationSource],
    ):
        # we keep the sources strictly separate.
        # the order here matters and represents the
        # precedence rule
        self._sources = sources

    @property
    def sources(self) -> MappingProxyType:
        return MappingProxyType(self._sources)

    def __len__(self):
        return len(self.keys())

    def __getitem__(self, key) -> ConfigurationItem:
        for s in self._sources.values():
            if key in s:
                return s[key]
        raise KeyError

    def getvalue(self, key, default: Any = None) -> Any:
        # TODO: consider on-access validation using a validator that
        # is possibly registered in another source
        # TODO: consolidate validation behavior with
        # ConfigurationSource.getvalue()
        for s in self._sources.values():
            if key in s:
                return s.getvalue(key)
        return default

    def __contains__(self, key):
        return any(key in s for s in self._sources.values())

    def keys(self) -> set[str]:
        return set(chain.from_iterable(s.keys()
                                       for s in self._sources.values()))
