"""Archive operation handlers"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
from abc import (
    ABC,
    abstractmethod,
)
from contextlib import contextmanager
from typing import (
    Any,
    Generator,
    IO,
)

import datalad

from ..config import ConfigManager
from ..iter_collections.utils import FileSystemItem


lgr = logging.getLogger('datalad.ext.next.archive_operations')


class ArchiveOperations(ABC):
    """
    """
    def __init__(self, location: Any, *, cfg: ConfigManager | None = None):
        """
        Parameters
        ----------
        location:
          Archive location identifier (path, URL, etc.) understood by a
          particular archive handler.
        cfg: ConfigManager, optional
          A config manager instance that implementations will consult for
          any configuration items they may support.
        """
        self._cfg = cfg

    @property
    def cfg(self) -> ConfigManager:
        if self._cfg is None:
            self._cfg = datalad.cfg
        return self._cfg

    @contextmanager
    @abstractmethod
    def open(self, item: Any) -> IO:
        """
        """
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def __contains__(self, item: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Generator[FileSystemItem, None, None]:
        raise NotImplementedError
