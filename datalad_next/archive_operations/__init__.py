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

from datalad_next.config import ConfigManager
from datalad_next.iter_collections.utils import FileSystemItem


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
        self._location = location

    def __str__(self) -> str:
        return f'{self.__class__.__name__}({self._location})'

    @property
    def cfg(self) -> ConfigManager:
        if self._cfg is None:
            self._cfg = datalad.cfg
        return self._cfg

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        # we have no desired to suppress exception, indicate standard
        # handling by not returning True
        return

    @contextmanager
    @abstractmethod
    def open(self, item: Any) -> IO:
        """
        """
        raise NotImplementedError

    def close(self) -> None:
        pass

    @abstractmethod
    def __contains__(self, item: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Generator[FileSystemItem, None, None]:
        raise NotImplementedError
