"""Handler for operations on various archive types

All handlers implement the API defined by :class:`ArchiveOperations`.

Available handlers:

.. currentmodule:: datalad_next.archive_operations
.. autosummary::
   :toctree: generated

   tarfile
"""

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
    """Base class of all archives handlers

    Any handler can be used as a context manager to adequately acquire and
    release any resources necessary to access an archive. Alternatively,
    the :func:`~ArchiveOperations.close()` method can be called, when archive
    access is no longer needed.

    In addition to the :func:`~ArchiveOperations.open()` method for accessing
    archive item content, each handler implements the standard
    ``__contains__()``, and ``__iter__()``.

    ``__contains__()`` reports whether the archive contains an items of a given
    identifier.

    ``__iter__()`` provides an iterator that yields
    :class:`~datalad_next.iter_collections.utils.FileSystemItem` instances with
    information on each archive item.
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

    def __repr__(self) -> str:
        return \
            f'{self.__class__.__name__}({self._location}, cfg={self._cfg!r})'

    @property
    def cfg(self) -> ConfigManager:
        """ConfigManager given to the constructor, or the session default"""
        if self._cfg is None:
            self._cfg = datalad.cfg
        return self._cfg

    def __enter__(self):
        """Default implementation that does nothing in particular"""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Default implementation that only calls ``.close()``"""
        self.close()
        # we have no desire to suppress exception, indicate standard
        # handling by not returning True
        return

    @contextmanager
    @abstractmethod
    def open(self, item: Any) -> Generator[IO | None]:
        """Get a file-like for an archive item

        Parameters
        ----------
        item:
          Any identifier for an archive item supported by a particular handler
        """
        raise NotImplementedError

    def close(self) -> None:
        """Default implementation for closing a archive handler

        This default implementation does nothing.
        """
        pass

    @abstractmethod
    def __contains__(self, item: Any) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Generator[FileSystemItem, None, None]:
        raise NotImplementedError
