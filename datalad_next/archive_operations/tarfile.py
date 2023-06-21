"""TAR archive operation handler"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
import tarfile
from contextlib import contextmanager
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import (
    Generator,
    IO,
)

from datalad_next.config import ConfigManager
# TODO we might just want to do it in reverse:
# move the code of `iter_tar` in here and have it call
# `TarArchiveOperations(path).__iter__()` instead.
# However, the flexibility to have `iter_tar()` behave
# differently depending on parameters (fp=True/False)
# is nice, and `__iter__()` only has `self`, such that
# any customization would need to be infused in the whole
# class. Potentially cumbersome.
from datalad_next.iter_collections.tarfile import (
    TarfileItem,
    iter_tar,
)

from . import ArchiveOperations

lgr = logging.getLogger('datalad.ext.next.archive_operations.tarfile')


class TarArchiveOperations(ArchiveOperations):
    """Handler for a TAR archive on a local file system

    Any methods that take an archive item/member name as an argument
    accept a POSIX path string, or any `PurePath` instance.
    """
    def __init__(self, location: Path, *, cfg: ConfigManager | None = None):
        """
        Parameters
        ----------
        location: Path
          TAR archive location
        cfg: ConfigManager, optional
          A config manager instance that is consulted for any supported
          configuration items
        """
        # TODO expose `mode` other kwargs of `tarfile.TarFile`
        super().__init__(location, cfg=cfg)

        # Consider supporting file-like for `location`,
        # see tarfile.open(fileobj=)
        self._tarfile_path = location
        self._tarfile = None

    @property
    def tarfile(self) -> tarfile.TarFile:
        """Returns `TarFile` instance, after creating it on-demand

        The instance is cached, and needs to be released by calling
        ``.close()`` if called outside a context manager.
        """
        if self._tarfile is None:
            self._tarfile = tarfile.open(self._tarfile_path, 'r')
        return self._tarfile

    def close(self) -> None:
        """Closes any opened TAR file handler"""
        if self._tarfile:
            self._tarfile.close()
            self._tarfile = None

    @contextmanager
    def open(self, item: str | PurePosixPath) -> Generator[IO | None]:
        """Get a file-like for a TAR archive item

        The file-like object allows to read from the archive-item specified
        by `item`.

        Parameters
        ----------
        item: str | PurePath
          The identifier must be a POSIX path string, or a `PurePath` instance.

        Returns
        -------
        IO | None
          A file-like object to read bytes from the item, if the item is a
          regular file, else `None`. (This is returned by the context manager
          that is created via the decorator `@contextmanager`.)

        Raises
        ------
        KeyError
          If no item with the name `item` can be found in the tar-archive
        """
        with self.tarfile.extractfile(_anyid2membername(item)) as fp:
            yield fp

    def __contains__(self, item: str | PurePosixPath) -> bool:
        try:
            self.tarfile.getmember(_anyid2membername(item))
            return True
        except KeyError:
            return False

    def __iter__(self) -> Generator[TarfileItem, None, None]:
        # if fp=True is needed, either `iter_tar()` can be used
        # directly, or `TarArchiveOperations.open`
        yield from iter_tar(self._tarfile_path, fp=False)


def _anyid2membername(item_id: str | PurePosixPath) -> str:
    if isinstance(item_id, PurePosixPath):
        return item_id.as_posix()
    else:
        return item_id
