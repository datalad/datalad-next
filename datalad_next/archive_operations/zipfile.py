"""Archive operation handler for zipfiles"""

from __future__ import annotations

import logging
import zipfile
from contextlib import contextmanager
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import (
    Generator,
    IO,
)
from zipfile import ZipInfo

from datalad_next.config import ConfigManager
# TODO we might just want to do it in reverse:
# move the code of `iter_zip` in here and have it call
# `ZipArchiveOperations(path).__iter__()` instead.
# However, the flexibility to have `iter_zip()` behave
# differently depending on parameters (fp=True/False)
# is nice, and `__iter__()` only has `self`, such that
# any customization would need to be infused in the whole
# class. Potentially cumbersome.
from datalad_next.iter_collections.zipfile import (
    ZipfileItem,
    iter_zip,
)
from . import ArchiveOperations


lgr = logging.getLogger('datalad.ext.next.archive_operations.zipfile')


class ZipArchiveOperations(ArchiveOperations):
    """
    """
    def __init__(self,
                 location: Path,
                 *,
                 cfg: ConfigManager | None = None,
                 **kwargs):
        """
        Parameters
        ----------
        location: Path
          ZIP archive location
        cfg: ConfigManager, optional
          A config manager instance that is consulted for any supported
          configuration items
        **kwargs: dict
          Keyword arguments that are passed to zipfile.ZipFile-constructor
        """
        super().__init__(location, cfg=cfg)

        self.zipfile_kwargs = kwargs
        # Consider supporting file-like for `location`,
        # see zipfile.ZipFile(file_like_object)
        self._zipfile_path = location
        self._zipfile: zipfile.ZipFile | None = None

    @property
    def zipfile(self) -> zipfile.ZipFile:
        if self._zipfile is None:
            self._zipfile = zipfile.ZipFile(
                self._zipfile_path,
                **self.zipfile_kwargs
            )
        return self._zipfile

    def close(self) -> None:
        if self._zipfile:
            self._zipfile.close()
            self._zipfile = None

    @contextmanager
    def open(self, item: str | PurePosixPath | ZipInfo, **kwargs) -> IO:
        """Context manager, returning an open file for a member of the archive.

        The file-like object will be closed when the context-handler
        exits.

        Parameters:
        ---------
        item: str | PurePosixPath | zipfile.ZipInfo
          Name, path, or ZipInfo-instance that identifies an item in the zipfile
        kwargs: dict
          Keyword arguments that will be used for ZipFile.open()

        Returns
        -------
        IO
          A file-like object to read bytes from the item or to write bytes
          to the item.
        """
        with self.zipfile.open(_anyzipid2membername(item), **kwargs) as fp:
            yield fp

    def __contains__(self, item: str | PurePosixPath | ZipInfo) -> bool:
        try:
            self.zipfile.getinfo(_anyzipid2membername(item))
            return True
        except KeyError:
            return False

    def __iter__(self) -> Generator[ZipfileItem, None, None]:
        # if fp=True is needed, either `iter_zip()` can be used
        # directly, or `ZipArchiveOperations.open`
        yield from iter_zip(self._zipfile_path, fp=False)


def _anyzipid2membername(item: str | PurePosixPath | ZipInfo) -> str | ZipInfo:
    if isinstance(item, ZipInfo):
        return item
    elif isinstance(item, PurePosixPath):
        return item.as_posix()
    return item
