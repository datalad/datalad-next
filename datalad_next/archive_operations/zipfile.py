"""Archive operation handler for zipfiles"""

from __future__ import annotations

import logging
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import (
    Any,
    Generator,
    IO,
)

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
from ..config import ConfigManager


lgr = logging.getLogger('datalad.ext.next.archive_operations')


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
        self._zipfile = None

    @property
    def zipfile(self) -> zipfile.ZipFile:
        if self._zipfile is None:
            self._zipfile = zipfile.ZipFile(
                self._zipfile_path,
                **self.zipfile_kwargs
            )
        return self._zipfile

    def __enter__(self):
        # trigger opening
        self.zipfile
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        # we have no desired to suppress exception, indicate standard
        # handling by not returning True
        return

    def close(self) -> None:
        if self._zipfile:
            self._zipfile.close()

    @contextmanager
    def open(self, item: Any, **kwargs) -> IO:
        """
        """
        yield self.zipfile.open(item, **kwargs)

    def __contains__(self, item: Any) -> bool:
        try:
            self.zipfile.getinfo(item)
            return True
        except KeyError:
            return False

    def __iter__(self) -> Generator[ZipfileItem, None, None]:
        # if fp=True is needed, either `iter_zip()` can be used
        # directly, or `ZipArchiveOperations.open`
        yield from iter_zip(self._zipfile_path, fp=False)
