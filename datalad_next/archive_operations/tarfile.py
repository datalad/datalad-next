"""TAR archive operation handler"""

# allow for |-type UnionType declarations
from __future__ import annotations

import logging
import tarfile
from contextlib import contextmanager
from pathlib import Path
from typing import (
    Any,
    Generator,
    IO,
)

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
from datalad_next.config import ConfigManager

lgr = logging.getLogger('datalad.ext.next.archive_operations.tarfile')


class TarArchiveOperations(ArchiveOperations):
    """
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
        if self._tarfile is None:
            self._tarfile = tarfile.open(self._tarfile_path, 'r')
        return self._tarfile

    def close(self) -> None:
        if self._tarfile:
            self._tarfile.close()
            self._tarfile = None

    @contextmanager
    def open(self, item: Any) -> IO:
        """
        """
        yield self.tarfile.extractfile(str(item))

    def __contains__(self, item: Any) -> bool:
        try:
            self.tarfile.getmember(item)
            return True
        except KeyError:
            return False

    def __iter__(self) -> Generator[TarfileItem, None, None]:
        # if fp=True is needed, either `iter_tar()` can be used
        # directly, or `TarArchiveOperations.open`
        yield from iter_tar(self._tarfile_path, fp=False)
