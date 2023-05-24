"""Archive operation handlers"""

# allow for |-type UnionType declarations
from __future__ import annotations

from contextlib import contextmanager
import logging
from pathlib import Path
from typing import (
    Any,
    Dict,
    IO,
)

lgr = logging.getLogger('datalad.ext.next.archive_operations')

#
# TODO
# - add ConfigManager type annotation after
#   https://github.com/datalad/datalad-next/pull/371 is available
#


class ArchiveOperations:
    """
    """
    def __init__(self, location: Any, *, cfg=None):
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
    def cfg(self):  # -> ConfigManager
        if self._cfg is None:
            self._cfg = datalad.cfg
        return self._cfg

    @contextmanager
    def open(self, item: Any) -> IO:
        """
        """
        raise NotImplementedError

    def __contains__(self, item: Any) -> bool:
        raise NotImplementedError

    def __iter__(self) -> Generator[TarfileItem, None, None]:
        raise NotImplementedError
