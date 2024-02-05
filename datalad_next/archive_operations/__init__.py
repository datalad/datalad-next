"""Handler for operations on various archive types

All handlers implement the API defined by :class:`ArchiveOperations`.

Available handlers:

.. currentmodule:: datalad_next.archive_operations
.. autosummary::
   :toctree: generated

   TarArchiveOperations
   ZipArchiveOperations
"""
from .tarfile import TarArchiveOperations
from .zipfile import ZipArchiveOperations


# TODO REMOVE EVERYTHING BELOW FOR V2.0
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
# this API is not cooked enough yet to promote it for 3rd-part extensions
from .base import ArchiveOperations
import datalad
from datalad_next.config import ConfigManager
from datalad_next.iter_collections.utils import FileSystemItem


lgr = logging.getLogger('datalad.ext.next.archive_operations')
