"""Report on the content of ZIP file

The main functionality is provided by the :func:`iter_zip()` function.
"""

from __future__ import annotations

import datetime
from functools import cached_property
import time
import zipfile
from dataclasses import dataclass
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import Generator

from .utils import (
    FileSystemItem,
    FileSystemItemType,
)


@dataclass
class ZipfileItem(FileSystemItem):
    name: str

    @cached_property
    def path(self) -> PurePosixPath:
        """Returns the item name as a ``PurePosixPath`` instance

        ZIP uses POSIX paths as item identifiers from version 6.3.3 onwards.
        Not all POSIX paths are legal paths on non-POSIX file systems or
        platforms.  Therefore we cannot use a platform-dependent
        ``PurePath``-instance to address ZIP-file items, anq we use
        ``PurePosixPath``-instances instead.
        """
        return PurePosixPath(self.name)


def iter_zip(
        path: Path,
        *,
        fp: bool = False,
) -> Generator[ZipfileItem, None, None]:
    """Uses the standard library ``zipfile`` module to report on ZIP-files

    A ZIP archive can represent more or less the full bandwidth of file system
    properties, therefore reporting on archive members is implemented
    similar to :func:`~datalad_next.iter_collections.directory.iter_dir()`.
    The iterator produces an :class:`ZipfileItem` instance with standard
    information on file system elements, such as ``size``, or ``mtime``.

    Parameters
    ----------
    path: Path
      Path of the ZIP archive to report content for (iterate over).
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded or the function
      returns.

    Yields
    ------
    :class:`ZipfileItem`
      The ``name`` attribute of an item is a ``str`` with the corresponding
      archive member name (in POSIX conventions).
    """
    with zipfile.ZipFile(path, mode='r') as zip_file:
        for zip_info in zip_file.infolist():
            item = _get_zipfile_item(zip_info)
            if fp and item.type == FileSystemItemType.file:
                with zip_file.open(zip_info) as amfp:
                    item.fp = amfp
                    yield item
            else:
                yield item


def _get_zipfile_item(zip_info: zipfile.ZipInfo) -> ZipfileItem:
    return ZipfileItem(
        name=zip_info.filename,
        type=FileSystemItemType.directory if zip_info.is_dir()
        else FileSystemItemType.file,
        size=zip_info.file_size,
        mtime=time.mktime(
            datetime.datetime(*zip_info.date_time).timetuple()
        )
    )
