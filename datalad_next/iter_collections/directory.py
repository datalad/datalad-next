"""Report on the content of directories

The main functionality is provided by the :func:`iter_dir()` function.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import (
    Path,
    PurePath,
)
import stat
from typing import Generator

from datalad_next.exceptions import CapturedException

from .utils import (
    FileSystemItem,
    FileSystemItemType,
)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class DirectoryItem(FileSystemItem):
    pass


def iter_dir(
    path: Path,
    *,
    fp: bool = False,
) -> Generator[DirectoryItem, None, None]:
    """Uses ``Path.iterdir()`` to iterate over a directory and reports content

    The iterator produces an :class:`DirectoryItem` instance with standard
    information on file system elements, such as ``size``, or ``mtime``.

    In addition to a plain ``Path.iterdir()`` the report includes a path-type
    label (distinguished are ``file``, ``directory``, ``symlink``). Moreover,
    any number of checksums for file content can be computed and reported.

    Parameters
    ----------
    path: Path
      Path of the directory to report content for (iterate over).
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded.

    Yields
    ------
    :class:`DirectoryItem`
    """
    for c in path.iterdir():
        # c could disappear while this is running. Example: temp files managed
        # by other processes.
        try:
            cstat = c.lstat()
        except FileNotFoundError as e:
            CapturedException(e)
            continue
        cmode = cstat.st_mode
        if stat.S_ISLNK(cmode):
            ctype = FileSystemItemType.symlink
        elif stat.S_ISDIR(cmode):
            ctype = FileSystemItemType.directory
        else:
            # the rest is a file
            # there could be fifos and sockets, etc.
            # but we do not recognize them here
            ctype = FileSystemItemType.file
        item = DirectoryItem(
            name=PurePath(c.name),
            type=ctype,
            size=cstat.st_size,
            mode=cmode,
            mtime=cstat.st_mtime,
            uid=cstat.st_uid,
            gid=cstat.st_gid,
        )
        if ctype == FileSystemItemType.symlink:
            # could be p.readlink() from PY3.9+
            item.link_target = PurePath(os.readlink(c))
        if fp and ctype == FileSystemItemType.file:
            with c.open('rb') as fp:
                item.fp = fp
                yield item
        else:
            yield item
