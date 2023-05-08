"""Report on the content of directories"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path
import stat
from typing import Generator

from datalad_next.exceptions import CapturedException


class PathType(Enum):
    file = 'file'
    directory = 'directory'
    symlink = 'symlink'


@dataclass
class IterdirItem:
    path: Path
    type: PathType
    symlink_target: Path | None = None


def iterdir(
    path: Path,
    symlink_targets: bool = True,
) -> Generator[IterdirItem, None, None]:
    """Use ``Path.iterdir()`` to iterate over a directory and report content

    In addition to a plain ``Path.iterdir()`` the report includes a path-type
    label (distinguished are ``file``, ``directory``, ``symlink``), and
    (optionally) information on the target path of a symlink.

    Parameters
    ----------
    path: Path
      Path of the directory to report content for (iterate over).
    symlink_targets: bool, optional
      Flag whether to read and report the target path of a symbolic link.


    Yields
    ------
    :class:`IterdirItem`
    """
    # anything reported from here will be state=untracked
    # figure out the type, as far as we need it
    # right now we do not detect a subdir to be a dataset
    # vs a directory, only directories
    for c in path.iterdir():
        # c could disappear while this is running. Example: temp files managed
        # by other processes.
        try:
            cmode = c.lstat().st_mode
        except FileNotFoundError as e:
            CapturedException(e)
            continue
        if stat.S_ISLNK(cmode):
            ctype = PathType.symlink
        elif stat.S_ISDIR(cmode):
            ctype = PathType.directory
        else:
            # the rest is a file
            # there could be fifos and sockets, etc.
            # but we do not recognize them here
            ctype = PathType.file
        item = IterdirItem(
            path=c,
            type=ctype,
        )
        if ctype == PathType.symlink:
            # could be p.readlink() from PY3.9+
            item.symlink_target = Path(os.readlink(c))
        yield item
