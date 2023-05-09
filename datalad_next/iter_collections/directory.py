"""Report on the content of directories"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import (
    Path,
    PurePath,
)
import stat
from typing import (
    Generator,
    List
)

from datalad_next.exceptions import CapturedException

from .utils import (
    FileSystemItem,
    FileSystemItemType,
    compute_multihash_from_fp,
)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class IterdirItem(FileSystemItem):
    pass


def iterdir(
    path: Path,
    hash: List[str] | None = None,
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
    link_targets: bool, optional
      Flag whether to read and report the target path of a symbolic link.


    Yields
    ------
    :class:`IterdirItem`
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
        item = IterdirItem(
            name=PurePath(c.name),
            type=ctype,
            size=cstat.st_size,
            mode=cmode,
            mtime=cstat.st_mtime,
            hash=_compute_hash(c, hash)
            if hash and ctype == FileSystemItemType.file else None,
        )
        if ctype == FileSystemItemType.symlink:
            # could be p.readlink() from PY3.9+
            item.link_target = PurePath(os.readlink(c))
        yield item


def _compute_hash(fpath: Path, hash: List[str]):
    with fpath.open('rb') as f:
        return compute_multihash_from_fp(f, hash)
