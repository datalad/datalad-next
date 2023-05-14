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
class DirectoryItem(FileSystemItem):
    pass


def iter_dir(
    path: Path,
    *,
    hash: List[str] | None = None,
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
    hash: list(str), optional
      Any number of hash algorithm names (supported by the ``hashlib`` module
      of the Python standard library. If given, an item corresponding to the
      algorithm will be included in the ``hash`` property dict of each
      reported file-type item.

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
