"""Utilities and types for collection iterators"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import os
from pathlib import (
    Path,
    PurePath,
)
import stat
from typing import (
    Any,
    IO,
    List,
)

from datalad_next.utils.consts import COPY_BUFSIZE
from datalad_next.utils.multihash import MultiHash


# TODO Could be `StrEnum`, came with PY3.11
class FileSystemItemType(Enum):
    """Enumeration of file system path types

    The associated ``str`` values are chosen to be appropriate for
    downstream use (e.g, as type labels in DataLad result records).
    """
    file = 'file'
    directory = 'directory'
    symlink = 'symlink'
    hardlink = 'hardlink'
    specialfile = 'specialfile'


@dataclass
class NamedItem:
    name: Any


@dataclass
class TypedItem:
    type: Any


@dataclass
class PathBasedItem(NamedItem):
    # a path-identifier in an appropriate context.
    # could be a filename, a relpath, or an absolute path.
    # should match platform conventions
    name: PurePath


@dataclass  # sadly PY3.10+ only (kw_only=True)
class FileSystemItem(PathBasedItem, TypedItem):
    type: FileSystemItemType
    size: int
    mtime: float | None = None
    mode: int | None = None
    uid: int | None = None
    gid: int | None = None
    link_target: PurePath | None = None
    fp: IO | None = None

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        link_target: bool = True,
    ):
        """Populate item properties from a single `stat` and `readlink` call

        The given ``path`` must exist. The ``link_target`` flag indicates
        whether to report the result of ``readlink`` for a symlink-type
        path.
        """
        cstat = path.lstat()
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
        item = cls(
            name=path,
            type=ctype,
            size=cstat.st_size,
            mode=cmode,
            mtime=cstat.st_mtime,
            uid=cstat.st_uid,
            gid=cstat.st_gid,
        )
        if ctype == FileSystemItemType.symlink:
            # could be p.readlink() from PY3.9+
            item.link_target = PurePath(os.readlink(path))
        return item


def compute_multihash_from_fp(fp, hash: List[str], bufsize=COPY_BUFSIZE):
    """Compute multiple hashes from a file-like
    """
    hash = MultiHash(hash)
    while True:
        chunk = fp.read(bufsize)
        if not chunk:
            break
        hash.update(chunk)
    return hash.get_hexdigest()
