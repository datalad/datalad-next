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
    TYPE_CHECKING,
    Dict,
    Union,
    Any,
    IO,
    List,
)

from datalad_next.consts import COPY_BUFSIZE
from datalad_next.utils import MultiHash

if TYPE_CHECKING:
    from .annexworktree import AnnexWorktreeFileSystemItem
    from .directory import DirectoryItem
    from .gitworktree import GitWorktreeFileSystemItem
    from io import BufferedReader
    from os import PathLike
    from tarfile import ExFileObject
    from zipfile import ZipExtFile


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
    """An item with a path as its ``name``

    A dedicated property supports the conversion of the
    native name representation into a ``PurePath`` instance.

    Any argument understood by the ``PurePath`` constructor can
    be used as ``name``, such as a a filename, a relative path, or an
    absolute path -- in string form, as path segment sequence, or
    a ``Path`` instance.

    It is recommended to use name/path values that are relative
    to the containing collection (directory, archive, repository, etc.).
    """
    def path(self) -> PurePath:
        """Returns the item name as a ``PurePath`` instance

        This default implementation assumes the ``name`` to be
        platform path conventions.
        """
        return PurePath(self.name)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class FileSystemItem(PathBasedItem, TypedItem):
    type: FileSystemItemType
    size: int
    mtime: float | None = None
    mode: int | None = None
    uid: int | None = None
    gid: int | None = None
    link_target: str | PathLike[str] | None = None
    fp: IO | None = None

    def link_target_path(self) -> PurePath | None:
        """Returns the link_target as a ``PurePath`` instance"""
        return PurePath(self.link_target) if self.link_target is not None \
            else None

    @classmethod
    def from_path(
        cls,
        path: Path,
        *,
        link_target: bool = True,
    ) -> Union[
        DirectoryItem,
        AnnexWorktreeFileSystemItem,
        FileSystemItem,
        GitWorktreeFileSystemItem,
    ]:
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
        if link_target and ctype == FileSystemItemType.symlink:
            # could be p.readlink() from PY3.9+
            # but check performance difference
            item.link_target = os.readlink(path)
        return item


def compute_multihash_from_fp(
    fp: Union[BufferedReader, ExFileObject, ZipExtFile],
    hash: List[str],
    bufsize: int = COPY_BUFSIZE,
) -> Dict[str, str]:
    """Compute multiple hashes from a file-like
    """
    mhash = MultiHash(hash)
    while True:
        chunk = fp.read(bufsize)
        if not chunk:
            break
        mhash.update(chunk)
    return mhash.get_hexdigest()
