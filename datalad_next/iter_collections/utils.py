"""Utilities and types for collection iterators"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
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
