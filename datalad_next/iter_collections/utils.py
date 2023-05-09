"""Utilities and types for collection iterators"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import PurePath
from typing import (
    Dict,
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
    hardlink = 'file'
    specialfile = 'file'


@dataclass(kw_only=True)
class FileSystemItem:
    name: PurePath
    type: FileSystemItemType
    size: int
    mtime: float
    mode: int
    link_target: PurePath | None = None
    hash: Dict[str, str] | None = None


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
