"""Report on the content of TAR archives"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
import tarfile
from typing import (
    Generator,
    List
)

from .utils import (
    FileSystemItem,
    FileSystemItemType,
    compute_multihash_from_fp,
)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class ItertarItem(FileSystemItem):
    pass


def itertar(
    path: Path,
    hash: List[str] | None = None,
) -> Generator[ItertarItem, None, None]:
    """
    Parameters
    ----------
    path: Path
      Path of the TAR archive to report content for (iterate over).

    Yields
    ------
    :class:`ItertarItem`
    """
    with tarfile.open(path, 'r') as tar:
        for member in tar:
            # reduce the complexity of tar member types to the desired
            # level (ie. disregard the diversity of special files and
            # block devices)
            mtype = FileSystemItemType.file if member.isreg() \
                else FileSystemItemType.directory if member.isdir() \
                else FileSystemItemType.symlink if member.issym() \
                else FileSystemItemType.hardlink if member.islnk() \
                else FileSystemItemType.specialfile
            item = ItertarItem(
                name=PurePosixPath(member.name),
                type=mtype,
                size=member.size,
                mode=member.mode,
                mtime=member.mtime,
                link_target=PurePath(PurePosixPath(member.linkname))
                if member.linkname else None,
                hash=_compute_hash(tar, member, hash)
                if hash and mtype in (
                    FileSystemItemType.file, FileSystemItemType.hardlink)
                else None,
            )
            yield item


def _compute_hash(
        tar: tarfile.TarFile, member: tarfile.TarInfo, hash: List[str]):
    with tar.extractfile(member) as f:
        return compute_multihash_from_fp(f, hash)
