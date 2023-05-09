"""Report on the content of TAR archives"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import (
    Path,
    PurePosixPath,
)
import tarfile
from typing import (
    Dict,
    Generator,
    List
)

from datalad_next.utils.consts import COPY_BUFSIZE
from datalad_next.utils.multihash import MultiHash


# TODO Could be `StrEnum`, came with PY3.11
class TarMemberType(Enum):
    """Enumeration of member types distinguished by ``itertar()``

    The associated ``str`` values are chosen to be appropriate for
    downstream use (e.g, as type labels in DataLad result records).
    """
    file = 'file'
    directory = 'directory'
    symlink = 'symlink'
    hardlink = 'file'
    specialfile = 'file'


@dataclass(kw_only=True)
class ItertarItem:
    name: PurePosixPath
    type: TarMemberType
    size: int
    mtime: float
    mode: int
    link_target: Path | None = None
    hash: Dict[str, str] | None = None


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
            mtype = TarMemberType.file if member.isreg() \
                else TarMemberType.directory if member.isdir() \
                else TarMemberType.symlink if member.issym() \
                else TarMemberType.hardlink if member.islnk() \
                else TarMemberType.specialfile
            item = ItertarItem(
                name=PurePosixPath(member.name),
                type=mtype,
                size=member.size,
                mode=member.mode,
                mtime=member.mtime,
                link_target=member.linkname or None,
                hash=_compute_hash(tar, member, hash)
                if hash and mtype in (
                    TarMemberType.file, TarMemberType.hardlink)
                else None,
            )
            yield item


# TODO deduplicate with directory._compute_hash()
def _compute_hash(
        tar: tarfile.TarFile, member: tarfile.TarInfo, hash: List[str]):
    with tar.extractfile(member) as f:
        hash = MultiHash(hash)
        while True:
            chunk = f.read(COPY_BUFSIZE)
            if not chunk:
                break
            hash.update(chunk)
    return hash.get_hexdigest()
