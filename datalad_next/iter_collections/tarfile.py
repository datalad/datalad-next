"""Report on the content of TAR archives

The main functionality is provided by the :func:`itertar()` function.
"""

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
    *,
    hash: List[str] | None = None,
) -> Generator[ItertarItem, None, None]:
    """Uses the standard library ``tarfile`` module to report on TAR archives

    A TAR archive can represent more or less the full bandwidth of file system
    properties, therefore reporting on archive members is implemented
    similar to :func:`~datalad_next.iter_collections.directory.iterdir()`.
    The iterator produces an :class:`ItertarItem` instance with standard
    information on file system elements, such as ``size``, or ``mtime``.

    Moreover, any number of checksums for file content can be computed and
    reported. When computing checksums, individual archive members are read
    sequentially without extracting the full archive.

    Parameters
    ----------
    path: Path
      Path of the TAR archive to report content for (iterate over).
    hash: list(str), optional
      Any number of hash algorithm names (supported by the ``hashlib`` module
      of the Python standard library. If given, an item corresponding to the
      algorithm will be included in the ``hash`` property dict of each
      reported file-type item.

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
                name=PurePath(PurePosixPath(member.name)),
                type=mtype,
                size=member.size,
                mode=member.mode,
                mtime=member.mtime,
                uid=member.uid,
                gid=member.gid,
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
