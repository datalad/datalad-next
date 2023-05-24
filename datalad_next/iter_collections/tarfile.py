"""Report on the content of TAR archives

The main functionality is provided by the :func:`iter_tar()` function.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import (
    Path,
    PurePath,
    PurePosixPath,
)
import tarfile
from typing import Generator

from .utils import (
    FileSystemItem,
    FileSystemItemType,
)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class TarfileItem(FileSystemItem):
    pass


def iter_tar(
    path: Path,
    *,
    fp: bool = False,
) -> Generator[TarfileItem, None, None]:
    """Uses the standard library ``tarfile`` module to report on TAR archives

    A TAR archive can represent more or less the full bandwidth of file system
    properties, therefore reporting on archive members is implemented
    similar to :func:`~datalad_next.iter_collections.directory.iter_dir()`.
    The iterator produces an :class:`TarfileItem` instance with standard
    information on file system elements, such as ``size``, or ``mtime``.

    Parameters
    ----------
    path: Path
      Path of the TAR archive to report content for (iterate over).
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded or the function
      returns.

    Yields
    ------
    :class:`TarfileItem`
    """
    with tarfile.open(path, 'r') as tar:
        for member in tar:
            # reduce the complexity of tar member types to the desired
            # level (i.e. disregard the diversity of special files and
            # block devices)
            mtype = FileSystemItemType.file if member.isreg() \
                else FileSystemItemType.directory if member.isdir() \
                else FileSystemItemType.symlink if member.issym() \
                else FileSystemItemType.hardlink if member.islnk() \
                else FileSystemItemType.specialfile
            item = TarfileItem(
                name=PurePath(PurePosixPath(member.name)),
                type=mtype,
                size=member.size,
                mode=member.mode,
                mtime=member.mtime,
                uid=member.uid,
                gid=member.gid,
                link_target=PurePath(PurePosixPath(member.linkname))
                if member.linkname else None,
            )
            if fp and mtype in (
                    FileSystemItemType.file, FileSystemItemType.hardlink):
                with tar.extractfile(member) as fp:
                    item.fp = fp
                    yield item
            else:
                yield item
