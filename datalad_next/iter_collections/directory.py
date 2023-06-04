"""Report on the content of directories

The main functionality is provided by the :func:`iter_dir()` function.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Generator

from datalad_next.exceptions import CapturedException

from .utils import (
    FileSystemItem,
    FileSystemItemType,
)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class DirectoryItem(FileSystemItem):
    pass


def iter_dir(
    path: Path,
    *,
    fp: bool = False,
) -> Generator[DirectoryItem, None, None]:
    """Uses ``Path.iterdir()`` to iterate over a directory and reports content

    The iterator produces an :class:`DirectoryItem` instance with standard
    information on file system elements, such as ``size``, or ``mtime``.

    In addition to a plain ``Path.iterdir()`` the report includes a path-type
    label (distinguished are ``file``, ``directory``, ``symlink``).

    Parameters
    ----------
    path: Path
      Path of the directory to report content for (iterate over).
    fp: bool, optional
      If ``True``, each file-type item includes a file-like object
      to access the file's content. This file handle will be closed
      automatically when the next item is yielded.

    Yields
    ------
    :class:`DirectoryItem`
    """
    for c in path.iterdir():
        # c could disappear while this is running. Example: temp files managed
        # by other processes.
        try:
            item = DirectoryItem.from_path(
                c,
                link_target=True,
            )
        except FileNotFoundError as e:
            CapturedException(e)
            continue
        if fp and item.type == FileSystemItemType.file:
            with c.open('rb') as fp:
                item.fp = fp
                yield item
        else:
            yield item
