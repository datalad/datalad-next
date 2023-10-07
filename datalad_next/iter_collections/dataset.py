from __future__ import annotations


from dataclasses import dataclass
from pathlib import Path
from typing import Generator

from datalad.distribution.dataset import require_dataset

from .utils import (
    FileSystemItemType,
    PathBasedItem,
)


@dataclass
# TODO: the first two elements are duplicated in FileSystemItem,
#  this is because FileSystemItem contains a number of properties
#  that we do not nedd. We should refactor FileSystemItem to
#  separate the path, type, size fields from the other FileSystemItem
#  fields and increase re-usability and decrease duplication.
class DatasetItem(PathBasedItem):
    type: FileSystemItemType
    size: int
    gitsha: str



def iter_dataset(
        path: Path,
        *,
        submodules: bool = False,
        fp: bool = False,
) -> Generator[DatasetItem, None, None]:
    """Iterate over all elements of a DataLad dataset

    Parameters
    ----------
    path: Path
      Path of the root of a DataLad dataset
    submodules: bool, optional
      If ``True`` recurse into submodules.
    fp: bool, optional
      If ``True``, each file-type item that is locally available,
      includes a file-like object to access the file's content.
      This file handle will be closed automatically when the next
      item is yielded or the function returns.
      If the file is not locally available, i.e. it is annexed and
      not yet fetched, ``None`` will be returned as file-like
      object.

    Yields
    ------
    :class:`DatasetItem
    """

    dataset = require_dataset(Path, purpose='iter_dataset')


    with zipfile.ZipFile(path, mode='r') as zip_file:
        for zip_info in zip_file.infolist():
            item = _get_zipfile_item(zip_info)
            if fp and item.type == FileSystemItemType.file:
                with zip_file.open(zip_info) as fp:
                    item.fp = fp
                    yield item
            else:
                yield item
