from __future__ import annotations

from dataclasses import dataclass
from pathlib import (
    Path,
    PurePosixPath,
)

from .. import ArchiveOperations
from datalad_next.iter_collections.utils import FileSystemItemType


@dataclass
class TestArchive:
    path: Path
    item_count: int
    content: bytes
    target_hash: dict[str, str]


def run_archive_basics_test(operations_class: type[ArchiveOperations],
                            sample_archive: TestArchive,
                            member_name: str,
                            ):
    with operations_class(sample_archive.path) as archive_ops:
        with archive_ops.open(member_name) as member:
            assert member.read() == sample_archive.content
        with archive_ops.open(PurePosixPath(member_name)) as member:
            assert member.read() == sample_archive.content


def run_archive_contain_test(operations_class: type[ArchiveOperations],
                             sample_archive: TestArchive,
                             member_name: str,
                             ):
    with operations_class(sample_archive.path) as archive_ops:
        assert member_name in archive_ops
        assert PurePosixPath(member_name) in archive_ops
        assert 'bogus' not in archive_ops


def run_archive_iterator_test(operation_class: type[ArchiveOperations],
                              sample_archive: TestArchive,
                              ):
    with operation_class(sample_archive.path) as archive_ops:
        items = list(archive_ops)
        assert len(items) == sample_archive.item_count
        for item in items:
            assert item.name in archive_ops


def run_archive_open_test(operation_class: type[ArchiveOperations],
                          sample_archive: TestArchive,
                          ):
    file_pointer = set()
    with operation_class(sample_archive.path) as archive:
        for item in archive:
            if item.type == FileSystemItemType.file:
                with archive.open(str(PurePosixPath(item.name))) as fp:
                    file_pointer.add(fp)
                    assert fp.read(len(sample_archive.content)) == sample_archive.content
        for fp in file_pointer:
            assert fp.closed is True
