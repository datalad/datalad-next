from __future__ import annotations

from dataclasses import dataclass
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import Generator

import pytest

from datalad_next.iter_collections.utils import FileSystemItemType

from ..zipfile import ZipArchiveOperations


@dataclass
class TestArchive:
    path: Path
    item_count: int
    content: bytes
    target_hash: dict[str, str]


@pytest.fixture(scope='session')
def structured_sample_zip(sample_zip) -> Generator[TestArchive, None, None]:
    yield TestArchive(
        path=sample_zip,
        item_count=4,
        content=b'zip-123\n',
        target_hash={
            'SHA1': 'b5dfcec4d1b6166067226fae102f7fbcf6bd1bd4',
            'md5': 'd700214df5487801e8ee23d31e60382a',
        }
    )


def test_basics(structured_sample_zip: TestArchive):
    member_name = 'test-archive/onetwothree.txt'
    with ZipArchiveOperations(structured_sample_zip.path) as archive_ops:
        with archive_ops.open(member_name) as member:
            assert member.read() == structured_sample_zip.content


def test_containment(structured_sample_zip: TestArchive):
    member_name = 'test-archive/onetwothree.txt'
    archive_ops = ZipArchiveOperations(structured_sample_zip.path)
    assert member_name in archive_ops


def test_iterator(structured_sample_zip: TestArchive):
    archive_ops = ZipArchiveOperations(structured_sample_zip.path)
    items = list(archive_ops)
    assert len(items) == structured_sample_zip.item_count
    for item in items:
        item_name = (
            str(PurePosixPath(item.name)) +
            '/' if item.type == FileSystemItemType.directory
            else str(PurePosixPath(item.name))
        )
        assert item_name in archive_ops
    archive_ops.close()


def test_open(structured_sample_zip: TestArchive):
    archive_ops = ZipArchiveOperations(structured_sample_zip.path)
    file_pointer = set()
    for item in list(archive_ops):
        if item.type == FileSystemItemType.file:
            with archive_ops.open(str(item.name)) as fp:
                file_pointer.add(fp)
                assert fp.read(len(structured_sample_zip.content)) == structured_sample_zip.content
    for fp in file_pointer:
        assert fp.closed is True
    archive_ops.close()
