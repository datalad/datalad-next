from __future__ import annotations

import zipfile
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
def sample_zip(tmp_path_factory) -> Generator[TestArchive, None, None]:
    """Create a sample zip file

    Provides a path to a zip with files and directories. Any file content is
    'zip-123\n'. The associated hashes are:

    md5: d700214df5487801e8ee23d31e60382a
    sha1: b5dfcec4d1b6166067226fae102f7fbcf6bd1bd4

    Layout::

        test-archive
        ├── onetwothree.txt
        └── subdir/
            └── onetwothree_again.txt
    """
    path = tmp_path_factory.mktemp('zipfile') / 'sample.zip'
    file_content = b'zip-123\n'
    with zipfile.ZipFile(path, mode='w') as zip_file:
        zip_file.writestr('test-archive/', '')
        zip_file.writestr('test-archive/subdir/', '')
        with zip_file.open('test-archive/onetwothree.txt', mode='w') as fp:
            fp.write(file_content)
        with zip_file.open('test-archive/subdir/onetwothree_again.txt', mode='w') as fp:
            fp.write(file_content)

    yield TestArchive(
        path=path,
        item_count=4,
        content=file_content,
        target_hash={
            'SHA1': 'b5dfcec4d1b6166067226fae102f7fbcf6bd1bd4',
            'md5': 'd700214df5487801e8ee23d31e60382a',
        }
    )
    path.unlink()


def test_basics(sample_zip: TestArchive):
    member_name = 'test-archive/onetwothree.txt'
    with ZipArchiveOperations(sample_zip.path) as archive_ops:
        with archive_ops.open(member_name) as member:
            assert member.read() == sample_zip.content


def test_containment(sample_zip: TestArchive):
    member_name = 'test-archive/onetwothree.txt'
    archive_ops = ZipArchiveOperations(sample_zip.path)
    assert member_name in archive_ops


def test_iterator(sample_zip: TestArchive):
    archive_ops = ZipArchiveOperations(sample_zip.path)
    items = list(archive_ops)
    assert len(items) == sample_zip.item_count
    for item in items:
        item_name = (
            str(PurePosixPath(item.name)) +
            '/' if item.type == FileSystemItemType.directory
            else str(PurePosixPath(item.name))
        )
        assert item_name in archive_ops
    archive_ops.close()


def test_open(sample_zip: TestArchive):
    archive_ops = ZipArchiveOperations(sample_zip.path)
    file_pointer = set()
    for item in list(archive_ops):
        if item.type == FileSystemItemType.file:
            with archive_ops.open(str(item.name)) as fp:
                file_pointer.add(fp)
                assert fp.read(len(sample_zip.content)) == sample_zip.content
    for fp in file_pointer:
        assert fp.closed is True
    archive_ops.close()
