from __future__ import annotations

from dataclasses import dataclass
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import Generator

import pytest

from datalad_next.iter_collections import FileSystemItemType

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


def test_ziparchive_basics(structured_sample_zip: TestArchive):
    spec = structured_sample_zip
    # this is intentionally a hard-coded POSIX relpath
    member_name = 'test-archive/onetwothree.txt'
    with ZipArchiveOperations(spec.path) as archive_ops:
        with archive_ops.open(member_name) as member:
            assert member.read() == spec.content
        with archive_ops.open(PurePosixPath(member_name)) as member:
            assert member.read() == spec.content


def test_ziparchive_contain(structured_sample_zip: TestArchive):
    # this is intentionally a hard-coded POSIX relpath
    member_name = 'test-archive/onetwothree.txt'
    with ZipArchiveOperations(structured_sample_zip.path) as archive_ops:
        assert member_name in archive_ops
        assert PurePosixPath(member_name) in archive_ops
        assert 'bogus' not in archive_ops


def test_ziparchive_iterator(structured_sample_zip: TestArchive):
    spec = structured_sample_zip
    with ZipArchiveOperations(spec.path) as archive_ops:
        items = list(archive_ops)
        assert len(items) == spec.item_count
        for item in items:
            assert item.name in archive_ops


def test_open(structured_sample_zip: TestArchive):
    spec = structured_sample_zip
    file_pointer = set()
    with ZipArchiveOperations(spec.path) as zf:
        for item in zf:
            if item.type != FileSystemItemType.file:
                continue
            with zf.open(item.name) as fp:
                file_pointer.add(fp)
                assert fp.read(len(spec.content)) == spec.content
        for fp in file_pointer:
            assert fp.closed is True


def test_open_zipinfo(structured_sample_zip: TestArchive):
    spec = structured_sample_zip
    with ZipArchiveOperations(spec.path) as zf:
        # get zipfile-native ZipInfo items
        for item in zf.zipfile.infolist():
            if item.filename.endswith('/'):
                # crude approach to skippping non-files
                continue
            with zf.open(item) as fp:
                assert fp.read(len(spec.content)) == spec.content


def test_ziparchive_noncontext(structured_sample_zip: TestArchive):
    spec = structured_sample_zip
    zip = ZipArchiveOperations(spec.path)
    assert zip.zipfile.filename == str(spec.path)
    zip.close()
    assert zip._zipfile is None
