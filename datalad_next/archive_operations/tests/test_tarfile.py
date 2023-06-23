from __future__ import annotations

from dataclasses import dataclass
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import Generator

import pytest

from datalad_next.iter_collections.utils import FileSystemItemType

from ..tarfile import TarArchiveOperations


@dataclass
class TestArchive:
    path: Path
    item_count: int
    content: bytes
    target_hash: dict[str, str]


@pytest.fixture(scope='session')
def structured_sample_tar_xz(
    sample_tar_xz
) -> Generator[TestArchive, None, None]:
    yield TestArchive(
        path=sample_tar_xz,
        item_count=6,
        content=b'123\n',
        target_hash={
            'SHA1': 'b5dfcec4d1b6166067226fae102f7fbcf6bd1bd4',
            'md5': 'd700214df5487801e8ee23d31e60382a',
        }
    )


def test_tararchive_basics(structured_sample_tar_xz: TestArchive):
    spec = structured_sample_tar_xz
    # this is intentionally a hard-coded POSIX relpath
    member_name = 'test-archive/onetwothree.txt'
    with TarArchiveOperations(spec.path) as archive_ops:
        with archive_ops.open(member_name) as member:
            assert member.read() == spec.content
        with archive_ops.open(PurePosixPath(member_name)) as member:
            assert member.read() == spec.content


def test_tararchive_contain(structured_sample_tar_xz: TestArchive):
    # this is intentionally a hard-coded POSIX relpath
    member_name = 'test-archive/onetwothree.txt'
    archive_ops = TarArchiveOperations(structured_sample_tar_xz.path)
    # POSIX path str
    assert member_name in archive_ops
    # POSIX path as obj
    assert PurePosixPath(member_name) in archive_ops
    assert 'bogus' not in archive_ops


def test_tararchive_iterator(structured_sample_tar_xz: TestArchive):
    spec = structured_sample_tar_xz
    with TarArchiveOperations(spec.path) as archive_ops:
        items = list(archive_ops)
        assert len(items) == spec.item_count
        for item in items:
            assert item.name in archive_ops


def test_open(structured_sample_tar_xz: TestArchive):
    spec = structured_sample_tar_xz
    file_pointer = set()
    with TarArchiveOperations(spec.path) as tf:
        for item in tf:
            if item.type == FileSystemItemType.file:
                with tf.open(str(PurePosixPath(item.name))) as fp:
                    file_pointer.add(fp)
                    assert fp.read(len(spec.content)) == spec.content
        # check the fp before we close the archive handler
        for fp in file_pointer:
            assert fp.closed is True
