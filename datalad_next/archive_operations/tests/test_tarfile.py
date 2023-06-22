from __future__ import annotations

from typing import Generator

import pytest

from .common import (
    TestArchive,
    run_archive_basics_test,
    run_archive_contain_test,
    run_archive_iterator_test,
    run_archive_open_test,
)
from ..tarfile import TarArchiveOperations


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


member_name = 'test-archive/onetwothree.txt'


def test_tararchive_basics(structured_sample_tar_xz: TestArchive):
    run_archive_basics_test(
        TarArchiveOperations,
        structured_sample_tar_xz,
        member_name)


def test_tararchive_contain(structured_sample_tar_xz: TestArchive):
    run_archive_contain_test(
        TarArchiveOperations,
        structured_sample_tar_xz,
        member_name)


def test_tararchive_iterator(structured_sample_tar_xz: TestArchive):
    run_archive_iterator_test(TarArchiveOperations, structured_sample_tar_xz)


def test_open(structured_sample_tar_xz: TestArchive):
    run_archive_open_test(TarArchiveOperations, structured_sample_tar_xz)
