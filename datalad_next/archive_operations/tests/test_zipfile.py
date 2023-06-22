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
from ..zipfile import ZipArchiveOperations


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


member_name = 'test-archive/onetwothree.txt'


def test_ziparchive_basics(structured_sample_zip: TestArchive):
    run_archive_basics_test(
        ZipArchiveOperations,
        structured_sample_zip,
        member_name)


def test_ziparchive_contain(structured_sample_zip: TestArchive):
    run_archive_contain_test(
        ZipArchiveOperations,
        structured_sample_zip,
        member_name)


def test_ziparchive_iterator(structured_sample_zip: TestArchive):
    run_archive_iterator_test(ZipArchiveOperations, structured_sample_zip)


def test_open(structured_sample_zip: TestArchive):
    run_archive_open_test(ZipArchiveOperations, structured_sample_zip)
