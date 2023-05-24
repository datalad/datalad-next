
from .common import check_basic_functionality
from ..zipfile import ZipArchiveOperations
from ...iter_collections.tests.utils import (
    TestCollection,
    sample_zip,
)


def test_basics(sample_zip: TestCollection):
    check_basic_functionality(
        ZipArchiveOperations,
        sample_zip,
        'test-archive/onetwothree.txt',
    )
