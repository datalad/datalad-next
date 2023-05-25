
from .common import check_basic_functionality
from datalad_next.archive_operations.tarfile import TarArchiveOperations
from datalad_next.iter_collections.tests.utils import (
    TestCollection,
    sample_tar_xz,
)


def test_basics(sample_tar_xz: TestCollection):
    check_basic_functionality(
        TarArchiveOperations,
        sample_tar_xz,
        'test-archive/123_hard.txt',
    )
