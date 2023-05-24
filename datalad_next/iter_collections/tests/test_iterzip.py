from pathlib import PurePath

from ..zipfile import (
    ZipfileItem,
    FileSystemItemType,
    iter_zip,
)

from .utils import (
    TestCollection,
    check_file_pointer,
    sample_zip,
)


def test_iter_zip(sample_zip: TestCollection):
    root = PurePath('test-archive')
    targets = [
        ZipfileItem(
            name=root,
            type=FileSystemItemType.directory,
            size=0,
        ),
        ZipfileItem(
            name=root / 'onetwothree.txt',
            type=FileSystemItemType.file,
            size=len(sample_zip.content),
        ),
        ZipfileItem(
            name=root / 'subdir',
            type=FileSystemItemType.directory,
            size=0,
        ),
        ZipfileItem(
            name=root / 'subdir' / 'onetwothree_again.txt',
            type=FileSystemItemType.file,
            size=len(sample_zip.content),
        ),
    ]

    items = check_file_pointer(sample_zip, iter_zip)

    # Check read items against target items
    for item in items:
        # do not compare fp or mtime
        item.fp = None
        item.mtime = None
    for target in targets:
        assert target in items
