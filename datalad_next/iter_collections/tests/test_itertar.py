from pathlib import PurePath

from .utils import (
    check_file_pointer,
    sample_tar_xz,
)
from ..tarfile import (
    TarfileItem,
    FileSystemItemType,
    iter_tar,
)


def test_iter_tar(sample_tar_xz):
    targets = [
        TarfileItem(
            name=PurePath('test-archive'),
            type=FileSystemItemType.directory,
            size=0,
            mtime=1683657433,
            mode=509,
            uid=1000,
            gid=1000),
        TarfileItem(
            name=PurePath('test-archive') / '123.txt',
            type=FileSystemItemType.symlink,
            size=0,
            mtime=1683657414,
            mode=511,
            uid=1000,
            gid=1000,
            link_target=PurePath('subdir') / 'onetwothree_again.txt'),
        TarfileItem(
            name=PurePath('test-archive') / '123_hard.txt',
            type=FileSystemItemType.file,
            size=len(sample_tar_xz.content),
            mtime=1683657364,
            mode=436,
            uid=1000,
            gid=1000,
            link_target=None),
        TarfileItem(
            name=PurePath('test-archive') / 'subdir',
            type=FileSystemItemType.directory,
            size=0,
            mtime=1683657400,
            mode=509,
            uid=1000,
            gid=1000),
        TarfileItem(
            name=PurePath('test-archive') / 'subdir' / 'onetwothree_again.txt',
            type=FileSystemItemType.file,
            size=len(sample_tar_xz.content),
            mtime=1683657400,
            mode=436,
            uid=1000,
            gid=1000,
            link_target=None),
        TarfileItem(
            name=PurePath('test-archive') / 'onetwothree.txt',
            type=FileSystemItemType.hardlink,
            size=0,
            mtime=1683657364,
            mode=436,
            uid=1000,
            gid=1000,
            link_target=PurePath('test-archive') / '123_hard.txt'),
    ]

    items = check_file_pointer(sample_tar_xz, iter_tar)

    # Check read items against target items
    for item in items:
        # do not compare fp
        item.fp = None
    for target in targets:
        assert target in items
