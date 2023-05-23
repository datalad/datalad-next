from pathlib import PurePath
import pytest

from datalad.api import download

from ..tarfile import (
    TarfileItem,
    FileSystemItemType,
    iter_tar,
)
from ..utils import compute_multihash_from_fp


@pytest.fixture(scope="session")
def sample_tar_xz(tmp_path_factory):
    """Provides a path to a tarball with file, directory, hard link,
    and soft link. Any file content is '123\n'. The associated hashes
    are:

    md5: ba1f2511fc30423bdbb183fe33f3dd0f
    sha1: a8fdc205a9f19cc1c7507a60c4f01b13d11d7fd0

    Layout::

        ❯ datalad tree --include-files test-archive
        test-archive
        ├── 123.txt -> subdir/onetwothree_again.txt
        ├── 123_hard.txt
        ├── onetwothree.txt
        └── subdir/
            └── onetwothree_again.txt
    """
    path = tmp_path_factory.mktemp("tarfile")
    tfpath = path / 'sample.tar.xz'
    download({
        'https://github.com/datalad/datalad-next/releases/download/0.1.0/test_archive.tar.xz':
        tfpath
    })

    yield tfpath

    tfpath.unlink()


def test_iter_tar(sample_tar_xz):
    target_hash = {'SHA1': 'a8fdc205a9f19cc1c7507a60c4f01b13d11d7fd0',
                   'md5': 'ba1f2511fc30423bdbb183fe33f3dd0f'}
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
            size=4,
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
            size=4,
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
    ires = []
    for i in iter_tar(sample_tar_xz, fp=True):
        # check that file pointer is usable
        if i.fp:
            assert compute_multihash_from_fp(
                i.fp, ['md5', 'SHA1']) == target_hash
            # we null the file pointers to ease the comparison
            i.fp = None
        ires.append(i)
    # root + subdir, 2 files, softlink, hardlink
    assert 6 == len(ires)
    for t in targets:
        assert t in ires
