import pytest
import zipfile
from pathlib import PurePath

from ..zipfile import (
    ZipfileItem,
    FileSystemItemType,
    iter_zip,
)

from ..utils import compute_multihash_from_fp


@pytest.fixture(scope="session")
def sample_zip(tmp_path_factory):
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

    yield path
    path.unlink()


def test_iter_zip(sample_zip):
    target_hash = {
        'SHA1': 'b5dfcec4d1b6166067226fae102f7fbcf6bd1bd4',
        'md5': 'd700214df5487801e8ee23d31e60382a',
    }
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
            size=8,
        ),
        ZipfileItem(
            name=root / 'subdir',
            type=FileSystemItemType.directory,
            size=0,
        ),
        ZipfileItem(
            name=root / 'subdir' / 'onetwothree_again.txt',
            type=FileSystemItemType.file,
            size=8,
        ),
    ]

    ires = []
    for i in iter_zip(sample_zip, fp=True):
        # check that file pointer is usable
        if i.fp:
            assert compute_multihash_from_fp(
                i.fp, ['md5', 'SHA1']) == target_hash
            # we null the file pointers to ease the comparison
            i.fp = None
        ires.append(i)

    # root + subdir, 2 files
    assert 4 == len(ires)
    for r in ires:
        # do not compare mtime
        r.mtime = None
    for t in targets:
        assert t in ires
