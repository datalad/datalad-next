from __future__ import annotations

import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Generator

import pytest

from datalad.api import download

from datalad_next.iter_collections.utils import (
    FileSystemItemType,
    compute_multihash_from_fp,
)


@dataclass
class TestCollection:
    path: Path
    item_count: int
    content: bytes
    target_hash: dict[str, str]


@pytest.fixture(scope="session")
def sample_zip(tmp_path_factory) -> Generator[TestCollection, None, None]:
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

    yield TestCollection(
        path=path,
        item_count=4,
        content=file_content,
        target_hash={
            'SHA1': 'b5dfcec4d1b6166067226fae102f7fbcf6bd1bd4',
            'md5': 'd700214df5487801e8ee23d31e60382a',
        }
    )
    path.unlink()


@pytest.fixture(scope="session")
def sample_tar_xz(tmp_path_factory) -> Generator[TestCollection, None, None]:
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
    path = tmp_path_factory.mktemp("tarfile") / 'sample.tar.xz'
    download({
        'https://github.com/datalad/datalad-next/releases/download/0.1.0/test_archive.tar.xz':
            path
    })

    yield TestCollection(
        path=path,
        item_count=6,
        content=b'123\n',
        target_hash={
            'SHA1': 'a8fdc205a9f19cc1c7507a60c4f01b13d11d7fd0',
            'md5': 'ba1f2511fc30423bdbb183fe33f3dd0f',
        }
    )
    path.unlink()


def check_file_pointer(sample_collection: TestCollection,
                       collection_iterator: callable
                       ) -> list:

    result = []
    for item in collection_iterator(sample_collection.path, fp=True):

        result.append(item)

        if item.type in (FileSystemItemType.file,
                         FileSystemItemType.hardlink):

            assert item.fp is not None
            assert compute_multihash_from_fp(
                item.fp,
                ['md5', 'SHA1']
            ) == sample_collection.target_hash

            item.fp.seek(0)
            assert item.fp.read() == sample_collection.content

    assert len(result) == sample_collection.item_count
    return result
