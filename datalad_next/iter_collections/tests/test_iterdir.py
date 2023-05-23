import os
from pathlib import PurePath
import pytest

from datalad_next.tests.utils import (
    create_tree,
    rmtree,
)
from datalad_next.utils import check_symlink_capability

from ..directory import (
    DirectoryItem,
    FileSystemItemType,
    iter_dir,
)
from ..utils import compute_multihash_from_fp


@pytest.fixture(scope="function")
def dir_tree(tmp_path_factory):
    path = tmp_path_factory.mktemp("dir_tree")
    create_tree(
        path,
        {
            "random_file1.txt": "some content",
            "some_dir": {
                "file_in_dir.txt": "some content in file in dir",
            },
        }
    )
    symlink = path / 'symlink'
    symlink_target = path / 'some_dir' / "file_in_dir.txt"

    if check_symlink_capability(symlink, symlink_target):
        symlink.symlink_to(symlink_target)

    yield path
    rmtree(path)


def test_iter_dir(dir_tree):
    target_hash = dict(md5='9893532233caff98cd083a116b013c0b',
                       SHA1='94e66df8cd09d410c62d9e0dc59d3a884e458e05')
    target_paths = [
        (dir_tree / 'random_file1.txt', FileSystemItemType.file, {}),
        (dir_tree / 'some_dir', FileSystemItemType.directory, {}),
    ]
    if check_symlink_capability(dir_tree / '__dummy1__',
                                dir_tree / '__dummy2__'):
        target_paths.append((
            dir_tree / 'symlink', FileSystemItemType.symlink,
            # how `readlink()` behaves on windows is fairly complex
            # rather than anticipating a result (that changes with
            # python version, see https://bugs.python.org/issue42957),
            # we simply test that this is compatible with `os.readlink()`
            dict(link_target=PurePath(os.readlink(dir_tree / 'symlink'))),
        ))
    target = [
        DirectoryItem(
            name=PurePath(path.name),
            type=type,
            size=path.lstat().st_size,
            mode=path.lstat().st_mode,
            mtime=path.lstat().st_mtime,
            uid=path.lstat().st_uid,
            gid=path.lstat().st_gid,
            **kwa
        )
        for path, type, kwa in target_paths
    ]

    iter_dir_res = []
    for i in iter_dir(dir_tree, fp=True):
        if i.fp:
            # capitalization of algorithm labels is preserved
            assert compute_multihash_from_fp(
                i.fp, ['md5', 'SHA1']) == target_hash
            # we null the file pointers to ease the comparison
            i.fp = None
        iter_dir_res.append(i)
    assert len(iter_dir_res) == len(target)

    # check iter_dir() to be robust to concurrent removal
    it = iter_dir(dir_tree)
    # start iteration
    next(it)
    # wipe out content
    for i in dir_tree.glob('*'):
        rmtree(i)
    # consume the rest of the generator, nothing more, but also no crashing
    assert [] == list(it)
