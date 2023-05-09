import os
from pathlib import PurePath
import pytest

from datalad_next.tests.utils import (
    create_tree,
    rmtree,
)
from datalad_next.utils import check_symlink_capability

from ..directory import (
    IterdirItem,
    FileSystemItemType,
    iterdir,
)


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


def test_iterdir(dir_tree):
    target_paths = [
        (dir_tree / 'random_file1.txt', FileSystemItemType.file,
         dict(hash=dict(md5='9893532233caff98cd083a116b013c0b',
                        SHA1='94e66df8cd09d410c62d9e0dc59d3a884e458e05'))),
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
        IterdirItem(
            name=PurePath(path.name),
            type=type,
            size=path.lstat().st_size,
            mode=path.lstat().st_mode,
            mtime=path.lstat().st_mtime,
            **kwa
        )
        for path, type, kwa in target_paths
    ]

    iterdir_res = list(iterdir(dir_tree))
    assert len(iterdir_res) == len(target)
    # capitalization of algorithm labels is preserved
    for item in iterdir(dir_tree, hash=['md5', 'SHA1']):
        assert item in target

    # check iterdir() to be robust to concurrent removal
    it = iterdir(dir_tree)
    # start iteration
    next(it)
    # wipe out content
    for i in dir_tree.glob('*'):
        rmtree(i)
    # consume the rest of the generator, nothing more, but also no crashing
    assert [] == list(it)
