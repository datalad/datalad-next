import pytest

from datalad_next.tests.utils import (
    create_tree,
    rmtree,
)
from datalad_next.utils import check_symlink_capability

from ..directory import (
    IterdirItem,
    PathType,
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
    target = [
        IterdirItem(path=dir_tree / 'random_file1.txt', type=PathType.file),
        IterdirItem(path=dir_tree / 'some_dir', type=PathType.directory),
    ]
    if check_symlink_capability(dir_tree / '__dummy1__',
                                dir_tree / '__dummy2__'):
        target.append(
            IterdirItem(
                path=dir_tree / 'symlink',
                type=PathType.symlink,
                symlink_target=dir_tree / 'some_dir' / "file_in_dir.txt",
            ),
        )

    iterdir_res = list(iterdir(dir_tree))
    assert len(iterdir_res) == len(target)
    for item in iterdir(dir_tree):
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
