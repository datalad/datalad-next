import os

from datalad.tests.utils_pytest import (
    assert_in,
    assert_not_in,
    assert_raises,
    eq_,
    neq_,
    patch_config,
    with_tree,
)
from ..tree import Tree, Walk

"""
Tests for datalad tree.
TODO: create fixture for precomputing tree output for a given set
of parameters, so it can be reused in multiple tests.
"""

# directory layout to be tested that will be created as temp dir.
# directory index is equals to the count of its subdirectories
# and the (count-1) of files contained underneath it.
# TODO: generate programmatically instead of hardcoding
# (though it's easier to visualize if hardcoded)
_temp_dir_tree = {
    "root": {
        "dir0": {},  # empty dir
        "dir1": {
            "dir1_file0": 'tempfile',
        },
        "dir2": {
            "dir2_dir0": {},
            "dir2_dir1": {
                "dir2_dir1_file0": 'tempfile',
            },
            "dir2_dir2": {
                "dir2_dir2_file0": 'tempfile',
                "dir2_dir2_file1": 'tempfile',
            },
            "dir2_file0": 'tempfile',
            "dir2_file1":'tempfile'
        },
        "file0": 'tempfile',
        "file1": 'tempfile',
    }
}


@with_tree(_temp_dir_tree)
def test_build_tree_dirs_only(path=None):
    root = os.path.join(path, 'root')
    walk = Walk(root, max_depth=3, include_files=False)
    walk.build_tree()
    actual = walk.get_tree()

    expected = f"""{root}
├── dir0
├── dir1
└── dir2
    ├── dir2_dir0
    ├── dir2_dir1
    └── dir2_dir2
"""
    eq_(expected, actual)


@with_tree(_temp_dir_tree)
def test_build_tree_including_files(path=None):
    root = os.path.join(path, 'root')
    walk = Walk(root, max_depth=3, include_files=True)
    walk.build_tree()
    actual = walk.get_tree()

    expected = f"""{root}
├── file0
├── file1
├── dir0
├── dir1
|   └── dir1_file0
└── dir2
    ├── dir2_file0
    ├── dir2_file1
    ├── dir2_dir0
    ├── dir2_dir1
    |   └── dir2_dir1_file0
    └── dir2_dir2
        ├── dir2_dir2_file0
        └── dir2_dir2_file1
"""
    eq_(expected, actual)

