import os

import pytest
from datalad.tests.utils_pytest import (
    assert_in,
    assert_not_in,
    assert_raises,
    eq_,
    neq_,
    with_tree, assert_str_equal,
)
from datalad.utils import rmtemp

from ..tree import Tree, Walk

"""
Tests for datalad tree.
"""

# directory layout to be tested that will be created as temp dir
_temp_dir_tree = {
    "root": {
        ".dir3": {
            "dir3_file0": 'tempfile',
            ".dir3_file1": 'tempfile',
        },
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
            "dir2_file1": 'tempfile',
        },
        ".file2": 'tempfile',
        "file0": 'tempfile',
        "file1": 'tempfile',
    }
}


@pytest.fixture(scope="module")
def path():
    """
    Create a temporary directory tree once for the whole module,
    to be used as test data for all tests.
    This is a shim for the 'with_tree' decorator so it can be used
    as module-scoped pytest fixture.
    """
    # function to be decorated by 'with_tree'
    # just return the argument (will be the created temp path)
    identity_func = lambda d: d

    # give an informative name to the lambda function, since
    # it will be included in the name of the temp dir
    identity_func.__name__ = "test_tree"

    # call the 'with_tree' decorator to return the path
    # of the created temp dir root, without deleting it
    temp_dir_root = with_tree(_temp_dir_tree, delete=False)(identity_func)()
    print(f"created temp dir at {temp_dir_root}")
    yield temp_dir_root
    rmtemp(temp_dir_root)  # this duplicates 'with_tree' code
    print(f"deleted temp dir at {temp_dir_root}")


def format_param_ids(val):
    """Helper to format pytest parameter IDs.
    If the parameter is a string containing newlines, we assume it
    is the parameter 'expected' (expected output of tree), and just
    give it a fixed ID."""
    if isinstance(val, str) and "\n" in val:
        return "expected"


param_matrix = [
    # (2 levels per param) ** (3 params) = 8 combinations + 8 expected results
    # column names: depth, include_files, include_hidden, expected
    [
        1,
        False,
        False,
        """
├── dir0
├── dir1
└── dir2
"""
    ],
    [
        3,
        False,
        False,
        """
├── dir0
├── dir1
└── dir2
    ├── dir2_dir0
    ├── dir2_dir1
    └── dir2_dir2
"""
    ],
    [
        1,
        True,
        False,
        """
├── file0
├── file1
├── dir0
├── dir1
└── dir2
"""
    ],
    [
        3,
        True,
        False,
        """
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
    ],
    [
        1,
        True,
        True,
        """
├── .file2
├── file0
├── file1
├── .dir3
├── dir0
├── dir1
└── dir2
"""
    ],
    [
        3,
        True,
        True,
        """
├── .file2
├── file0
├── file1
├── .dir3
|   ├── .dir3_file1
|   └── dir3_file0
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
    ],
    [
        1,
        False,
        True,
        """
├── .dir3
├── dir0
├── dir1
└── dir2
"""
    ],
    [
        3,
        False,
        True,
        """
├── .dir3
├── dir0
├── dir1
└── dir2
    ├── dir2_dir0
    ├── dir2_dir1
    └── dir2_dir2
"""
    ]
]


@pytest.mark.parametrize(
    ["depth", "include_files", "include_hidden", "expected"],
    param_matrix, ids=format_param_ids
)
def test_print_tree_with_params(
    path, depth, include_files, include_hidden, expected
):
    root = os.path.join(path, 'root')
    walk = Walk(
        root, max_depth=depth,
        include_files=include_files, include_hidden=include_hidden)
    walk.build_tree()
    actual_res = walk.get_tree()
    expected_res = root + expected
    assert_str_equal(expected_res, actual_res)
