import os
from random import random

import pytest
from datalad.tests.utils_pytest import (
    assert_in,
    assert_not_in,
    assert_raises,
    eq_,
    neq_,
    with_tree, assert_str_equal
)
from datalad.utils import rmtemp

from ..tree import Tree

"""
Tests for datalad tree.
"""


def create_temp_dir_tree(tree_dict):
    """
    Create a temporary directory tree.
    This is a shim for the 'with_tree' decorator so it can be used
    in a module-scoped pytest fixture.
    """
    # function to be decorated by 'with_tree'
    # just return the argument (will be the created temp path)
    identity_func = lambda d: d

    # give an informative name to the lambda function, since
    # it will be included in the name of the temp dir
    identity_func.__name__ = "test_tree"

    # call the 'with_tree' decorator to return the path
    # of the created temp dir root, without deleting it
    temp_dir_root = with_tree(tree_dict, delete=False)(identity_func)()
    return temp_dir_root


@pytest.fixture(scope="module")
def path():
    """
    Fixture for temporary directory tree including nested
    directories, without datasets
    """
    dir_tree = {
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

    temp_dir_root = create_temp_dir_tree(dir_tree)
    yield temp_dir_root
    rmtemp(temp_dir_root)
    assert not os.path.exists(temp_dir_root)


def format_param_ids(val):
    """Helper to format pytest parameter IDs.
    If the parameter is a multiline string, we assume it is the
    parameter 'expected' (expected output of tree), and just
    give it a fixed ID."""
    if isinstance(val, str) and "\n" in val:
        return "expected"


# Combinations of parameters to be tested and their expected results.
# (2 levels per param) ** (3 params) = 8 combinations + 8 expected results
param_combinations = [
    {
        "depth": 1,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "3 directories, 0 datasets, 0 files",
        "expected_str": """
├── dir0
├── dir1
└── dir2
"""
    },
    {
        "depth": 3,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "6 directories, 0 datasets, 0 files",
        "expected_str": """
├── dir0
├── dir1
└── dir2
    ├── dir2_dir0
    ├── dir2_dir1
    └── dir2_dir2
"""
    },
    {
        "depth": 1,
        "include_files": True,
        "include_hidden": False,
        "expected_stats_str": "3 directories, 0 datasets, 2 files",
        "expected_str": """
├── file0
├── file1
├── dir0
├── dir1
└── dir2
"""
    },
    {
        "depth": 3,
        "include_files": True,
        "include_hidden": False,
        "expected_stats_str": "6 directories, 0 datasets, 8 files",
        "expected_str": """
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
    },
    {
        "depth": 1,
        "include_files": True,
        "include_hidden": True,
        "expected_stats_str": "4 directories, 0 datasets, 3 files",
        "expected_str": """
├── .file2
├── file0
├── file1
├── .dir3
├── dir0
├── dir1
└── dir2
"""
    },
    {
        "depth": 3,
        "include_files": True,
        "include_hidden": True,
        "expected_stats_str": "7 directories, 0 datasets, 11 files",
        "expected_str": """
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
    },
    {
        "depth": 1,
        "include_files": False,
        "include_hidden": True,
        "expected_stats_str": "4 directories, 0 datasets, 0 files",
        "expected_str": """
├── .dir3
├── dir0
├── dir1
└── dir2
"""
    },
    {
        "depth": 3,
        "include_files": False,
        "include_hidden": True,
        "expected_stats_str": "7 directories, 0 datasets, 0 files",
        "expected_str": """
├── .dir3
├── dir0
├── dir1
└── dir2
    ├── dir2_dir0
    ├── dir2_dir1
    └── dir2_dir2
"""
    },
]


def build_param_matrix(param_names):
    matrix = []
    for combination in param_combinations:
        matrix.append(
            # order of combinations does not matter
            [val for key, val in combination.items() if key in param_names]
        )
    return matrix


@pytest.mark.parametrize(
    ["depth", "include_files", "include_hidden", "expected_str"],
    build_param_matrix(["depth", "include_files", "include_hidden", "expected_str"]), ids=format_param_ids
)
def test_print_tree_with_params(
    path, depth, include_files, include_hidden, expected_str
):
    root = os.path.join(path, 'root')
    tree = Tree(
        root, max_depth=depth,
        include_files=include_files, include_hidden=include_hidden)
    actual_res = str(tree)
    expected_res = root + expected_str
    assert_str_equal(expected_res, actual_res)


def test_print_tree_for_nonexistent_directory():
    """Obtain nonexistent directory by creating a temp dir
    and deleting it (may be safest method)"""
    nonexistent_dir = with_tree({"to_be_deleted": []})(lambda f: f)()
    with assert_raises(ValueError):
        Tree(nonexistent_dir, max_depth=1)


@pytest.mark.parametrize(
    ["depth", "include_files", "include_hidden", "expected_stats_str"],
    build_param_matrix(["depth", "include_files", "include_hidden", "expected_stats_str"])
)
def test_tree_stats(
        path, depth, include_files, include_hidden, expected_stats_str
):
    root = os.path.join(path, 'root')
    tree = Tree(
        root, max_depth=depth,
        include_files=include_files, include_hidden=include_hidden).build()
    actual_res = tree.stats()
    expected_res = expected_stats_str + "\n"
    assert_str_equal(expected_res, actual_res)
