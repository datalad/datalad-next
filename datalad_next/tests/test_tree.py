import os
from os.path import join as opj
from datetime import datetime

import pytest
from datalad.distribution.dataset import Dataset
from datalad.tests.utils_pytest import (
    assert_raises,
    assert_str_equal,
    with_tree, assert_re_in
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
def path_no_ds():
    """
    Fixture for temporary directory tree including nested
    directories, without datasets
    """
    dir_tree = {
        "root": {
            ".dir3": {
                "dir3_file0": '',
                ".dir3_file1": '',
            },
            "dir0": {},  # empty dir
            "dir1": {
                "dir1_file0": '',
            },
            "dir2": {
                "dir2_dir0": {},
                "dir2_dir1": {
                    "dir2_dir1_file0": '',
                },
                "dir2_dir2": {
                    "dir2_dir2_file0": '',
                    "dir2_dir2_file1": '',
                },
                "dir2_file0": '',
                "dir2_file1": '',
            },
            ".file2": '',
            "file0": '',
            "file1": '',
        }
    }

    temp_dir_root = create_temp_dir_tree(dir_tree)
    yield temp_dir_root
    rmtemp(temp_dir_root)
    assert not os.path.exists(temp_dir_root)


@pytest.fixture(scope="module")
def path_ds():
    """
    Fixture for temporary directory tree including nested
    directories and datasets
    """
    ds_tree = {
        "root": {
            "superds0": {
                "sd0_file0": "",
                "sd0_subds0": {
                    "sd0_sub0_subds0": {}
                }
            },
            "superds1": {
                "sd1_file0": "",
                "sd1_dir0": {
                    "sd1_d0_dir0": {},
                    "sd1_d0_subds0": {},
                },
                "sd1_ds0": {},  # not registered as subdataset
                "sd1_subds0": {},  # not installed (drop all)
            },
            "dir0": {
                "d0_file0": ""
            },
            "file0": "",
        }
    }

    temp_dir_root = create_temp_dir_tree(ds_tree)

    # create datasets
    root = opj(temp_dir_root, "root")
    superds0 = Dataset(opj(root, "superds0")).create(force=True)
    sd0_subds0 = superds0.create("sd0_subds0", force=True)
    sd0_subds0.create("sd0_sub0_subds0", force=True)
    superds1 = Dataset(opj(root, "superds1")).create(force=True)
    superds1.create(opj("sd1_dir0", "sd1_d0_subds0"), force=True)
    Dataset(opj(root, "superds1", "sd1_ds0")).create(force=True)
    sd1_subds0 = superds1.create("sd1_subds0", force=True)
    sd1_subds0.drop(what='all', reckless='kill', recursive=True)

    yield temp_dir_root

    # delete temp dir
    rmtemp(temp_dir_root)
    assert not os.path.exists(temp_dir_root)


def format_param_ids(val):
    """Helper to format pytest parameter IDs.
    If the parameter is a multiline string, we assume it is the
    parameter 'expected' (expected output of tree), and just
    give it a fixed ID."""
    if isinstance(val, str) and "\n" in val:
        return "expected"


# combinations of parameters to be tested and their expected results.
# (2 levels per param) ** (3 params) = 8 combinations + 8 expected results
matrix_no_ds = [
    {
        "depth": 1,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "3 directories, 0 datasets, 0 files",
        "expected_str": """
├── dir0/
├── dir1/
└── dir2/
"""
    },
    {
        "depth": 3,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "6 directories, 0 datasets, 0 files",
        "expected_str": """
├── dir0/
├── dir1/
└── dir2/
    ├── dir2_dir0/
    ├── dir2_dir1/
    └── dir2_dir2/
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
├── dir0/
├── dir1/
└── dir2/
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
├── dir0/
├── dir1/
|   └── dir1_file0
└── dir2/
    ├── dir2_file0
    ├── dir2_file1
    ├── dir2_dir0/
    ├── dir2_dir1/
    |   └── dir2_dir1_file0
    └── dir2_dir2/
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
├── .dir3/
├── dir0/
├── dir1/
└── dir2/
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
├── .dir3/
|   ├── .dir3_file1
|   └── dir3_file0
├── dir0/
├── dir1/
|   └── dir1_file0
└── dir2/
    ├── dir2_file0
    ├── dir2_file1
    ├── dir2_dir0/
    ├── dir2_dir1/
    |   └── dir2_dir1_file0
    └── dir2_dir2/
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
├── .dir3/
├── dir0/
├── dir1/
└── dir2/
"""
    },
    {
        "depth": 3,
        "include_files": False,
        "include_hidden": True,
        "expected_stats_str": "7 directories, 0 datasets, 0 files",
        "expected_str": """
├── .dir3/
├── dir0/
├── dir1/
└── dir2/
    ├── dir2_dir0/
    ├── dir2_dir1/
    └── dir2_dir2/
"""
    },
]

# for trees with datasets, we test the dataset-specific options
matrix_ds = [
    {
        "depth": 1,
        "datasets_only": False,
        "expected_stats_str": "1 directories, 2 datasets, 0 files",
        "expected_str": """
├── dir0/
├── superds0/  [DS~0]
└── superds1/  [DS~0]
""",
    },
    {
        "depth": 4,
        "datasets_only": False,
        "expected_stats_str": "3 directories, 7 datasets, 0 files",
        "expected_str": """
├── dir0/
├── superds0/  [DS~0]
|   └── sd0_subds0/  [DS~1]
|       └── sd0_sub0_subds0/  [DS~2]
└── superds1/  [DS~0]
    ├── sd1_dir0/
    |   ├── sd1_d0_dir0/
    |   └── sd1_d0_subds0/  [DS~1]
    ├── sd1_ds0/  [DS~0]
    └── sd1_subds0/  [DS~1, not installed]
""",
    },
]


def build_param_matrix(matrix, params):
    """Turn inner dicts into lists (required by pytest parametrize)"""
    matrix_out = []
    for combination in matrix:
        matrix_out.append(
            # order of combinations does not matter
            [val for key, val in combination.items() if key in params]
        )
    return matrix_out


# ================== Test directory tree without datasets ==================

param_names = ["depth", "include_files", "include_hidden", "expected_str"]


@pytest.mark.parametrize(
    param_names, build_param_matrix(matrix_no_ds, param_names),
    ids=format_param_ids
)
def test_print_tree_with_params_no_ds(
        path_no_ds, depth, include_files, include_hidden, expected_str
):
    root = os.path.join(path_no_ds, "root")
    tree = Tree(
        root, max_depth=depth,
        include_files=include_files, include_hidden=include_hidden)
    # skip the first line with the root directory
    # as we will test it separately
    lines = tree.print_line()
    next(lines)  # skip the first line (root dir)
    actual_res = "\n".join(l for l in lines) + "\n"
    expected_res = expected_str.lstrip("\n")  # strip first newline
    assert_str_equal(expected_res, actual_res)


@pytest.mark.parametrize(
    "root_dir_name", ["root/", "root/.", "root/./", "root/../root"]
)
def test_root_path_is_normalized(path_no_ds, root_dir_name):
    """
    Test that root path in the first line of string output
    is normalized path
    """
    root = os.path.join(path_no_ds, root_dir_name)
    tree = Tree(root, max_depth=0)
    root_path = next(tree.print_line())  # first line of tree output
    expected = os.path.join(path_no_ds, "root")
    actual = root_path
    assert_str_equal(expected, actual)


def test_print_tree_fails_for_nonexistent_directory():
    """Obtain nonexistent directory by creating a temp dir
    and deleting it (may be safest method)"""
    dir_name = f"to_be_deleted_{datetime.now().timestamp()}"
    nonexistent_dir = with_tree({dir_name: []})(lambda f: f)()
    with assert_raises(ValueError):
        Tree(nonexistent_dir, max_depth=1)


param_names = ["depth", "include_files", "include_hidden", "expected_stats_str"]

@pytest.mark.parametrize(
    param_names, build_param_matrix(matrix_no_ds, param_names)
)
def test_print_stats_no_ds(
        path_no_ds, depth, include_files, include_hidden, expected_stats_str
):
    root = os.path.join(path_no_ds, 'root')
    tree = Tree(
        root, max_depth=depth,
        include_files=include_files, include_hidden=include_hidden
    ).build()
    actual_res = tree.stats()
    expected_res = expected_stats_str
    assert_str_equal(expected_res, actual_res)


def test_tree_to_string(path_no_ds):
    root = os.path.join(path_no_ds, 'root')
    tree = Tree(root, 3)
    actual = tree.to_string()
    expected = "\n".join(tree._lines)
    assert_str_equal(expected, actual)


# ================== Test directory tree with datasets ==================

param_names = ["depth", "datasets_only", "expected_str"]


@pytest.mark.parametrize(
    param_names, build_param_matrix(matrix_ds, param_names),
    ids=format_param_ids
)
def test_print_tree_with_params_with_ds(
        path_ds, depth, datasets_only, expected_str
):
    root = os.path.join(path_ds, "root")
    tree = Tree(root, max_depth=depth, datasets_only=datasets_only)
    # skip the first line with the root directory
    # as we will test it separately
    lines = tree.print_line()
    next(lines)  # skip the first line (root dir)
    actual_res = "\n".join(l for l in lines) + "\n"
    expected_res = expected_str.lstrip("\n")  # strip first newline
    assert_str_equal(expected_res, actual_res)


param_names = ["depth", "datasets_only", "expected_stats_str"]


@pytest.mark.parametrize(
    param_names, build_param_matrix(matrix_ds, param_names)
)
def test_print_stats_with_ds(
        path_ds, depth, datasets_only, expected_stats_str
):
    root = os.path.join(path_ds, 'root')
    tree = Tree(
        root, max_depth=depth, datasets_only=datasets_only
    ).build()
    actual_res = tree.stats()
    expected_res = expected_stats_str
    assert_str_equal(expected_res, actual_res)


def test_print_tree_full_paths():
    # run in the cwd so detecting full paths is easier
    tree = Tree('.', max_depth=1, full_paths=True)
    # get the second line (first child, hopefully exists)
    lines = tree.print_line()
    next(lines)  # skip the first line (root dir)
    first_child = next(lines)
    assert_re_in(r"(?:└──|├──) \./", first_child)


def test_print_tree_depth_zero(path_no_ds):
    root = os.path.join(path_no_ds, "root")
    tree = Tree(root, max_depth=0,
                include_files=True)  # should have no effect
    actual = tree.to_string()
    expected = root
    assert_str_equal(expected, actual)
