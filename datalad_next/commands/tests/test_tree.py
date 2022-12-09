from contextlib import contextmanager
from pathlib import Path
from os import sep

import pytest
from datalad_next.tests.utils import (
    BasicGitTestRepo,
    assert_raises,
    assert_str_equal,
    with_tree,
    ok_exists,
    get_deeply_nested_structure,
    skip_wo_symlink_capability,
    skip_if_on_windows,
    ok_good_symlink,
    ok_broken_symlink,
    run_main,
)
from datalad_next.utils import (
    rmtemp,
    make_tempfile,
    chpwd
)
from datalad_next.uis import ui_switcher as ui

from datalad_next.datasets import Dataset

from ..tree import (
    Tree,
    TreeCommand
)

"""Tests for the ``datalad tree`` command."""


# ============================ Helper functions ===============================

@contextmanager
def ensure_no_permissions(path: Path):
    """Remove all permissions for given file/directory and restore the
    original permissions at the end"""

    # modeled after 'datalad.utils.ensure_write_permission'
    original_mode = path.stat().st_mode
    try:
        path.chmod(0o000)
        yield
    finally:
        try:
            path.chmod(original_mode)
        except FileNotFoundError:
            # ignore error if path was deleted in the context block
            pass


def create_temp_dir_tree(tree_dict: dict) -> Path:
    """
    Create a temporary directory tree.

    This is a shim for the ``with_tree()`` decorator so it can be used
    in a module-scoped pytest fixture.

    Parameters
    ----------
    tree_dict: dict
        A dict describing a directory tree (see parameter of ``with_tree``)

    Returns
    -------
    Path
        Root directory of the newly created tree
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
    return Path(temp_dir_root).resolve()


@pytest.fixture(scope="function")
def path():
    """Generic fixture for creating a temporary directory tree.

    TODO: harness pytest's native ``tmp_path`` / ``tmp_path_factory``
    fixtures"""
    temp_dir_root = create_temp_dir_tree({})  # empty directory
    yield temp_dir_root
    rmtemp(temp_dir_root)
    assert not temp_dir_root.exists()


@pytest.fixture(scope="module")
def path_no_ds():
    """Fixture for creating a temporary directory tree (**without** datasets)
    to be used in tests.

    Returns
    -------
    Path
        Root directory of the newly created tree
    """
    dir_tree = {
        "root": {
            ".dir3": {
                "dir3_file0": '',
                ".dir3_file1": '',
            },
            "dir0": {},
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
    assert not temp_dir_root.exists()


@pytest.fixture(scope="module")
def path_ds():
    """Fixture for creating a temporary directory tree (**including** datasets)
    to be used in tests.

    Returns
    -------
    Path
        Root directory of the newly created tree
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
                    "sd1_d0_repo0": {},
                    "sd1_d0_subds0": {},
                },
                "sd1_ds0": {},  # not registered as subdataset
                "sd1_subds0": {},  # not installed (drop all)
            },
            # plain git repo (contents are defined in BasicGitTestRepo)
            "repo0": {},
            "file0": "",
        }
    }

    temp_dir_root = create_temp_dir_tree(ds_tree)

    # create datasets / repos
    root = temp_dir_root / "root"
    BasicGitTestRepo(path=root / "repo0", puke_if_exists=False)
    ckwa = dict(force=True, result_renderer="disabled")
    superds0 = Dataset(root / "superds0").create(**ckwa)
    sd0_subds0 = superds0.create("sd0_subds0", **ckwa)
    sd0_subds0.create("sd0_sub0_subds0", **ckwa)
    superds1 = Dataset(root / "superds1").create(**ckwa)
    superds1.create(Path("sd1_dir0") / "sd1_d0_subds0", **ckwa)
    Dataset(root / "superds1" / "sd1_ds0").create(**ckwa)
    BasicGitTestRepo(
        path=root / "superds1" / "sd1_dir0" / "sd1_d0_repo0",
        puke_if_exists=False)
    sd1_subds0 = superds1.create("sd1_subds0", **ckwa)
    sd1_subds0.drop(what='all', reckless='kill',
                    recursive=True, result_renderer='disabled')

    yield temp_dir_root

    # delete temp dir
    rmtemp(temp_dir_root)
    assert not temp_dir_root.exists()


def get_tree_rendered_output(tree_cmd: list, exit_code: int = 0):
    """
    Run 'tree' CLI command with the given list of arguments and
    return the output of the custom results renderer, broken down into
    3 components (tree root, tree body, report line).

    Assumes command exit code 0 and no additional logging to stdout.

    Parameters
    ----------
    tree_cmd: list(str)
        'tree' command given as list of strings
    exit_code: int
        Expected exit code of command (default: 0)

    Returns
    -------
    Tuple[str, str, str]
        3-value tuple consisting of: tree root, tree body, report line
    """
    # remove any empty strings from command
    out, _ = run_main([c for c in tree_cmd if c != ''], exit_code=exit_code)

    # remove trailing newline
    lines = out.rstrip("\n").split("\n")

    root = lines[0]  # first line of tree output
    body = "\n".join(lines[1:-1])
    report = lines[-1]

    return root, body, report


@pytest.fixture(scope="class")
def inject_path(request, path_ds, path_no_ds):
    """
    Set a path fixture (root path of temp directory tree) as class attribute,
    to make it available to all tests in the class. The fixture is chosen based
    on the class' ``tree_with_ds`` attribute.
    """
    if request.cls.tree_with_ds:
        request.cls.path = path_ds
    else:
        request.cls.path = path_no_ds


def format_param_ids(val) -> str:
    """
    Helper to format pytest parameter IDs.

    If the parameter is a multiline string, we assume it is the
    parameter 'expected' (expected output of tree), and just
    give it a fixed ID (otherwise, it would be displayed in the
    parameter list as a long unreadable string).

    Parameters
    ----------
    val
        Parameter value
    """
    if isinstance(val, str) and "\n" in val:
        return "expected"


def build_param_matrix(matrix, params):
    """Turn inner dicts into lists (required by pytest parametrize)"""
    matrix_out = []
    for combination in matrix:
        matrix_out.append(
            # order of combinations does not matter
            [val for key, val in combination.items() if key in params]
        )
    return matrix_out


def pytest_generate_tests(metafunc):
    """Pytest helper to automatically configure parametrization.

    Avoids having to duplicate definition of parameter names and values
    across tests that use the same data.

    See: https://docs.pytest.org/en/7.1.x/example/parametrize.html#parametrizing-test-methods-through-per-class-configuration
    """
    if metafunc.cls and \
            hasattr(metafunc.cls, 'params') and \
            hasattr(metafunc.cls, 'MATRIX'):
        test_id = metafunc.function.__name__
        test_params_dict = metafunc.cls.params
        matrix = metafunc.cls.MATRIX
        if test_id in metafunc.cls.params:
            param_names = test_params_dict[test_id]
            metafunc.parametrize(
                param_names,
                build_param_matrix(matrix, param_names),
                ids=format_param_ids
            )

# ================================= Tests =====================================


@pytest.mark.usefixtures("inject_path")
class TestTree:
    """Base class with tests that should run for multiple Tree
    configurations.

    Configurations are defined by:

    - ``MATRIX``: dicts of pytest parameters and their values, where each dict
      corresponds to a separate parametrized test instance.
    - ``params``: a dict defining for each test method, which parameters
      will be used in that test (from the parameter names contained in
      ``MATRIX``).
    """
    __test__ = False  # tells pytest to not collect tests in this class
    tree_with_ds = False
    path = None  # will be set by the inject_* fixture to temp dir tree root

    # matrix of combinations of parameters to be tested and their
    # expected results
    MATRIX = []

    # dict specifying parameter sets for each test method
    params = {
        "test_print_tree": [
            "depth", "include_files", "include_hidden", "expected_str"
        ],
        "test_print_stats": [
            "depth", "include_files", "include_hidden", "expected_stats_str"
        ],
        "test_exhausted_levels_are_below_current_depth": [
            "depth", "include_files", "include_hidden"
        ]
    }


class TestTreeWithoutDatasets(TestTree):
    """Test directory tree without any datasets"""

    __test__ = True
    tree_with_ds = False

    MATRIX = [
    {
        "depth": 1,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "0 datasets, 3 directories",
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
        "expected_stats_str": "0 datasets, 6 directories",
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
        "expected_stats_str": "0 datasets, 3 directories, 2 files",
        "expected_str": """
├── dir0/
├── dir1/
├── dir2/
├── file0
└── file1
"""
    },
    {
        "depth": 3,
        "include_files": True,
        "include_hidden": False,
        "expected_stats_str": "0 datasets, 6 directories, 8 files",
        "expected_str": """
├── dir0/
├── dir1/
│   └── dir1_file0
├── dir2/
│   ├── dir2_dir0/
│   ├── dir2_dir1/
│   │   └── dir2_dir1_file0
│   ├── dir2_dir2/
│   │   ├── dir2_dir2_file0
│   │   └── dir2_dir2_file1
│   ├── dir2_file0
│   └── dir2_file1
├── file0
└── file1
"""
    },
    {
        "depth": 1,
        "include_files": True,
        "include_hidden": True,
        "expected_stats_str": "0 datasets, 4 directories, 3 files",
        "expected_str": """
├── .dir3/
├── .file2
├── dir0/
├── dir1/
├── dir2/
├── file0
└── file1
"""
    },
    {
        "depth": 3,
        "include_files": True,
        "include_hidden": True,
        "expected_stats_str": "0 datasets, 7 directories, 11 files",
        "expected_str": """
├── .dir3/
│   ├── .dir3_file1
│   └── dir3_file0
├── .file2
├── dir0/
├── dir1/
│   └── dir1_file0
├── dir2/
│   ├── dir2_dir0/
│   ├── dir2_dir1/
│   │   └── dir2_dir1_file0
│   ├── dir2_dir2/
│   │   ├── dir2_dir2_file0
│   │   └── dir2_dir2_file1
│   ├── dir2_file0
│   └── dir2_file1
├── file0
└── file1
"""
    },
    {
        "depth": 1,
        "include_files": False,
        "include_hidden": True,
        "expected_stats_str": "0 datasets, 4 directories",
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
        "expected_stats_str": "0 datasets, 7 directories",
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

    def test_print_tree(
            self, depth, include_files, include_hidden, expected_str
    ):
        root = str(self.path / "root")
        command = [
            'tree',
            root,
            '--depth', str(depth),
            '--include-hidden' if include_hidden else '',
            '--include-files' if include_files else ''
        ]
        _, actual_res, _ = get_tree_rendered_output(command)
        expected_res = expected_str.lstrip("\n")  # strip first newline
        ui.message("expected:")
        ui.message(expected_res)
        ui.message("actual:")
        ui.message(actual_res)
        assert_str_equal(expected_res, actual_res)

    def test_print_stats(
            self, depth, include_files, include_hidden, expected_stats_str
    ):
        root = str(self.path / 'root')
        command = [
            'tree',
            root,
            '--depth', str(depth),
            '--include-hidden' if include_hidden else '',
            '--include-files' if include_files else ''
        ]
        _, _, actual_res = get_tree_rendered_output(command)
        expected_res = expected_stats_str
        assert_str_equal(expected_res, actual_res)

    @pytest.mark.parametrize(
        "root_dir_name", ["root/", "root/.", "root/./", "root/../root"]
    )
    def test_root_path_is_normalized(self, root_dir_name):
        """
        Test that root path in the first line of string output
        is normalized path
        """
        root = str(self.path / root_dir_name)
        command = ['tree', root, '--depth', '0']
        actual, _, _ = get_tree_rendered_output(command)
        expected = str(self.path / "root")
        assert_str_equal(expected, actual)

    def test_no_difference_if_root_path_absolute_or_relative(self):
        """Tree output should be identical whether the root directory
        is given as absolute or relative path"""
        root = str(self.path / "root")
        output_abs_path = get_tree_rendered_output(['tree', root])
        with chpwd(root):
            output_rel_path = get_tree_rendered_output(['tree', '.'])

        assert output_abs_path == output_rel_path

    def test_print_tree_depth_zero(self):
        root = str(self.path / "root")
        # including files should have no effect
        command = ['tree', root, '--depth', '0', '--include-files']
        actual = get_tree_rendered_output(command)
        expected = (root, '', '0 datasets, 0 directories, 0 files')
        assert expected == actual

    def test_exhausted_levels_are_below_current_depth(
            self, depth, include_files, include_hidden):
        """For each node, the exhausted levels reported for that node
        should be smaller or equal to the node's depth"""

        results = TreeCommand.__call__(
            self.path,
            depth=depth,
            include_files=include_files,
            include_hidden=include_hidden,
            result_renderer="disabled",
            # return only 'depth' and 'exhausted_levels' from result dicts
            result_xfm=lambda res: {k: res[k]
                                    for k in ("depth", "exhausted_levels")}
        )
        # sanity checks
        assert len(results) > 1
        assert any(res["exhausted_levels"] for res in results)

        # actual test
        assert all(level <= res["depth"]
                   for res in results
                   for level in res["exhausted_levels"])


class TestTreeWithDatasets(TestTreeWithoutDatasets):
    """Test directory tree with datasets"""

    __test__ = True
    tree_with_ds = True
    # set `include_files` and `include_hidden` to False,
    # they should be already covered in `TestTreeWithoutDatasets`
    MATRIX = [
    {
        "depth": 1,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "2 datasets, 1 directory",
        "expected_str": """
├── repo0/
├── [DS~0] superds0/
└── [DS~0] superds1/
""",
    },
    {
        "depth": 4,
        "include_files": False,
        "include_hidden": False,
        "expected_stats_str": "7 datasets, 3 directories",
        "expected_str": """
├── repo0/
├── [DS~0] superds0/
│   └── [DS~1] sd0_subds0/
│       └── [DS~2] sd0_sub0_subds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    │   ├── sd1_d0_repo0/
    │   └── [DS~1] sd1_d0_subds0/
    ├── [DS~0] sd1_ds0/
    └── [DS~1] (not installed) sd1_subds0/
""",
    },
    ]


class TestDatasetTree(TestTree):
    """Test dataset tree with max_dataset_depth parameter"""

    __test__ = True
    tree_with_ds = True
    MATRIX = [
    {
        "dataset_depth": 0,
        "depth": 0,
        "expected_stats_str": "3 datasets, 0 directories",
        "expected_str": """
├── [DS~0] superds0/
└── [DS~0] superds1/
    └── [DS~0] sd1_ds0/
"""
    },
    {
        "dataset_depth": 0,
        "depth": 1,
        "expected_stats_str": "3 datasets, 1 directory",
        "expected_str": """
├── [DS~0] superds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    └── [DS~0] sd1_ds0/
"""
    },
    {
        "dataset_depth": 0,
        "depth": 2,
        "expected_stats_str": "3 datasets, 2 directories",
        "expected_str": """
├── [DS~0] superds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    │   └── sd1_d0_repo0/
    └── [DS~0] sd1_ds0/
"""
    },
    {
        "dataset_depth": 1,
        "depth": 0,
        "expected_stats_str": "6 datasets, 1 directory",
        "expected_str": """
├── [DS~0] superds0/
│   └── [DS~1] sd0_subds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    │   └── [DS~1] sd1_d0_subds0/
    ├── [DS~0] sd1_ds0/
    └── [DS~1] (not installed) sd1_subds0/
"""
    },
    {
        "dataset_depth": 1,
        "depth": 2,
        "expected_stats_str": "6 datasets, 2 directories",
        "expected_str": """
├── [DS~0] superds0/
│   └── [DS~1] sd0_subds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    │   ├── sd1_d0_repo0/
    │   └── [DS~1] sd1_d0_subds0/
    ├── [DS~0] sd1_ds0/
    └── [DS~1] (not installed) sd1_subds0/
"""
    },
    {
        "dataset_depth": None,
        "depth": 0,
        "expected_stats_str": "7 datasets, 1 directory",
        "expected_str": """
├── [DS~0] superds0/
│   └── [DS~1] sd0_subds0/
│       └── [DS~2] sd0_sub0_subds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    │   └── [DS~1] sd1_d0_subds0/
    ├── [DS~0] sd1_ds0/
    └── [DS~1] (not installed) sd1_subds0/
"""
    },
    {
        "dataset_depth": None,
        "depth": 2,
        "expected_stats_str": "7 datasets, 2 directories",
        "expected_str": """
├── [DS~0] superds0/
│   └── [DS~1] sd0_subds0/
│       └── [DS~2] sd0_sub0_subds0/
└── [DS~0] superds1/
    ├── sd1_dir0/
    │   ├── sd1_d0_repo0/
    │   └── [DS~1] sd1_d0_subds0/
    ├── [DS~0] sd1_ds0/
    └── [DS~1] (not installed) sd1_subds0/
"""
    },
    ]

    params = {
        "test_print_tree": [
            "dataset_depth", "depth", "expected_str"
        ],
        "test_print_stats": [
            "dataset_depth", "depth", "expected_stats_str"
        ]
    }

    def test_print_tree(
            self, dataset_depth, depth, expected_str
    ):
        root = str(self.path / "root")

        recursive_opts = ["--recursive"]
        if dataset_depth is not None:
            recursive_opts = ['--recursion-limit', str(dataset_depth)]

        command = [
            'tree',
            root,
            '--depth', str(depth),
            *recursive_opts
        ]
        _, actual_res, _ = get_tree_rendered_output(command)
        expected_res = expected_str.lstrip("\n")  # strip first newline
        ui.message("expected:")
        ui.message(expected_res)
        ui.message("actual:")
        ui.message(actual_res)
        assert_str_equal(expected_res, actual_res)

    def test_print_tree_without_datasets(self):
        """If there are no datasets, should only print the root"""
        root = str(self.path / "root" / "repo0")
        command = [
            'tree',
            root,
            '--depth', '10',
            '--recursive',
            '--include-files'
        ]
        _, actual_res, _ = get_tree_rendered_output(command)
        expected_res = ""
        ui.message("expected:")
        ui.message(expected_res)
        ui.message("actual:")
        ui.message(actual_res)
        assert_str_equal(expected_res, actual_res)

    def test_print_stats(
            self, dataset_depth, depth, expected_stats_str
    ):
        root = str(self.path / "root")

        recursive_opts = ["--recursive"]
        if dataset_depth is not None:
            recursive_opts = ['--recursion-limit', str(dataset_depth)]

        command = [
            'tree',
            root,
            '--depth', str(depth),
            *recursive_opts
        ]
        _, _, actual_res = get_tree_rendered_output(command)
        expected_res = expected_stats_str
        assert_str_equal(expected_res, actual_res)


class TestTreeFilesystemIssues:
    """Test tree with missing permissions, broken symlinks, etc."""

    def test_print_tree_fails_for_nonexistent_directory(self):
        """Obtain nonexistent directory by creating a temp dir and deleting it
        (may be safest method)"""
        with make_tempfile(mkdir=True) as nonexistent_dir:
            ok_exists(nonexistent_dir)  # just wait for it to be deleted
        with assert_raises(ValueError):
            Tree(Path(nonexistent_dir), max_depth=1)

    @skip_if_on_windows
    @skip_wo_symlink_capability
    def test_print_tree_permission_denied(self, path):
        """
        - If the tree contains a directory for which the user has no
          permissions (so it would not be possible to traverse it), a message
          should be displayed next to the affected directory path
        - The rest of the tree following the forbidden directory should
          be printed as usual
        - The command should return error exit status but not crash
        """
        (Path(path) / 'z_dir' / 'subdir').mkdir(parents=True)
        forbidden_dir = Path(path) / 'a_forbidden_dir'
        forbidden_dir.mkdir(parents=True)
        # temporarily remove all permissions (octal 000)
        # restore permissions at the end, otherwise we can't delete temp dir
        with ensure_no_permissions(forbidden_dir):
            command = ['tree', str(path), '--depth', '2']
            # expect exit code 1
            _, actual, _ = get_tree_rendered_output(command, exit_code=1)
            expected = f"""
├── {forbidden_dir.name}/ [error opening dir]
└── z_dir/
    └── subdir/
""".lstrip("\n")
            ui.message("expected:")
            ui.message(expected)
            ui.message("actual:")
            ui.message(actual)
            assert_str_equal(expected, actual)

    @skip_wo_symlink_capability
    @pytest.mark.parametrize("include_files", (True, False))
    def test_tree_with_broken_symlinks(self, path, include_files):
        """Test that broken symlinks are reported as such"""
        # prep
        dir1 = path / 'real' / 'dir1'
        file1 = path / 'real' / 'dir1' / 'file1'
        dir1.mkdir(parents=True)
        file1.touch()
        (path / 'links').mkdir()

        # create symlinks
        # 1. broken symlink pointing to non-existent target
        link_to_nonexistent = path / 'links' / '1_link_to_nonexistent'
        link_to_nonexistent.symlink_to(path / 'nonexistent')
        ok_broken_symlink(link_to_nonexistent)
        # 2. broken symlink pointing to itself
        link_to_self = path / 'links' / '2_link_to_self'
        link_to_self.symlink_to(link_to_self)
        with assert_raises((RuntimeError, OSError)):  # OSError on Windows
            # resolution should fail because of infinite loop
            link_to_self.resolve()

        # 3. good symlink pointing to existing directory
        link_to_dir1 = path / 'links' / '3_link_to_dir1'
        link_to_dir1.symlink_to(dir1, target_is_directory=True)
        ok_good_symlink(link_to_dir1)
        # 4. good symlink pointing to existing file
        link_to_file1 = path / 'links' / '4_link_to_file1'
        link_to_file1.symlink_to(file1)
        ok_good_symlink(link_to_file1)

        # test results dict using python API
        # implicitly also tests that command yields tree without crashing
        actual = TreeCommand.__call__(
            path,
            depth=None,  # unlimited
            include_files=include_files,
            result_renderer="disabled",
            result_xfm=lambda res: (Path(res["path"]).name,
                                    res["is_broken_symlink"]),
            result_filter=lambda res: "is_broken_symlink" in res,
            return_type="list",
            on_failure="ignore"
        )

        if include_files:
            expected = [
                # (path, is_broken_symlink)
                (link_to_nonexistent.name, True),
                (link_to_self.name, True),
                (link_to_dir1.name, False),
                (link_to_file1.name, False)
            ]
        else:
            expected = [
                (link_to_dir1.name, False)
            ]
        assert set(expected) == set(actual)

    @skip_if_on_windows
    @skip_wo_symlink_capability
    @pytest.mark.parametrize("include_files", (True, False))
    def test_tree_with_broken_symlinks_to_inaccessible_targets(
            self, path, include_files):
        """Test that symlinks to targets underneath inaccessible directories
        are reported as broken, whereas symlinks to inaccessible
        file/directories themselves are not reported as broken."""
        # prep
        root = path / "root"  # tree root
        root.mkdir(parents=True)

        # create file and directory without permissions outside of tree
        # root (permissions will be removed later ad-hoc, because need to
        # create symlinks first)
        forbidden_file = path / "forbidden_file"
        forbidden_file.touch()  # permissions will be removed later ad-hoc
        forbidden_dir = path / "forbidden_dir"
        forbidden_dir.mkdir()
        file_in_forbidden_dir = forbidden_dir / "file_in_forbidden_dir"
        file_in_forbidden_dir.touch()
        dir_in_forbidden_dir = forbidden_dir / "dir_in_forbidden_dir"
        dir_in_forbidden_dir.mkdir()

        # create symlinks
        # 1. broken symlink pointing to file under inaccessible directory
        link_to_file_in_forbidden_dir = root / "1_link_to_file_in_forbidden_dir"
        link_to_file_in_forbidden_dir.symlink_to(file_in_forbidden_dir)
        with ensure_no_permissions(forbidden_dir):
            with assert_raises(PermissionError):
                # resolution should fail because of missing permissions
                link_to_file_in_forbidden_dir.resolve(strict=True)

        # 2. broken symlink pointing to directory under inaccessible directory
        link_to_dir_in_forbidden_dir = root / "2_link_to_dir_in_forbidden_dir"
        link_to_dir_in_forbidden_dir.symlink_to(dir_in_forbidden_dir)
        with ensure_no_permissions(forbidden_dir):
            with assert_raises(PermissionError):
                # resolution should fail because of missing permissions
                link_to_dir_in_forbidden_dir.resolve(strict=True)

        # 3. good symlink pointing to existing but inaccessible directory
        link_to_forbidden_dir = root / "3_link_to_forbidden_dir"
        link_to_forbidden_dir.symlink_to(forbidden_dir, target_is_directory=True)
        with ensure_no_permissions(forbidden_dir):
            ok_good_symlink(link_to_forbidden_dir)

        # 4. good symlink pointing to existing but inaccessible file
        link_to_forbidden_file = root / "4_link_to_forbidden_file"
        link_to_forbidden_file.symlink_to(forbidden_file)
        with ensure_no_permissions(forbidden_file):
            ok_good_symlink(link_to_forbidden_file)

        # temporarily remove all permissions (octal 000)
        # restore permissions at the end, otherwise we can't delete temp dir
        with ensure_no_permissions(forbidden_dir), \
                ensure_no_permissions(forbidden_file):

            # test results dict using python API
            # implicitly also tests that command yields tree without crashing
            actual = TreeCommand.__call__(
                root,
                depth=None,
                include_files=include_files,
                result_renderer="disabled",
                result_xfm=lambda res: (Path(res["path"]).name,
                                        res["is_broken_symlink"]),
                result_filter=lambda res: "is_broken_symlink" in res,
                return_type="list",
                on_failure="ignore"
            )

        if include_files:
            expected = [
                # (path, is_broken_symlink)
                (link_to_file_in_forbidden_dir.name, True),
                (link_to_dir_in_forbidden_dir.name, True),
                (link_to_forbidden_dir.name, False),
                (link_to_forbidden_file.name, False)
            ]
        else:
            expected = [
                (link_to_forbidden_dir.name, False)
            ]
        assert set(expected) == set(actual)

    @skip_wo_symlink_capability
    def test_print_tree_with_recursive_symlinks(self, path):
        """
        TODO: break down into separate tests

        - Symlinks targets are displayed in custom renderer output
        - We do not follow symlinks that point to directories underneath
          the tree root or its parent (to prevent duplicate subtrees)
        - Symlinks pointing to datasets are not considered dataset nodes
          themselves, but regular directories (to prevent duplicate counts
          of datasets)
        """
        ds = get_deeply_nested_structure(str(path / 'superds'))

        # change current dir to create symlinks with relative path
        with chpwd(ds.path):
            # create symlink to a sibling directory of the tree
            # (should be recursed into)
            (path / 'ext_dir' / 'ext_subdir').mkdir(parents=True)
            Path('link2extdir').symlink_to(Path('..') / 'ext_dir',
                                           target_is_directory=True)

            # create symlink to grandparent of the tree root (should NOT
            # be recursed into)
            Path('link2parent').symlink_to(Path('..') / '..',
                                           target_is_directory=True)

            # create symlink to subdir of the tree root at depth > max_depth
            # (should be recursed into)
            deepdir = Path('subds_modified') / 'subdir' / 'deepdir'
            deepdir.mkdir()
            (deepdir / 'subdeepdir').mkdir()
            Path('link2deepdir').symlink_to(deepdir, target_is_directory=True)

        root = ds.path
        command = ["tree", "--depth", "2", root]
        _, actual_res, counts = get_tree_rendered_output(command)
        s = sep
        expected_res = f"""
├── directory_untracked/
│   └── link2dir/ -> ..{s}subdir
├── link2deepdir/ -> subds_modified{s}subdir{s}deepdir
│   └── subdeepdir/
├── link2dir/ -> subdir
├── link2extdir/ -> ..{s}ext_dir
│   └── ext_subdir/
├── link2parent/ -> ..{s}..
├── link2subdsdir/ -> subds_modified{s}subdir
├── link2subdsroot/ -> subds_modified
├── subdir/
└── [DS~1] subds_modified/
    ├── link2superdsdir/ -> ..{s}subdir
    ├── subdir/
    └── [DS~2] subds_lvl1_modified/
""".lstrip("\n")

        # Compare with output of 'tree' command
        # ui.message(counts)
        # import subprocess
        # subprocess.run(["tree", "-dlL", "2", root])

        ui.message("expected:")
        ui.message(expected_res)
        ui.message("actual:")
        ui.message(actual_res)
        assert_str_equal(expected_res, actual_res)
