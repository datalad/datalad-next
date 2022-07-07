# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad_osf package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""
'tree'-like command for visualization of dataset hierarchies.

This command covers 2 main use cases:

(1) Glorified `tree` command:
    ----
    As a datalad user, I want to list the contents of a directory tree and
    see which directories are datalad datasets, so that I can locate my
    datasets in the context of the whole directory layout.
    ----
    This is basically what is implemented by the `tree-datalad` utility --
    just `tree` with visual markers for datasets.
    In addition to it, `datalad-tree` provides the following:
    1.  The subdataset hierarchy level information
        (included in the dataset marker, e.g. [DS~0]).
        This is the absolute level, meaning it may take into account
        superdatasets that are not included in the display.
    2.  The option to list only directories that are datasets
    3.  The count of displayed datasets in the "report line"
        (where `tree` only reports count of directories and files)

(2) Descriptor of nested subdataset hierarchies:
    ---
    As a datalad user, I want to visualize the structure of multiple datasets
    and their hierarchies at once based on the subdataset nesting level,
    regardless of their actual depth in the directory tree. This helps me
    understand and communicate the layout of my datasets.
    ---
    This is the more datalad-specific case. Here we redefine 'depth' as the
    level in the subdataset hierarchy instead of the filesystem hierarchy.

"""

__docformat__ = 'restructuredtext'

import json
import logging
import os
from functools import wraps

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.support.exceptions import CapturedException, NoDatasetFound
from datalad.support.param import Parameter
from datalad.distribution.dataset import (
    datasetmethod,
    EnsureDataset,
    require_dataset,
)
from datalad.interface.results import (
    get_status_dict,
)
from datalad.interface.utils import (
    eval_results,
    generic_result_renderer,
)
from datalad.support.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr, EnsureInt, EnsureBool, EnsureRange, Constraints,
)
from datalad.support import ansi_colors

lgr = logging.getLogger('datalad.local.tree')


@build_doc
class TreeCommand(Interface):
    """Visualize directory and dataset hierarchies

    This command mimics the UNIX/MSDOS 'tree' command to display a
    directory tree, highlighting DataLad datasets in the hierarchy.

    """
    result_renderer = 'tailored'

    _params_ = dict(
        path=Parameter(
            args=("path",),
            nargs='?',
            doc="""path to directory from which to generate the tree.
            Defaults to the current directory.""",
            constraints=EnsureStr() | EnsureNone()),
        depth=Parameter(
            args=("-L", "--depth",),
            doc="""maximum level of directory tree to display.
            If not specified, will display all levels.""",
            constraints=EnsureInt() & EnsureRange(min=0) | EnsureNone()),
        dataset_depth=Parameter(
            args=("-R", "--dataset-depth",),
            doc="""maximum level of nested subdatasets to display""",
            constraints=EnsureInt() & EnsureRange(min=0) | EnsureNone()),
        datasets_only=Parameter(
            args=("--datasets-only",),
            doc="""only list directories that are datasets""",
            action='store_true'),
        include_files=Parameter(
            args=("--include-files",),
            doc="""include files in output display""",
            action='store_true'),
        include_hidden=Parameter(
            args=("-a", "--include-hidden",),
            doc="""include hidden files/directories in output""",
            action='store_true'),
        full_paths=Parameter(
            args=("--full-paths",),
            doc="""display the full path for files/directories""",
            action='store_true'),
    )

    _examples_ = [
        dict(
            text="Display up to 3 levels of subdirectories and their contents "
                 "including files, starting from the current directory",
            code_py="tree(depth=3, include_files=True)",
            code_cmd="datalad tree -L 3 --include-files"),
        dict(text="List all first- and second-level subdatasets "
                  "of parent datasets located anywhere under /tmp, "
                  "regardless of directory depth",
             code_py="tree('/tmp', dataset_depth=2, datasets_only=True, full_paths=True)",
             code_cmd="datalad tree /tmp -R 2 --datasets-only --full-paths"),
        dict(text="Display first- and second-level subdatasets and their"
                  "contents up to 3 directories deep (within each subdataset)",
             code_py="tree('.', dataset_depth=2, directory_depth=1)",
             code_cmd="datalad tree -R 2 -L 3"),
    ]

    @staticmethod
    @datasetmethod(name='tree')
    @eval_results
    def __call__(
            path='.',
            *,
            depth=None,
            dataset_depth=None,
            datasets_only=False,
            include_files=False,
            include_hidden=False,
            full_paths=False,
    ):
        # print tree output
        tree = Tree(
            path,
            max_depth=depth,
            dataset_max_depth=dataset_depth,
            datasets_only=datasets_only,
            include_files=include_files,
            include_hidden=include_hidden,
            full_paths=full_paths
        )

        for line in tree.print_line():
            # print one line at a time to improve perceived speed
            print(line)
        print("\n" + tree.stats() + "\n")

        # return a generic OK status
        yield get_status_dict(
            action='tree',
            status='ok',
            path=path,
        )


def increment_node_count(node_generator_func):
    """
    Decorator for incrementing the node count whenever a ``_TreeNode`` is yielded.
    """
    @wraps(node_generator_func)
    def _wrapper(*args, **kwargs):
        self = args[0]   # 'self' is a Tree instance
        for node in node_generator_func(*args, **kwargs):
            node_type = node.__class__.__name__
            if node_type not in self._stats:
                raise ValueError(f"No stats collected for unknown node type '{node_type}'")
            if node.depth > 0:  # we do not count the root directory
                self._stats[node_type] += 1

            yield node  # yield what the generator yielded

    return _wrapper


def is_path_child_of_parent(child, parent):
    parent_abs = os.path.abspath(parent)
    child_abs = os.path.abspath(child)
    return os.path.commonpath([parent_abs]) == \
           os.path.commonpath([parent_abs, child_abs])


class Tree(object):
    """
    Main class for building and serializing a directory tree.
    Does not store ``_TreeNode`` objects, only the string representation
    of the whole tree and the statistics (counts of different node types).
    """

    def __init__(self, root: str, max_depth=None, dataset_max_depth=None,
                 datasets_only=False, include_files=False,
                 include_hidden=False, full_paths=False):

        # TODO: validate parameters
        if not os.path.isdir(root):
            raise ValueError(f"directory '{root}' not found")
        self.root = os.path.normpath(root)

        self.max_depth = max_depth
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        self.dataset_max_depth = dataset_max_depth
        self.datasets_only = datasets_only
        self.include_files = include_files
        self.include_hidden = include_hidden
        self.full_paths = full_paths

        self._lines = []  # holds the list of lines of output string
        self._last_children = []
        # TODO: stats should automatically register all concrete _TreeNode classes
        self._stats = {"DirectoryNode": 0, "DatasetNode": 0, "FileNode": 0}

    def _current_depth(self, path: str):
        """Directory depth of current path relative to root of the tree"""
        # directory depth can be safely inferred from the number of
        # path separators in path, since pathsep characters are illegal
        # in file or directory names.
        return path.count(os.path.sep) - self.root.count(os.path.sep)

    def _is_last_child(self, path):
        """Whether an item is the last child within its subtree"""
        return path in self._last_children

    def _is_max_depth_reached(self, path):
        """
        If max depth is reached, it means we will not traverse
        any further directories in the next iteration.
        However, we will still list any directories or files
        right below the current level.
        Therefore, we 'reach' when we get to 1 level *before* max_depth.
        """
        if self.max_depth is not None:
            return self._current_depth(path) == self.max_depth - 1
        return False  # unlimited depth

    def _is_max_dataset_depth_reached(self, path):
        pass

    def stats(self):
        """
        Equivalent of tree command's 'report line' at the end of the
        tree output.
        The 3 node types (directory, dataset, file) are mutually exclusive,
        so their total is the total count of nodes.
        Only counts contents below the root directory, does not count
        the root itself.
        """
        return f"{self._stats['DirectoryNode']} directories, " \
            f"{self._stats['DatasetNode']} datasets, " \
            f"{self._stats['FileNode']} files"

    def _total_nodes(self):
        return sum(c for c in self._stats.values())

    def build(self):
        """
        Construct the tree string representation and return back the instance.
        """
        self.to_string()
        return self

    @increment_node_count
    def _generate_nodes(self):
        """
        Yields _TreeNode objects, each representing a directory, dataset
        or file. Nodes are traversed in depth-first order.
        """

        # os.walk() does depth-first traversal
        for path, dirs, files in os.walk(self.root):

            # modify os.walk()'s output in-place to prevent
            # traversal into those directories
            if not self.include_hidden:
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                files[:] = [f for f in files if not f.startswith(".")]

            # sort directories and files alphabetically in-place.
            # note that directories and files are sorted separately.
            # files are all listed before the directories
            # (just by convention, no particular reason).
            dirs.sort()
            files.sort()

            # check if node is the last child within its subtree
            # (needed for displaying special end-of-subtree prefix)
            if dirs or files:  # if there is a next level
                # files are listed first, directories come last.
                # so we take the last subdirectory if it exists,
                # otherwise the last file.
                self._last_children.append(
                    os.path.join(path, dirs[-1] if dirs else files[-1])
                )

            current_depth = self._current_depth(path)

            # handle directories/datasets
            dir_or_ds = DirectoryOrDatasetNode(
                path, current_depth, self._is_last_child(path), self.full_paths
            )
            if current_depth == 0 or \
                    not self.datasets_only or \
                    self.datasets_only and isinstance(dir_or_ds, DatasetNode):
                yield dir_or_ds

            if self.max_depth == 0:
                break  # just yield the root dir and exit

            # handle files
            if self.include_files:
                for file in files:
                    file_path = os.path.join(path, file)
                    yield FileNode(
                        file_path, current_depth + 1,
                        self._is_last_child(file_path), self.full_paths
                    )

            if self._is_max_depth_reached(path):
                # generate any remaining directory/dataset nodes,
                # which will not be traversed in the next iteration
                for child_dir in dirs:
                    dir_path = os.path.join(path, child_dir)

                    dir_or_ds = DirectoryOrDatasetNode(
                        dir_path, current_depth + 1,
                        self._is_last_child(dir_path), self.full_paths
                    )

                    if not self.datasets_only or \
                            self.datasets_only and isinstance(dir_or_ds,
                                                              DatasetNode):
                        yield dir_or_ds

                # empty in-place the list of next directories to
                # traverse, which effectively stops os.walk's walking
                dirs[:] = []

    def to_string(self):
        """Return complete tree as string"""
        if not self._lines:
            return "\n".join(list(self.print_line()))
        return "\n".join(self._lines)

    def print_line(self):
        """Generator for tree output lines"""
        if not self._lines:
            # string output has not been generated yet
            for line in self._yield_lines():
                self._lines.append(line)
                yield line
        else:
            # string output is already generated
            for line in self._lines:
                yield line
                yield "\n"  # newline at the very end

    def _yield_lines(self):
        """
        Generator of lines of the tree string representation.
        Each line represents a node (directory or dataset or file).
        A line follows the structure:
            ``[<indentation>] [<branch_tip_symbol>] <path>``
        Example line:
            ``|   |   ├── path_dir_level3``
        """

        # keep track of levels where subtree is exhaused, i.e.
        # we have reached the last child of the subtree.
        # this is needed to build the indentation string for each
        # node, which takes into account whether any parent
        # is the last node of its own subtree.
        levels_with_exhausted_subtree = set([])

        for node in self._generate_nodes():
            lgr.debug(node)

            if node.is_last_child:  # last child of its subtree
                levels_with_exhausted_subtree.add(node.depth)
            else:
                # 'discard' does not raise exception
                # if value does not exist in set
                levels_with_exhausted_subtree.discard(node.depth)

            # build indentation string
            indentation = ""
            if node.depth > 0:
                indentation_symbols_for_levels = [
                    "    "
                    if level in levels_with_exhausted_subtree
                    else "|   "
                    for level in range(1, node.depth)
                ]
                indentation = "".join(indentation_symbols_for_levels)

            line = indentation + str(node)
            yield line


class _TreeNode(object):
    """
    Base class for a directory or file represented as a single
    tree node and printed as single line of the 'tree' output.
    """
    COLOR = None  # ANSI color for the path, if terminal color are enabled

    def __init__(self, path: str, depth: int, is_last_child: bool,
                 use_full_paths=False):
        self.path = path
        self.depth = depth  # depth in the directory tree
        self.is_last_child = is_last_child  # if it is last item of its subtree
        self.use_full_paths = use_full_paths

    def __str__(self):
        if self.depth == 0 or self.use_full_paths:
            path = self.path
        else:
            path = os.path.basename(self.path)

        if self.COLOR is not None:
            path = ansi_colors.color_word(path, self.COLOR)

        prefix = ""
        if self.depth > 0:
            prefix = "└── " if self.is_last_child else "├── "

        return prefix + path

    def _get_tree_root(self):
        """Calculate tree root path from node path and depth"""
        root = self.path
        for _ in range(self.depth):
            root = os.path.dirname(root)
        return root


class DirectoryNode(_TreeNode):
    COLOR = ansi_colors.BLUE

    def __str__(self):
        string = super().__str__()
        if self.depth > 0:
            return string + "/"
        return string


class FileNode(_TreeNode):
    pass


class DirectoryOrDatasetNode(_TreeNode):
    """
    Factory class for creating either a ``DirectoryNode`` or ``DatasetNode``,
    based on whether the current path is a dataset or not.
    """
    def __new__(cls, path, *args, **kwargs):
        if cls.is_dataset(path):
            ds_node = DatasetNode(path, *args, **kwargs)
            ds_node.calculate_dataset_depth()
            return ds_node
        else:
            return DirectoryNode(path, *args, **kwargs)

    @staticmethod
    def is_dataset(path):
        """
        We infer that a directory is a dataset if it is either:
        (A) installed, or
        (B) not installed, but it has an installed superdatset.
        """
        ds = require_dataset(path, check_installed=False)
        if ds.is_installed():
            return True

        # check if it has an installed superdataset
        superds = ds.get_superdataset(datalad_only=True, topmost=False,
                                      registered_only=True)
        return superds is not None


class DatasetNode(DirectoryNode):
    COLOR = ansi_colors.MAGENTA

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ds = require_dataset(self.path, check_installed=False)
        self.is_installed = self.ds.is_installed()
        self._ds_depth = None
        self._absolute_ds_depth = None

    def __str__(self):
        install_flag = ", not installed" if not self.is_installed else ""
        suffix = f"  [DS~{self._absolute_ds_depth}{install_flag}]"
        return super().__str__() + suffix

    def calculate_dataset_depth(self):
        """
        Calculate 2 measures of a dataset's nesting depth/level:
        1. subdataset depth relative to the tree root
        2. absolute subdataset depth in the full hierarchy
        """
        self._ds_depth = 0
        self._absolute_ds_depth = 0

        ds = self.ds

        while ds:
            superds = ds.get_superdataset(
                datalad_only=True, topmost=False, registered_only=True)

            if superds is None:
                # it is not a dataset, do nothing
                break
            else:
                if superds == ds:
                    # it is a top-level dataset, we are done
                    break

                self._absolute_ds_depth += 1
                if is_path_child_of_parent(superds.path, self._get_tree_root()):
                    # if the parent dataset is underneath the tree
                    # root, we increment the relative depth
                    self._ds_depth += 1

            ds = superds
