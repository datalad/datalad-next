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

import logging
import os
from functools import wraps, lru_cache
from pathlib import Path

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.support.param import Parameter
from datalad.distribution.dataset import (
    datasetmethod,
    require_dataset,
)
from datalad.interface.results import (
    get_status_dict,
)
from datalad.interface.utils import (
    eval_results,
)
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr, EnsureInt, EnsureRange,
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
             code_py="tree('/tmp', dataset_depth=2, depth=0, full_paths=True)",
             code_cmd="datalad tree /tmp -R 2 -L 0 --full-paths"),
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
            include_files=False,
            include_hidden=False,
            full_paths=False,
    ):
        if dataset_depth is not None:
            tree = DatasetTree(
                path,
                max_depth=depth,
                max_dataset_depth=dataset_depth,
                include_files=include_files,
                include_hidden=include_hidden,
                full_paths=full_paths
            )
        else:
            tree = Tree(
                path,
                max_depth=depth,
                include_files=include_files,
                include_hidden=include_hidden,
                full_paths=full_paths
            )

        for line in tree.print_line():
            # print one line at a time to improve UX / perceived speed
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
    Decorator for incrementing the node count whenever
    a ``_TreeNode`` is generated.
    """
    @wraps(node_generator_func)
    def _wrapper(*args, **kwargs):
        self = args[0]   # 'self' is a Tree instance
        for node in node_generator_func(*args, **kwargs):
            node_type = node.__class__.__name__
            if node_type not in self._stats:
                raise ValueError(
                    f"No stats collected for unknown node type '{node_type}'"
                )
            if node.depth > 0:  # we do not count the root directory
                self._stats[node_type] += 1

            yield node  # yield what the generator yielded

    return _wrapper


# whether path is a dataset should not change within
# command execution time, so we cache it
@lru_cache
def is_dataset(path):
    """
    Fast dataset detection.
    Infer that a directory is a dataset if it is either:
    (A) installed, or
    (B) not installed, but has an installed superdatset.
    Only consider datalad datasets, not plain git/git-annex repos.
    """
    ds = require_dataset(path, check_installed=False)

    # detect if it is an installed datalad-proper dataset
    # (as opposed to git/git-annex repo).
    # could also query `ds.id`, but checking just for existence
    # of config file is quicker.
    if os.path.isfile(os.path.join(ds.path, ".datalad", "config")):
        return True

    # if it is not installed, check if it has an installed superdataset.
    # instead of querying ds.is_installed() (which checks if the
    # directory has the .git folder), we check if the directory
    # is empty (faster) -- as e.g. after a non-recursive `datalad clone`
    def is_empty_dir():
        with os.scandir(path) as contents:
            if any(contents):
                return False
        return True

    if is_empty_dir():
        superds = ds.get_superdataset(datalad_only=True, topmost=False,
                                      registered_only=True)
        if superds is not None:
            return True

    # TODO: do we have a way to detect a datalad dataset if it
    # is not installed and it is not a subdataset?
    return False


def is_path_child_of_parent(child, parent):
    parent_abs = os.path.abspath(parent)
    child_abs = os.path.abspath(child)
    return os.path.commonpath([parent_abs]) == \
        os.path.commonpath([parent_abs, child_abs])

class _TreeNode(object):
    """
    Base class for a directory or file represented as a single
    tree node and printed as single line of the 'tree' output.
    """
    COLOR = None  # ANSI color for the path, if terminal color are enabled

    def __init__(self, path: str, depth: int, is_last_child: bool,
                 use_full_paths=False):
        self.path = path  # path relative to tree root
        self.depth = depth  # depth in the directory tree
        self.is_last_child = is_last_child  # if it is last item of its subtree
        self.use_full_paths = use_full_paths

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(self.path)

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

        return prefix + str(path)

    def _get_tree_root(self):
        """Calculate tree root path from node path and depth"""
        root = self.path
        for _ in range(self.depth):
            root = os.path.dirname(root)
        return root



class Tree(object):
    """
    Main class for building and serializing a directory tree.
    Does not store ``_TreeNode`` objects, only the string representation
    of the whole tree and the statistics (counts of different node types).
    """

    def __init__(self, root: Path, max_depth=None,
                 include_files=False, include_hidden=False,
                 full_paths=False, exclude_datasets=False,
                 skip_root=False):

        # TODO: root should already be given as Path object
        self.root = Path(root).resolve()
        if not self.root.is_dir():
            raise ValueError(f"directory '{root}' not found")

        self.max_depth = max_depth
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        self.include_files = include_files
        self.include_hidden = include_hidden
        self.full_paths = full_paths
        self.exclude_datasets = exclude_datasets
        self.skip_root = skip_root  # do not print first line with root path

        self._lines = []  # list of lines of output string
        # TODO: stats should automatically register all concrete _TreeNode classes
        self._stats = {"DirectoryNode": 0, "DatasetNode": 0, "FileNode": 0}

    def _get_depth(self, path: Path):
        """Directory depth of current path relative to root of the tree"""
        return len(path.relative_to(self.root).parts)

    def _is_max_depth_reached(self, path):
        if self.max_depth is not None:
            return self._get_depth(path) == self.max_depth
        return False  # unlimited depth

    def stats(self):
        """
        Equivalent of tree command's 'report line' at the end of the
        tree output.
        The 3 node types (directory, dataset, file) are mutually exclusive,
        so their sum equals to the total node count.
        Does not count the root itself, only the contents below the root.
        """
        return f"{self._stats['DatasetNode']} datasets, " \
            f"{self._stats['DirectoryNode']} directories, " \
            f"{self._stats['FileNode']} files"

    def _total_nodes(self):
        return sum(c for c in self._stats.values())

    def build(self):
        """
        Construct the tree string representation and return back the instance.
        """
        self.to_string()
        return self

    def _new_node(self, node: _TreeNode):
        """A helper to instantiate TreeNode and increment node count"""
        node_type = node.__class__.__name__
        if node_type not in self._stats:
            raise ValueError(
                f"No stats collected for unknown node type '{node_type}'"
            )
        if node.depth > 0:  # we do not count the root directory
            self._stats[node_type] += 1
        return node

    def _generate_dataset_tree_nodes(self, dir_path: Path, is_last_child=True):
        pass

    def _generate_nodes(self, dir_path: Path, is_last_child=True):
        """
        Yields ``_TreeNode`` objects, each representing a directory or
        dataset or file. Nodes are traversed in depth-first order.
        """
        def is_excluded(path):
            exclusion_criteria = []
            if not self.include_files:
                exclusion_criteria.append(path.is_file())
            if not self.include_hidden:
                exclusion_criteria.append(path.name.startswith("."))
            return any(exclusion_criteria)

        yield self._new_node(DirectoryOrDatasetNode(
            dir_path, self._get_depth(dir_path), is_last_child
        ))

        if self._get_depth(dir_path) < self.max_depth:
            # apply exclusion filter
            selected_children = (
                p for p in Path(dir_path).iterdir()
                if not is_excluded(p)
            )
            # sort directory contents alphabetically
            children = sorted(list(selected_children))

            for child in children:

                # check if node is the last child within its subtree
                # (needed for displaying special end-of-subtree prefix)
                is_last_child = (child == children[-1])

                if child.is_file():
                    yield self._new_node(
                        FileNode(child, self._get_depth(child), is_last_child)
                    )
                elif child.is_dir():
                    # recurse into subdirectories
                    for node in self._generate_nodes(child, is_last_child):
                        yield node

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

        for node in self._generate_nodes(self.root):

            if self.skip_root and node.depth == 0:
                continue

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


class DatasetTree(Tree):
    def __init__(self, *args, max_dataset_depth=0, **kwargs):
        super().__init__(*args, **kwargs)
        # by default, do not recurse into datasets' subdirectories
        # (that are not datasets themselves)
        if self.max_depth is None:
            self.max_depth = 0

        self.max_dataset_depth = max_dataset_depth
        # meaning of 'dataset depth':
        #   -1 means no datasets encountered,
        #   0 means top-level superdatasets (relative to the tree root),
        #   1 means first-level subdatasets (relative to the tree root).
        self._current_dataset_depth = -1  # -1 means no datasets encountered

    def _is_max_dataset_depth_reached(self):
        if self.max_dataset_depth is not None:
            return self._current_dataset_depth > self.max_dataset_depth
        return False  # unlimited depth

    def _generate_nodes(self):
        # keep track of datasets' parent directories that have already
        # been yielded, so we do not yield them twice
        visited_parents = set([])
        for path, dirs, files in os.walk(self.root):

            # optimization: do not traverse the .git folder
            # (we're not going to find datasets in there)
            dirs[:] = [d for d in dirs if d != ".git"]

            # sort directories alphabetically in-place
            dirs.sort()

            # update the last child nodes
            if dirs:  # if there is a next level
                self._last_children.add(
                    os.path.join(path, dirs[-1])
                )

            if is_dataset(path):
                current_depth = self._get_depth(path)

                ds = DatasetNode(
                    path, current_depth, self._is_last_child(path),
                    use_full_paths=self.full_paths)
                self._current_dataset_depth = ds.ds_depth

                if self._is_max_dataset_depth_reached():
                    # we do not prune the walk (empty the list of
                    # directories to traverse next), but just skip to
                    # to the next node. this is because datasets
                    # of a given depth *could* be located at any
                    # directory depth, even below datasets at the same
                    # or deeper level.
                    continue

                # yield intermediate directories
                for parent, parent_depth in ds.parent_nodes():
                    if parent not in visited_parents:
                        visited_parents.add(parent)
                        yield DirectoryOrDatasetNode(
                            parent,
                            parent_depth,
                            self._is_last_child(parent),
                            use_full_paths=self.full_paths
                        )
                visited_parents.add(ds.path)
                yield ds

                if self.max_depth > 0:
                    # yield directories/files underneath dataset.
                    # here, the `max_depth` parameter refers to the
                    # directory depth of each dataset's subtree.
                    subtree = Tree(
                        ds.path, max_depth=self.max_depth,
                        full_paths=self.full_paths,
                        include_files=self.include_files,
                        include_hidden=self.include_hidden,
                        exclude_datasets=True
                    )
                    for subtree_node in subtree._generate_nodes():
                        # skip root node and dataset nodes
                        # (will be yielded in main loop)
                        if subtree_node.depth > 0 and \
                                not isinstance(subtree_node, DatasetNode):
                            # increment depth by offset of root
                            subtree_node.depth += ds.depth
                            if isinstance(subtree_node, DirectoryNode):
                                visited_parents.add(subtree_node.path)

                            yield subtree_node


class DirectoryNode(_TreeNode):
    COLOR = ansi_colors.BLUE

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        string = super().__str__()
        if self.depth > 0:
            return string + "/"
        return string

    def parent_nodes(self):
        """
        Generate parent nodes of the current node, excluding the tree root
        """
        if self.depth > 0:
            parent = self.path
            parent_paths = []
            for _ in range(self.depth):
                parent = os.path.dirname(parent)
                parent_paths.append(parent)
            parent_paths = parent_paths[::-1]  # sort from root to current node
            # print(f"path: {self.path}")
            # print(f"parents: {parent_paths}")
            for depth, path in enumerate(parent_paths):
                # print(depth_offset, p)
                yield path, depth
            #
            # for ix, parent_path in enumerate(parent_paths, start=1):
            #     yield DirectoryOrDatasetNode(
            #         path=os.path.join(root, parent_path),
            #         depth=self.depth - ix,
            #         is_last_child=True,  # temporary
            #         use_full_paths=self.use_full_paths
            #     )


class FileNode(_TreeNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DirectoryOrDatasetNode(_TreeNode):
    """
    Factory class for creating either a ``DirectoryNode`` or ``DatasetNode``,
    based on whether the current path is a dataset or not.
    """
    def __new__(cls, path, *args, **kwargs):
        if is_dataset(path):
            return DatasetNode(path, *args, **kwargs)
        else:
            return DirectoryNode(path, *args, **kwargs)


class DatasetNode(DirectoryNode):
    COLOR = ansi_colors.MAGENTA

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ds = require_dataset(self.path, check_installed=False)
        self.is_installed = self.ds.is_installed()
        self.ds_depth = None
        self.ds_absolute_depth = None
        self.calculate_dataset_depth()

    def __str__(self):
        install_flag = ", not installed" if not self.is_installed else ""
        suffix = f"  [DS~{self.ds_absolute_depth}{install_flag}]"
        return super().__str__() + suffix

    def calculate_dataset_depth(self):
        """
        Calculate 2 measures of a dataset's nesting depth/level:
        1. subdataset depth relative to the tree root
        2. absolute subdataset depth in the full hierarchy
        """
        # TODO: run this already in the constructor
        self.ds_depth = 0
        self.ds_absolute_depth = 0

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

                self.ds_absolute_depth += 1
                # TODO: Path.is_relative_to()
                # TODO: rglob() relative glob
                if is_path_child_of_parent(superds.path, self._get_tree_root()):
                    # if the parent dataset is underneath the tree
                    # root, we increment the relative depth
                    self.ds_depth += 1

            ds = superds
