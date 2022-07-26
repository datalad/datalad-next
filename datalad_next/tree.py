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
    This is basically just `tree` with visual markers for datasets.
    In addition to it, `datalad-tree` provides the following:
    1.  The subdataset hierarchy level information
        (included in the dataset marker, e.g. [DS~0]).
        This is the absolute level, meaning it may take into account
        superdatasets that are located above the tree root and thus are
        not included in the display.
    2.  The option to list only directories that are datasets.
    3.  The count of displayed datasets in the "report line"
        (where `tree` only reports count of directories and files).

(2) Descriptor of nested subdataset hierarchies:
    ---
    As a datalad user, I want to visualize the structure of multiple datasets
    and their hierarchies at once based on the subdataset nesting level,
    regardless of their depth in the directory tree. This helps me
    understand and communicate the layout of my datasets.
    ---
    This is the more datalad-specific case. Here we redefine 'depth' as the
    level in the subdataset hierarchy instead of the filesystem hierarchy.

"""

__docformat__ = 'restructuredtext'

import logging
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
            If not specified, will display all levels.
            If paired with [CMD: --dataset-depth CMD][PY: dataset_depth PY],
            refers to the maximum directory level to display underneath each
            dataset.""",
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
    )

    _examples_ = [
        dict(
            text="Display up to 3 levels of subdirectories and their contents "
                 "including files, starting from the current directory",
            code_py="tree(depth=3, include_files=True)",
            code_cmd="datalad tree -L 3 --include-files"),
        dict(text="List all first- and second-level subdatasets "
                  "of parent datasets located anywhere under /tmp, "
                  "regardless of directory depth, "
                  "including in hidden directories",
             code_py="tree('/tmp', dataset_depth=2, include_hidden=True)",
             code_cmd="datalad tree /tmp -R 2 --include-hidden"),
        dict(text="Display first- and second-level subdatasets and their "
                  "contents, up to 1 directory deep within each dataset",
             code_py="tree(dataset_depth=2, depth=1)",
             code_cmd="datalad tree -R 2 -L 1"),
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
    ):

        if dataset_depth is not None:
            tree = DatasetTree(
                Path(path),
                max_depth=depth,
                max_dataset_depth=dataset_depth,
                exclude_node_func=build_excluded_node_func(
                    include_hidden=include_hidden, include_files=include_files
                )
            )
        else:
            tree = Tree(
                Path(path),
                max_depth=depth,
                exclude_node_func=build_excluded_node_func(
                    include_hidden=include_hidden, include_files=include_files
                )
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


def build_excluded_node_func(include_hidden=False, include_files=False):
    """
    Returns a callable to filter tree nodes.
    The callable takes the Path object of a tree node as input,
    and returns true if the node should not be displayed in the tree.
    """

    def is_excluded(path):
        return any((
            not path.is_dir() if not include_files else False,
            path.name.startswith(".") if not include_hidden else False
        ))

    return is_excluded


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
    ds_path = Path(ds.path)

    # detect if it is an installed datalad-proper dataset
    # (as opposed to git/git-annex repo).
    # could also query `ds.id`, but checking just for existence
    # of config file is quicker.
    if Path(ds_path / ".datalad" / "config").is_file():
        return True

    # if it is not installed, check if it has an installed superdataset.
    # instead of querying ds.is_installed() (which checks if the
    # directory has the .git folder), we check if the directory
    # is empty (faster) -- as e.g. after a non-recursive `datalad clone`
    def is_empty_dir():
        return not any(ds_path.iterdir())

    if is_empty_dir():
        superds = ds.get_superdataset(datalad_only=True, topmost=False,
                                      registered_only=True)
        if superds is not None:
            return True

    # TODO: is there a way to detect a datalad dataset if it
    # is not installed and it is not a subdataset? For now, we don't
    return False


class Tree:
    """
    Main class for building and serializing a directory tree.
    Does not store ``_TreeNode`` objects, only the string representation
    of the whole tree and the statistics (counts of different node types).
    """

    def __init__(self,
                 root: Path,
                 max_depth=None,
                 skip_root=False,
                 exclude_node_func=None):

        self.root = root.resolve()
        if not self.root.is_dir():
            raise ValueError(f"directory '{root}' not found")

        self.max_depth = max_depth
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        # do not print first line with root path
        self.skip_root = skip_root

        # set custom or default filter criteria
        self.exclude_node_func = exclude_node_func or self.default_exclude_func

        # store list of lines of output string
        self._lines = []

        # dict with count of nodes for each _TreeNode subtype
        self._stats = {node_type.__name__: 0
                       for node_type in _TreeNode.__subclasses__()}

    @staticmethod
    def default_exclude_func(path: Path):
        """By default, only include non-hidden directories, no files"""
        return any((not path.is_dir(), path.name.startswith(".")))

    def path_depth(self, path: Path):
        """Get directory depth of a given path relative to root of the tree"""
        return len(path.relative_to(self.root).parts)

    def stats(self):
        """
        Returns a string with counts of different node types, similar
        to the tree command's 'report line' at the end of the tree
        output.
        The 3 node types (directory, dataset, file) are mutually exclusive,
        so their sum equals to the total node count.
        Does not count the root itself, only the contents below the root.
        """
        # sort node type names alphabetically
        node_types = sorted(
            _TreeNode.__subclasses__(),
            key=lambda c: c.__name__
        )
        return ", ".join(
            node_type.stats_description(self._stats[node_type.__name__])
            for node_type in node_types
        )

    def _generate_tree_nodes(self, dir_path: Path, is_last_child=True):
        """Recursively yield directory tree nodes starting from ``dir_path``"""

        if not self.skip_root or \
                self.skip_root and self.path_depth(dir_path) > 0:
            yield DirectoryOrDatasetNode(
                dir_path, self.path_depth(dir_path), is_last_child
            )

        # check that we are within max_depth levels
        # (None means unlimited depth)
        if self.max_depth is None or \
                self.path_depth(dir_path) < self.max_depth:

            # apply exclusion filter
            selected_children = (
                p for p in dir_path.iterdir()
                if not self.exclude_node_func(p)
            )
            # sort directory contents alphabetically
            children = sorted(list(selected_children))

            for child in children:

                # check if node is the last child within its subtree
                # (needed for displaying special end-of-subtree prefix)
                is_last_child = (child == children[-1])

                if child.is_dir():
                    # recurse into subdirectories
                    yield from self._generate_tree_nodes(child, is_last_child)
                else:
                    yield FileNode(child, self.path_depth(child), is_last_child)

    @increment_node_count
    def generate_nodes(self):
        """
        Traverse a directory tree starting from the root path.
        Yields ``_TreeNode`` objects, each representing a directory or
        dataset or file. Nodes are traversed in depth-first order.
        """
        # because the underlying generator is recursive, we cannot
        # directly decorate it with `increment_node_count` (since
        # it would count twice whenever the function recurses).
        # so we decorate a separate function where we just yield
        # from the underlying decorator.
        for node in self._generate_tree_nodes(self.root):
            yield node

    def build(self):
        """
        Construct the tree string representation and return back the instance.
        """
        self.to_string()
        return self

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

        for node in self.generate_nodes():

            if node.is_last_child:  # last child of its subtree
                levels_with_exhausted_subtree.add(node.depth)
            else:
                # 'discard' does not raise exception
                # if value does not exist in set
                levels_with_exhausted_subtree.discard(node.depth)

            # build indentation string
            indentation = ""
            spacing = node.INDENTATION_SPACING
            if node.depth > 0:
                indentation_symbols_for_levels = [
                    (node.INDENTATION_SYMBOL
                        if level not in levels_with_exhausted_subtree
                        else " ") + spacing
                    for level in range(1, node.depth)
                ]
                indentation = "".join(indentation_symbols_for_levels)

            line = indentation + str(node)
            yield line


class DatasetTree(Tree):
    """
    ``DatasetTree`` is a ``Tree`` where hierarchy depth refers to the
    subdataset hierarchy level, instead of directory depth.
    Because of the different semantics of the ``max_depth`` parameter,
    we implement a separate subclass of ``Tree``.
    """
    def __init__(self, *args, max_dataset_depth=0, **kwargs):
        super().__init__(*args, **kwargs)
        # by default, do not recurse into datasets' subdirectories
        # (other than paths to nested subdatasets)
        if self.max_depth is None:
            self.max_depth = 0

        self.max_dataset_depth = max_dataset_depth

    @increment_node_count
    def generate_nodes(self):
        """
        A ``DatasetTree`` is just an unlimited-depth ``Tree`` with more
        complex rules for excluding nodes. Each exclusion rule is encoded
        in a function. The rules are then combined in a final
        ``exclusion_func`` which is supplied to the ``Tree`` constructor.
        """

        def is_git_folder(path: Path):
            return path.is_dir() and path.name == ".git"

        def ds_exceeds_max_ds_depth(path: Path):
            if path.is_dir() and is_dataset(path):
                ds = DatasetNode(path, self.path_depth(path), False)
                return ds.ds_depth > self.max_dataset_depth
            return False

        def ds_child_node_exceeds_max_depth(path: Path):
            if not (path.is_dir() and is_dataset(path)):
                ds_parents = [
                    p for p in path.parents
                    if p.is_relative_to(self.root) and
                    is_dataset(p)
                ]
                if ds_parents:
                    parent = ds_parents[-1]  # closest parent
                    relative_depth = self.path_depth(path) - self.path_depth(parent)
                    return relative_depth > self.max_depth
            return True

        def is_parent_of_included_ds(path: Path):
            def exclude(p: Path):
                return not p.is_dir() or is_git_folder(p)

            if path.is_dir() and not is_dataset(path):
                # search in the subtree with the current path as root
                subtree = Tree(
                    path,
                    max_depth=None,
                    exclude_node_func=exclude,
                    skip_root=True
                )

                def child_datasets():
                    """Generator of dataset nodes"""
                    for node in subtree.generate_nodes():
                        if isinstance(node, DatasetNode):
                            # offset depth by depth of current path
                            node.depth += self.path_depth(path)
                            # need to recalculate dataset depth after
                            # updating directory depth
                            node.ds_depth, _ = node.calculate_dataset_depth()
                            yield node

                return any(
                    ds.ds_depth <= self.max_dataset_depth
                    for ds in child_datasets()
                )

            return False

        def exclude_func(path: Path):
            """Combine exclusion criteria from different functions"""
            criteria = self.exclude_node_func(path)

            if path.is_dir() and is_dataset(path):
                # check if maximum dataset depth is exceeded
                criteria |= ds_exceeds_max_ds_depth(path)
            else:
                # do not traverse the .git folder (we will not find
                # datasets underneath it)
                criteria |= is_git_folder(path)

                # exclude files or directories underneath a dataset,
                # if they have depth (relative to dataset root) > max_depth,
                # unless they are themselves parents of a dataset with
                # dataset depth within the valid ds_depth range
                if not (path.is_dir() and is_parent_of_included_ds(path)):
                    criteria |= ds_child_node_exceeds_max_depth(path)

            return criteria

        ds_tree = Tree(
            self.root,
            max_depth=None,  # unlimited traversal (datasets could be anywhere)
            exclude_node_func=exclude_func,
            skip_root=self.skip_root,
        )

        yield from ds_tree.generate_nodes()


class _TreeNode:
    """
    Base class for a directory or file represented as a single
    tree node and printed as single line of the 'tree' output.
    """
    COLOR = None  # ANSI color for the path, if terminal color are enabled
    # symbols for the tip of the 'tree branch', depending on
    # whether a node is the last in it subtree or not
    PREFIX_MIDDLE_CHILD = "├── "
    PREFIX_LAST_CHILD = "└── "
    # symbol for representing the continuation of a 'tree branch'
    INDENTATION_SYMBOL = "│"
    # space between the indentation symbol of one level and the next
    INDENTATION_SPACING = "   "

    def __init__(self, path: Path, depth: int, is_last_child: bool):
        self.path = path
        self.depth = depth  # depth in the Tree
        self.is_last_child = is_last_child  # if is last sibling in its subtree

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(str(self.path))

    def __str__(self):
        if self.depth == 0:
            path = self.path
        else:
            path = self.path.name

        if self.COLOR is not None:
            path = ansi_colors.color_word(path, self.COLOR)

        prefix = ""
        if self.depth > 0:
            prefix = self.PREFIX_LAST_CHILD if self.is_last_child \
                else self.PREFIX_MIDDLE_CHILD

        return prefix + str(path)

    @staticmethod
    def stats_description(count):
        """String describing the node count that will be
        included in the tree's report line"""
        raise NotImplementedError
        # return str(count) + (" node" if int(count) == 1 else " nodes")

    @property
    def tree_root(self):
        """Calculate tree root path from node path and depth"""
        parents = self.parents
        return parents[0] if parents \
            else self.path  # we are the root

    @property
    def parents(self):
        """
        List of parent paths (beginning from the tree root)
        in top-down order.
        """
        parents_from_tree_root = []
        for depth, path in enumerate(self.path.parents):
            if depth >= self.depth:
                break
            parents_from_tree_root.append(path)

        return parents_from_tree_root[::-1]  # top-down order


class DirectoryNode(_TreeNode):
    COLOR = ansi_colors.BLUE

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        string = super().__str__()
        if self.depth > 0:
            return string + "/"
        return string

    @staticmethod
    def stats_description(count):
        return str(count) + (" directory" if int(count) == 1 else " directories")


class FileNode(_TreeNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def stats_description(count):
        return str(count) + (" file" if int(count) == 1 else " files")


class DatasetNode(_TreeNode):
    COLOR = ansi_colors.MAGENTA

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ds = require_dataset(self.path, check_installed=False)
        self.is_installed = self.ds.is_installed()
        self.ds_depth, self.ds_absolute_depth = self.calculate_dataset_depth()

    def __str__(self):
        install_flag = ", not installed" if not self.is_installed else ""
        dir_suffix = "/" if self.depth > 0 else ""
        ds_suffix = f"  [DS~{self.ds_absolute_depth}{install_flag}]"
        return super().__str__() + dir_suffix + ds_suffix

    @staticmethod
    def stats_description(count):
        return str(count) + (" dataset" if int(count) == 1 else " datasets")

    def calculate_dataset_depth(self):
        """
        Calculate 2 measures of a dataset's nesting depth/level:
        1. subdataset depth relative to the tree root
        2. absolute subdataset depth in the full hierarchy
        """
        ds_depth = 0
        ds_absolute_depth = 0

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

                ds_absolute_depth += 1
                if Path(superds.path).is_relative_to(self.tree_root):
                    # if the parent dataset is underneath the tree
                    # root, we increment the relative depth
                    ds_depth += 1

            ds = superds

        return ds_depth, ds_absolute_depth


class DirectoryOrDatasetNode:
    """
    Factory class for creating either a ``DirectoryNode`` or ``DatasetNode``,
    based on whether the current path is a dataset or not.
    """
    def __new__(cls, path, *args, **kwargs):
        if is_dataset(path):
            return DatasetNode(path, *args, **kwargs)
        else:
            return DirectoryNode(path, *args, **kwargs)
