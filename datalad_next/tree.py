# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad_osf package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""'tree'-like command for visualizing dataset hierarchies"""

__docformat__ = "numpy"

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

    This command mimics the UNIX/MSDOS ``tree`` utility to display a directory
    tree, highlighting DataLad datasets in the hierarchy.

    Two main use cases are covered:

    1. Glorified ``tree`` command:

      Display the contents of a directory tree and see which directories are
      datalad datasets (including subdatasets that are present but not
      installed, such as after a non-recursive clone).

      This is basically just ``tree`` with visual markers for datasets. In
      addition to it, ``datalad-tree`` provides the following:

        - The subdataset hierarchy level is shown in the dataset marker
          (e.g. [DS~2]). This is the absolute level, meaning it may also take
          into account superdatasets located above the tree root and thus
          not included in the output.
        - The 'report line' at the bottom of the output shows the count of
          displayed datasets, in addition to the count of directories and
          files.

    2. Descriptor of nested subdataset hierarchies:

      Display the structure of multiple datasets and their hierarchies based
      on subdataset nesting level, regardless of their location in the
      directory tree.

      In this case, the tree depth is determined by subdataset depth.
      There is also the option to display contents (directories/files) of
      each dataset up to max_depth levels, to provide better context around
      the datasets.
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
            args=("--depth",),
            doc="""maximum level of directory tree to display.
            If not specified, will display all levels.
            If paired with [CMD: --dataset-depth CMD][PY: dataset_depth PY],
            refers to the maximum directory level to display underneath each
            dataset.""",
            constraints=EnsureInt() & EnsureRange(min=0) | EnsureNone()),
        dataset_depth=Parameter(
            args=("--dataset-depth",),
            doc="""maximum level of nested subdatasets to display""",
            constraints=EnsureInt() & EnsureRange(min=0) | EnsureNone()),
        include_files=Parameter(
            args=("--include-files",),
            doc="""include files in output display""",
            action='store_true'),
        include_hidden=Parameter(
            args=("--include-hidden",),
            doc="""include hidden files/directories in output display""",
            action='store_true'),
    )

    _examples_ = [
        dict(text="Display up to 3 levels of the current directory's "
                  "subdirectories and their contents",
             code_py="tree(depth=3, include_files=True)",
             code_cmd="datalad tree --depth 3 --include-files"),
        dict(text="Display all first- and second-level subdatasets of "
                  "datasets located anywhere under /tmp (including in hidden "
                  "directories) regardless of directory depth",
             code_py="tree('/tmp', dataset_depth=2, include_hidden=True)",
             code_cmd="datalad tree /tmp --dataset-depth 2 --include-hidden"),
        dict(text="Display first- and second-level subdatasets and their "
                  "contents, up to 1 directory deep within each dataset",
             code_py="tree(dataset_depth=2, depth=1)",
             code_cmd="datalad tree --dataset-depth 2 --depth 1"),
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
            include_hidden=False):

        if dataset_depth is not None:
            # special tree defined by subdataset nesting depth
            tree_cls = DatasetTree
            dataset_tree_args = {"max_dataset_depth": dataset_depth}
        else:
            # simple tree defined by directory depth
            tree_cls = Tree
            dataset_tree_args = {}

        tree = tree_cls(
            Path(path),
            max_depth=depth,
            exclude_node_func=build_excluded_node_func(
                include_hidden=include_hidden, include_files=include_files),
            **dataset_tree_args
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
    """Return a function to exclude ``_TreeNode`` objects from the tree (
    prevents them from being yielded by the node generator).

    Returns
    -------
    Callable
        Function that takes the Path object of a ``_TreeNode`` as input,
        and returns true if the node should *not* be displayed in the tree.
    """

    def is_excluded(path):
        return any((
            not path.is_dir() if not include_files else False,
            path.name.startswith(".") if not include_hidden else False
        ))

    return is_excluded


def increment_node_count(node_generator_func):
    """Decorator for incrementing the node count whenever a ``_TreeNode`` is
    yielded.

    Parameters
    ----------
    node_generator_func: Callable
        Function that yields ``_TreeNode`` objects
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


@lru_cache
def is_dataset(path: Path):
    """Fast dataset detection.
    Infer that a directory is a dataset if it is either:

    - installed, or
    - not installed, but has an installed superdatset.

    Only consider datalad datasets, not plain git/git-annex repos.

    Results are cached because the check is somewhat expensive and may be run
    multiple times on the same path.

    TODO: is there a way to detect a datalad dataset if it is not installed
    and it is not a subdataset?

    Parameters
    ----------
    path: Path
        Path to directory to be identified as dataset or non-dataset
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
        if get_superdataset(ds) is not None:
            return True

    return False


@lru_cache
def get_superdataset(ds):
    """Wrapper for ``Dataset.get_superdataset()`` with predefined options.

    Results are cached, as this function may be rerun on the same dataset
    multiple times.

    Parameters
    ----------
    ds: Dataset

    Returns
    -------
    Dataset or None
    """
    return ds.get_superdataset(
        datalad_only=True, topmost=False, registered_only=True)


def is_path_relative_to(my_path: Path, other_path: Path):
    """Copy of pathlib's ``Path.is_relative_to()`` that requires python3.9+"""
    try:
        my_path.relative_to(other_path)
        return True
    except ValueError:
        return False


class Tree:
    """Main class for generating and serializing a directory tree"""

    def __init__(self,
                 root: Path,
                 max_depth=None,
                 skip_root=False,
                 exclude_node_func=None):
        """
        Parameters
        ----------
        root: Path
            Directory to be used as tree root
        max_depth: int or None
            Maximum directory depth for traversing the tree
        skip_root: bool
            If true, will not print the first line with tree root
        exclude_node_func: Callable or None
            Function to filter out tree nodes from the tree
        """
        self.root = root.resolve()
        if not self.root.is_dir():
            raise ValueError(f"directory '{root}' not found")

        self.max_depth = max_depth
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        self.skip_root = skip_root

        # set custom or default filter criteria
        self.exclude_node_func = exclude_node_func or self.default_exclude_func

        # store list of lines of output string
        self._lines = []

        # store dict with count of nodes for each _TreeNode subtype
        self._stats = {node_type.__name__: 0
                       for node_type in _TreeNode.__subclasses__()}

    @staticmethod
    def default_exclude_func(path: Path):
        """By default, only include non-hidden directories, no files"""
        return any((not path.is_dir(), path.name.startswith(".")))

    def path_depth(self, path: Path) -> int:
        """Calculate directory depth of a given path relative to the root of
        the tree"""
        return len(path.relative_to(self.root).parts)

    def stats(self) -> str:
        """
        Produces a string with counts of different node types, similar
        to the tree command's 'report line' at the end of the tree
        output.

        The node types (subclasses of ``_TreeNode``) are mutually exclusive,
        so the sum of their counts equals to the total node count.

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
        """Recursively yield ``_TreeNode`` objects starting from
        ``dir_path``

        Parameters
        ----------
        dir_path: Path
            Directory from which to calculate the tree
        is_last_child: bool
            Whether the directory ``dir_path`` is the last child of its
            parent in the ordered list of child nodes
        """

        if not self.skip_root or \
                self.skip_root and self.path_depth(dir_path) > 0:
            yield DirectoryOrDatasetNode(
                dir_path, self.path_depth(dir_path), is_last_child
            )

        # check that we are within max_depth levels (None means unlimited
        # depth)
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

                # check if node is the last child within its subtree (needed
                # for displaying special end-of-subtree prefix)
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

        Returns
        -------
        Generator[_TreeNode]
        """
        # because the node generator is recursive, we cannot directly
        # decorate it with `increment_node_count` (since it would count
        # twice whenever the function recurses).
        # so we decorate a separate function where we just yield from the
        # underlying generator.
        yield from self._generate_tree_nodes(self.root)

    def build(self):
        """Construct the tree string representation (will be stored in
        instance attribute) and return the instance."""
        self.to_string()
        return self

    def to_string(self) -> str:
        """Return complete tree as string"""
        if not self._lines:
            return "\n".join(list(self.print_line()))
        return "\n".join(self._lines)

    def print_line(self):
        """Generator for tree string output lines

        Returns
        -------
        Generator[str]
        """
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
        """Generator of lines of the tree string representation.

        Each line represents a tree node (directory or dataset or file).

        A line follows the structure:
            ``[<indentation>] [<branch_tip_symbol>] <path>``

        Example line:
            ``│   │   ├── path_dir_level3``

        Returns
        -------
        Generator[str]
        """

        # keep track of levels where subtree is exhaused, i.e. we have
        # reached the last child of the subtree.
        # this is needed to build the indentation string for each node,
        # which takes into account whether any parent is the last node of
        # its own subtree.
        levels_with_exhausted_subtree = set([])

        for node in self.generate_nodes():

            if node.is_last_child:  # last child of its subtree
                levels_with_exhausted_subtree.add(node.depth)
            else:
                # 'discard' does not raise exception if value does not exist
                # in set
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
    ``DatasetTree`` is a ``Tree`` whose depth is determined by the
    subdataset hierarchy level, instead of directory depth.

    Because of the different semantics of the ``max_depth`` parameter,
    we implement a separate subclass of ``Tree``.
    """
    def __init__(self, *args, max_dataset_depth=0, **kwargs):
        super().__init__(*args, **kwargs)
        # by default, do not recurse into datasets' subdirectories (other
        # than paths to nested subdatasets)
        if self.max_depth is None:
            self.max_depth = 0

        self.max_dataset_depth = max_dataset_depth

    @increment_node_count
    def generate_nodes(self):
        """
        Yield ``_TreeNode`` objects that belong to the tree.

        A ``DatasetTree`` is just an unlimited-depth ``Tree`` with more
        complex rules for pruning (skipping traversal of particular nodes).
        Each exclusion rule is encoded in a function. The rules are then
        combined in a final ``exclusion_func`` which is supplied to the
        ``Tree`` constructor.

        Returns
        -------
        Generator[_TreeNode]
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
                    if is_path_relative_to(p, self.root) and
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
                    """Generator of dataset nodes below the current node"""
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
    """Base class for a directory or file represented as a single tree node
    and printed as single line of the 'tree' output.
    """
    COLOR = None  # ANSI color for the path, if terminal color are enabled

    # symbols for the tip of the 'tree branch', depending on
    # whether a node is the last in it subtree or not
    PREFIX_MIDDLE_CHILD = "├──"
    PREFIX_LAST_CHILD = "└──"

    # symbol for representing the continuation of a 'tree branch'
    INDENTATION_SYMBOL = "│"
    # spacing between the indentation symbol of one level and the next
    INDENTATION_SPACING = "   "

    def __init__(self, path: Path, depth: int, is_last_child: bool):
        """
        Parameters
        ----------
        path: Path
            Path of the tree node
        depth: int
            Directory depth of the node within its tree
        is_last_child: bool
            Whether the node is the last node among its parent's children
        """
        self.path = path
        self.depth = depth
        self.is_last_child = is_last_child

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(str(self.path))

    def __str__(self):
        # display root directory with full path, all other nodes with basename
        if self.depth == 0:
            path = self.path
        else:
            path = self.path.name

        if self.COLOR is not None:
            path = ansi_colors.color_word(path, self.COLOR)

        if self.depth > 0:
            prefix = self.PREFIX_LAST_CHILD if self.is_last_child \
                else self.PREFIX_MIDDLE_CHILD
            return " ".join([prefix, path])
        return str(path)  # root directory has no prefix

    @staticmethod
    def stats_description(count):
        """String describing the node count that will be included in the
        tree's report line"""
        # should be implemented by subclasses
        raise NotImplementedError

    @property
    def tree_root(self) -> Path:
        """Calculate tree root path from node path and depth"""
        parents = self.parents
        return parents[0] if parents \
            else self.path  # we are the root

    @property
    def parents(self):
        """List of parent paths (beginning from the tree root) in top-down
        order.

        Returns
        -------
        List[Path]
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
        default_str = super().__str__()

        ds_marker_depth = ansi_colors.color_word(
            f"DS~{self.ds_absolute_depth}", ansi_colors.WHITE)
        install_flag = " (not installed)" if not self.is_installed else ""
        ds_marker = f"[{ds_marker_depth}]{install_flag}"

        if self.depth > 0:
            prefix = self.PREFIX_LAST_CHILD if self.is_last_child else \
                self.PREFIX_MIDDLE_CHILD
            custom_str = default_str.replace(prefix, f"{prefix} {ds_marker}")
        else:
            custom_str = f"{ds_marker} {default_str}"

        return custom_str + ("/" if self.depth > 0 else "")

    @staticmethod
    def stats_description(count):
        return str(count) + (" dataset" if int(count) == 1 else " datasets")

    def calculate_dataset_depth(self):
        """
        Calculate 2 measures of a dataset's nesting depth/level:

        1. ``ds_depth``: subdataset depth relative to the tree root
        2. ``ds_absolute_depth``: absolute subdataset depth in the full
           hierarchy, potentially taking into account parent datasets at
           levels above the tree root

        Returns
        -------
        Tuple[int, int]
            Tuple of relative dataset depth and absolute dataset depth
        """
        ds_depth = 0
        ds_absolute_depth = 0

        ds = self.ds

        while ds:
            superds = get_superdataset(ds)

            if superds is None:
                # it is not a dataset, do nothing
                break
            else:
                if superds == ds:
                    # it is a top-level dataset, we are done
                    break

                ds_absolute_depth += 1
                if is_path_relative_to(Path(superds.path), self.tree_root):
                    # if the parent dataset is underneath the tree
                    # root, we increment the relative depth
                    ds_depth += 1

            ds = superds

        return ds_depth, ds_absolute_depth


class DirectoryOrDatasetNode:
    """Factory class for creating either a ``DirectoryNode`` or
    ``DatasetNode``, based on whether the path is a dataset or not.
    """
    def __new__(cls, path, *args, **kwargs):
        if is_dataset(path):
            return DatasetNode(path, *args, **kwargs)
        else:
            return DirectoryNode(path, *args, **kwargs)
