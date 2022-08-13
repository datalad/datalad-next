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
from os import readlink
from pathlib import Path

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.support.exceptions import (
    CapturedException,
    NoDatasetFound
)
from datalad.support.param import Parameter
from datalad.distribution.dataset import (
    datasetmethod,
    require_dataset,
    Dataset,
)
from datalad.interface.results import (
    get_status_dict,
)
from datalad.interface.utils import eval_results

from datalad.local.subdatasets import Subdatasets
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
    EnsureInt,
    EnsureRange,
)
from datalad.utils import get_dataset_root
from datalad.ui import ui

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

        for node in tree.generate_nodes():
            # yield one node at a time to improve UX / perceived speed
            res_dict = {
                "action": "tree",
                "path": str(node.path),
                "type": node.TYPE,
                "depth": node.depth,
                "exhausted_levels": list(tree.exhausted_levels),
                "count": {
                    "datasets": tree.node_count["DatasetNode"],
                    "directories": tree.node_count["DirectoryNode"],
                    **({"files": tree.node_count["FileNode"]}
                       if include_files else {})
                },
            }
            if node.TYPE == "dataset":
                res_dict.update({
                    "dataset_depth": node.ds_depth,
                    "dataset_abs_depth": node.ds_absolute_depth,
                    "dataset_is_installed": node.is_installed
                })

            if node.is_symlink():
                # TODO: should we inform if the symlink is recursive (as per
                #  `tree.is_recursive_symlink()`) although not broken? The
                #  UNIX 'tree' command shows the message '[recursive,
                #  not followed]' next to the path. Not sure if this is
                #  interesting at all or more confusing.
                res_dict["symlink_target"] = node.get_symlink_target()
                res_dict["is_broken_symlink"] = node.is_broken_symlink()

            if node.exception is not None:
                # mimic error message of unix 'tree' command for
                # permission denied error, otherwise use exception short
                # message
                message = "error opening dir" \
                    if node.exception.name == "PermissionError" \
                    else node.exception.message

                yield get_status_dict(
                    status="error",
                    message=message,
                    exception=node.exception,
                    **res_dict
                )
            else:
                yield get_status_dict(
                    status="ok",
                    **res_dict
                )

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        """
        Each node is printed on one line. The string uses the format:
        ``[<indentation>] [<branch_tip_symbol>] <path> [<ds_marker]``

        Example line:
        ``│   │   ├── path_dir_level3``
        """
        from datalad.support import ansi_colors

        # get values from result record
        node_type = res["type"]
        node_path = res["path"]
        depth = res["depth"]
        exhausted_levels = res["exhausted_levels"]

        # build indentation string
        indentation = ""
        if depth > 0:
            indentation_symbols_for_levels = [
                ("│"
                 if level not in exhausted_levels
                 else " ") + "   "
                for level in range(1, depth)
            ]
            indentation = "".join(indentation_symbols_for_levels)

        # build prefix (tree branch tip)
        prefix = ""
        if depth > 0:  # root node has no prefix
            is_last_child = depth in exhausted_levels
            prefix = "└──" if is_last_child else "├──"

        # build dataset marker if dataset
        ds_marker = ""
        if node_type == "dataset":
            ds_absolute_depth = res["dataset_abs_depth"]
            ds_is_installed = res["dataset_is_installed"]

            ds_marker_depth = ansi_colors.color_word(
                f"DS~{ds_absolute_depth}",
                ansi_colors.WHITE)
            install_flag = " (not installed)" if not ds_is_installed else ""
            ds_marker = f"[{ds_marker_depth}]" + install_flag

        # build path string with optional color
        # display only root directory with full path, all other nodes
        # with basename
        path = node_path if depth == 0 else Path(node_path).name
        color_for_type = {
            "dataset": ansi_colors.MAGENTA,
            "directory": ansi_colors.BLUE,
            "file": None,
            "broken_symlink": ansi_colors.RED
        }
        # ANSI color for the path, if terminal colors are enabled
        color = color_for_type[node_type]
        if color is not None:
            path = ansi_colors.color_word(path, color)
        if res.get("is_broken_symlink", False):
            path = ansi_colors.color_word(path,
                                          color_for_type["broken_symlink"])

        # set suffix for directories
        dir_suffix = ""
        if depth > 0 and node_type in ("directory", "dataset"):
            dir_suffix = "/"

        # append symlink target if symlink
        symlink_target = ""
        if "symlink_target" in res:
            symlink_target = " -> " + res["symlink_target"]

        # add short error message if there was exception
        error_msg = ""
        if "exception" in res:
            error_msg = f" [{res['message']}]"

        line = indentation + \
            " ".join((s for s in (prefix, ds_marker, path) if s != "")) + \
            dir_suffix + symlink_target + error_msg
        ui.message(line)

    @staticmethod
    def custom_result_summary_renderer(res, **kwargs):
        """Print the summary 'report line' with count of nodes by type"""

        c_ds = res[-1]['count']['datasets']
        c_dirs = res[-1]['count']['directories']
        # files may not be included in results (if not using command
        # option '--include-files')
        c_files = res[-1]['count'].get('files')

        descriptions = [
            f"{c_ds} " + ("dataset" if int(c_ds) == 1 else "datasets"),
            f"{c_dirs} " + ("directory" if int(c_dirs) == 1 else "directories")
        ]
        if c_files is not None:
            descriptions.append(
                f"{c_files} " + ("file" if int(c_files) == 1 else "files"))

        ui.message("\n" + ", ".join(descriptions))


def build_excluded_node_func(include_hidden=False, include_files=False):
    """Return a function to exclude ``_TreeNode`` objects from the tree
    (prevents them from being yielded by the node generator).

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
            if node_type not in self.node_count:
                raise ValueError(
                    f"No counts collected for unknown node type '{node_type}'"
                )
            if node.depth > 0:  # do not count the root directory
                # TODO: do not count symlinks if they point to
                #  files/directories that are already included in the tree
                #  (to prevent double counting)? Note that UNIX 'tree' does
                #  count double.
                self.node_count[node_type] += 1

            yield node  # yield what the generator yielded

    return _wrapper


def yield_with_last_item(generator):
    """Takes a generator and yields for each item, the item itself and
    whether it is the last item in the sequence.

    Returns
    -------
    Tuple[bool, Any]
        A tuple (is_last_item, item)
    """
    prev_val = next(generator, None)
    if prev_val is not None:
        for current_val in generator:
            yield False, prev_val
            prev_val = current_val
        yield True, prev_val


def is_empty_dir(path: Path):
    return path.is_dir() and not any(path.iterdir())


@lru_cache
def is_dataset(path: Path):
    """Fast dataset detection.

    Infer that a directory is a dataset if it is either:

    - installed, or
    - not installed, but has an installed superdatset.

    Only consider datalad datasets, not plain git/git-annex repos. Datasets
    used for aggregating metadatata from subdatasets are also counted as
    datasets, although they do not have a dataset ID themselves.

    Symlinks pointing to datasets are not resolved, so will always return
    False for symlinks. This prevents potentially detecting duplicate datasets
    if the symlink and its target are both included in the tree.

    Results are cached because the check is somewhat expensive and may
    be run multiple times on the same path.

    TODO: is there a way to detect a datalad dataset if it is not installed
     and it is not a subdataset?

    Parameters
    ----------
    path: Path
        Path to directory to be identified as dataset or non-dataset
    """
    try:
        if path.is_symlink():
            # ignore symlinks even if pointing to datasets, otherwise we may
            # get duplicate counts of datasets
            lgr.debug("Path is a symlink, will not check if it points to a "
                      f"dataset: '{path}'")
            return False

        if (path / ".datalad" / "config").is_file() or \
                (path / ".datalad" / "metadata").is_dir():
            # could also query `ds.id`, but checking just for existence
            # of config file is quicker.
            return True

        # if it is not installed, check if it has an installed superdataset.
        # instead of querying ds.is_installed() (which checks if the
        # directory has the .git folder), we check if the directory
        # is empty (faster) -- as e.g. after a non-recursive `datalad clone`
        if is_empty_dir(path):
            if get_superdataset(path) is not None:
                return True

    except Exception as ex:
        # if anything fails (e.g. permission denied), we raise exception
        # instead of returning False. this can be caught and handled by the
        # caller.
        raise NoDatasetFound(f"Cannot determine if '{path.name}' is a "
                             f"dataset") from ex

    return False


@lru_cache
def get_subds_paths(ds_path: Path):
    """Return paths of immediate subdatasets for a given dataset path."""
    # This is an expensive operation because it calls git to read the
    # submodules. Since we need to run it to (A) calculate dataset depth and
    # (B) detect non-installed datasets, we cache results, so that the list of
    # subdatasets is computed only once for each parent dataset.

    def res_filter(res):
        return res.get('status') == 'ok' and res.get('type') == 'dataset'

    # call subdatasets command instead of dataset method `ds.subdatasets()`
    # to avoid potentially expensive import of full datalad API
    return Subdatasets.__call__(
        dataset=ds_path,
        recursive=False,
        state='any',  # include not-installed subdatasets
        result_filter=res_filter,
        on_failure='ignore',
        result_xfm='paths',
        result_renderer='disabled',
        return_type='list'
    )


def is_subds_of_parent(subds_path: Path, parent_path: Path):
    return any(
        is_path_relative_to(Path(p), subds_path)
        for p in get_subds_paths(parent_path)
    )


def get_dataset_root_datalad_only(path: Path):
    """Get root of dataset containing a given path (datalad datasets only,
    not pure git/git-annex repo)

    Parameters
    ----------
    path: Path
        Path to file or directory

    Returns
    -------
    Path
    """
    ds_root = path
    while ds_root:
        potential_ds_root = get_dataset_root(str(ds_root))

        if potential_ds_root is None:
            return None  # we are not inside a dataset

        potential_ds_root = Path(potential_ds_root)
        if is_dataset(potential_ds_root):
            return potential_ds_root  # it's a match

        # we go one directory higher and try again
        ds_root = Path.resolve(potential_ds_root / '..')
    return ds_root


@lru_cache
def get_superdataset(path: Path):
    """Reimplementation of ``Dataset.get_superdataset()`` to allow caching
    results of `ds.subdatasets()` (the most expensive operation).

    Parameters
    ----------
    path: Path
        Path to a dataset

    Returns
    -------
    Dataset or None
    """
    path = str(path)
    superds_path = None

    while path:
        # normalize the path after adding .. so we guaranteed to not
        # follow into original directory if path itself is a symlink
        parent_path = Path.resolve(Path(path) / '..')
        sds_path_ = get_dataset_root_datalad_only(parent_path)
        if sds_path_ is None:
            # no more parents, use previous found
            break

        superds = Dataset(sds_path_)

        # test if path is registered subdataset of the parent
        if not is_subds_of_parent(Path(path), superds.pathobj):
            break

        # That was a good candidate
        superds_path = sds_path_
        path = str(parent_path)
        break

    if superds_path is None:
        # None was found
        return None
    return Dataset(superds_path)


def is_path_relative_to(my_path: Path, other_path: Path):
    """Port of pathlib's ``Path.is_relative_to()`` (requires python3.9+)"""
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
                 exclude_node_func=None):
        """
        Parameters
        ----------
        root: Path
            Directory to be used as tree root
        max_depth: int or None
            Maximum directory depth for traversing the tree
        exclude_node_func: Callable or None
            Function to filter out tree nodes from the tree
        """
        self.root = root.resolve()
        if not self.root.is_dir():
            raise ValueError(f"Directory not found: '{root}'")

        self.max_depth = max_depth
        if max_depth is not None and max_depth < 0:
            raise ValueError("max_depth must be >= 0")

        # set callable to exclude nodes from the tree, meaning they
        # will not be yielded by the node generator
        self.exclude_node_func = exclude_node_func or self.default_exclude_func

        # keep track of levels where the subtree is exhausted, i.e. we
        # have reached the last node of the current subtree.
        # this is needed for the custom results renderer, to display nodes
        # differently depending on whether they are the last child or not.
        self.exhausted_levels = set([])

        # store dict with count of nodes for each node type, similar to the
        # tree command's 'report line' at the end of the output.
        # the node types (subclasses of ``_TreeNode``) are mutually exclusive,
        # so the sum of their counts equals to the total node count.
        # does not count the root itself, only the contents below the root.
        self.node_count = {node_type.__name__: 0
                           for node_type in _TreeNode.__subclasses__()}

    @staticmethod
    def default_exclude_func(path: Path):
        """By default, exclude files and hidden directories from the tree"""
        return any((not path.is_dir(), path.name.startswith(".")))

    def path_depth(self, path: Path):
        """Calculate directory depth of a given path relative to the root of
        the tree.

        Can also be a negative integer if the path is a parent of the
        tree root.

        Parameters
        ----------
        path: Path

        Returns
        -------
        int
            Number of levels of the given path *below* the tree root (positive
            integer) or *above* the tree root (negative integer)
        """
        if is_path_relative_to(path, self.root):
            return len(path.relative_to(self.root).parts)
        elif is_path_relative_to(self.root, path):
            return - len(self.root.relative_to(path).parts)
        else:
            raise ValueError("Could not calculate directory depth: "
                             f"'{path}' is not relative to the tree root "
                             f"'{self.root}' (or vice-versa)")

    def is_recursive_symlink(self, dir_path: Path):
        """Detect symlink pointing to a directory within the same tree
        (directly or indirectly).

        The default behaviour is to follow symlinks. However, we do not follow
        symlinks to directories that we may visit or have visited already,
        i.e. are also located under the tree root or any parent of
        the tree root (within a distance of ``max_depth``).

        Otherwise, the same subtree could be generated multiple times in
        different places, potentially in a recursive loop (e.g. if the
        symlink points to its parent).

        This is similar to the logic of the UNIX 'tree' command, but goes a
        step further to prune all duplicate subtrees.
        """
        if not dir_path.is_symlink():
            return False

        if not dir_path.is_dir():
            # we are only interested in symlinks pointing to a directory
            raise ValueError("Path must be a directory")

        target_dir = dir_path.resolve()

        if is_path_relative_to(target_dir, self.root) or \
                is_path_relative_to(self.root, target_dir):
            # either:
            # - target dir is within `max_depth` levels beneath the tree
            #   root, so it will likely be yielded or has already been
            #   yielded (bar any exclusion filters)
            # - target dir is a parent of the tree root, so we may still
            #   get into a loop if we recurse more than `max_depth` levels
            return self.max_depth is None or \
                abs(self.path_depth(target_dir)) <= self.max_depth

    def _generate_tree_nodes(self, dir_path: Path):
        """Recursively yield ``_TreeNode`` objects starting from
        ``dir_path``

        Parameters
        ----------
        dir_path: Path
            Directory from which to calculate the tree
        """
        # yield current node
        yield DirectoryOrDatasetNode(dir_path, self.path_depth(dir_path))

        # check that we are within max_depth levels
        # (None means unlimited depth)
        if self.max_depth is None or \
                self.path_depth(dir_path) < self.max_depth:

            if self.is_recursive_symlink(dir_path):
                # if symlink points to directory that we may visit or may
                # have visited already, do not recurse into it
                lgr.debug(f"Symlink is potentially recursive, "
                          f"will not traverse target directory: '{dir_path}'")
                return

            try:
                # sort child nodes alphabetically
                # needs to be done *before* calling the exclusion function,
                # because the function may depend on sort order
                all_children = sorted(list(dir_path.iterdir()))
            except OSError:
                # do not recurse into children.
                # the error should have been already stored as
                # `CapturedException` in the `exception` attribute of the
                # current parent node on creation.
                return

            # apply exclusion filters
            children = (
                p for p in all_children
                if not self.exclude_node_func(p)
            )

            # exclusion function could be expensive to compute, so we
            # use a generator for child nodes. however, we need to be able
            # to detect the last child node within each subtree (needed for
            # displaying special end-of-subtree prefix). so we wrap the
            # generator in another 'lookahead' generator to detect the last
            # item.
            for is_last_child, child in yield_with_last_item(children):

                if is_last_child:  # last child of its subtree
                    self.exhausted_levels.add(self.path_depth(child))
                else:
                    self.exhausted_levels.discard(self.path_depth(child))

                # remove exhausted levels that are deeper than the
                # current depth (we don't need them anymore)
                levels = set(self.exhausted_levels)  # copy
                self.exhausted_levels.difference_update(
                    l for l in levels if l > self.path_depth(child)
                )

                try:
                    # `child.is_dir()` could fail because of permission error
                    # error if node is symlink pointing to contents under
                    # inaccessible directory
                    if child.is_dir():
                        # recurse into subdirectories
                        yield from self._generate_tree_nodes(child)
                    else:
                        yield FileNode(child, self.path_depth(child))
                except OSError as ex:
                    # assume it's a file
                    yield FileNode(
                        child,
                        self.path_depth(child),
                        exception=CapturedException(ex, level=10)
                    )

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

        # secondary 'helper' generator that will traverse the whole tree
        # (once) and yield only datasets and their parents directories
        self._ds_generator = self._generate_datasets()

        # current value of the ds_generator. the generator will be initialized
        # lazily, so for now we set the value to a dummy `_TreeNode`
        # with an impossible depth just to distinguish it from None
        # (None means the generator has finished).
        self._next_ds = _TreeNode(self.root, None)

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

        def exclude_func(path: Path):
            """Exclusion function -- here is the crux of the logic for
            pruning the main tree."""

            try:
                # initialize dataset(-parent) generator if not done yet
                if self._next_ds is not None and \
                        self._next_ds.depth is None:  # dummy depth
                    self._advance_ds_generator()

                if path.is_dir() and is_dataset(path):
                    # check if maximum dataset depth is exceeded
                    is_valid_ds = self._is_valid_dataset(path)
                    if is_valid_ds:
                        self._advance_ds_generator()  # go to next dataset(-parent)
                    return not is_valid_ds

                # exclude file or directory underneath a dataset,
                # if it has depth (relative to dataset root) > max_depth,
                # unless (in case of a directory) it is itself the parent of a
                # valid dataset. if it's a parent of a dataset, we don't apply
                # any filters -- it's just a means to get to the next dataset.
                if not self._is_parent_of_ds(path):
                    return self.exclude_node_func(path) or \
                           self._ds_child_node_exceeds_max_depth(path)

            except Exception as ex:
                CapturedException(ex, level=10)  # DEBUG level
                lgr.debug(f"Excluding path '{path}' from tree because "
                          "an exception occurred while applying the "
                          "exclusion filter.")
                return True  # exclude by default

            return False  # do not exclude

        tree = Tree(
            self.root,
            max_depth=None,  # unlimited traversal (datasets could be anywhere)
            exclude_node_func=exclude_func,
        )
        # synchronize exhausted levels with the main tree
        self.exhausted_levels = tree.exhausted_levels

        yield from tree.generate_nodes()

    def _advance_ds_generator(self):
        """Go to the next dataset or parent of dataset"""
        self._next_ds = next(self._ds_generator, None)
        if self._next_ds is not None:
            lgr.debug(
                f"Next dataset" +
                (" parent" if isinstance(self._next_ds, DirectoryNode) else "")
                + f": {self._next_ds.path}")

    def _generate_datasets(self):
        """Generator of dataset nodes and their parent directories starting
        from below the tree root and up to ``max_dataset_depth`` levels.

        This secondary 'helper' tree will be generated in parallel with the
        main tree but will be one step ahead, such that it always points to
        the next dataset (or dataset parent) relative to the current node in
        the main tree.

        We can use it to look into downstream/future nodes and decide
        efficiently whether to prune the current node in the main tree.

        Returns
        -------
        Generator[DirectoryNode or DatasetNode]
        """

        def exclude(p: Path):
            # we won't find any datasets underneath the git folder
            return not p.is_dir() or \
                   (p.is_dir() and p.name == ".git")

        ds_tree = Tree(
            self.root,
            max_depth=None,
            exclude_node_func=exclude,
        )

        # keep track of node paths that have already been yielded
        visited = set([])

        nodes_below_root = ds_tree.generate_nodes()
        next(nodes_below_root)  # skip root node

        for node in nodes_below_root:
            # for each dataset node, yield its parents first, then
            # yield the dataset itself
            if isinstance(node, DatasetNode) and \
                    node.ds_depth <= self.max_dataset_depth and \
                    not self.exclude_node_func(node.path):

                # yield parent directories if not already done
                parents_below_root = node.parents[1:]  # first parent is root
                for depth, parent in enumerate(parents_below_root):
                    if parent not in visited:
                        visited.add(parent)

                        yield DirectoryOrDatasetNode(parent, depth)

                visited.add(node.path)
                yield node

    def _is_valid_dataset(self, path: Path):
        return path.is_dir() and \
               is_path_relative_to(path, self.root) and \
               is_dataset(path) and \
               not self.exclude_node_func(path) and \
               not self._ds_exceeds_max_ds_depth(path)

    def _ds_exceeds_max_ds_depth(self, path: Path):
        ds = DatasetNode(path, self.path_depth(path))
        return ds.ds_depth > self.max_dataset_depth

    def _ds_child_node_exceeds_max_depth(self, path: Path):
        ds_parent = get_dataset_root_datalad_only(path)
        if ds_parent is None:
            return True  # it's not a dataset child, we exclude it

        if not self._is_valid_dataset(ds_parent):
            return True  # also exclude

        # check directory depth relative to the dataset parent
        rel_depth = self.path_depth(path) - self.path_depth(ds_parent)
        assert rel_depth >= 0, "relative depth from parent cannot be < 0 " \
                               f"(path: '{path}', parent: '{ds_parent}')"
        return rel_depth > self.max_depth

    def _is_parent_of_ds(self, path: Path):
        if not path.is_dir():
            return False  # files can't be parents

        if self._next_ds is None:
            return False  # no more datasets, can't be a parent

        if self._next_ds.path == path:
            # we hit a dataset or the parent of a dataset
            self._advance_ds_generator()
            return True

        return False


class _TreeNode:
    """Base class for a directory or file represented as a single tree node
    and printed as single line of the 'tree' output."""
    TYPE = None  # needed for command result dict

    def __init__(self, path: Path, depth: int,
                 exception: CapturedException = None):
        """
        Parameters
        ----------
        path: Path
            Path of the tree node
        depth: int
            Directory depth of the node within its tree
        exception: CapturedException
            Exception that may have occurred at validation/creation
        """
        self.path = path
        self.depth = depth
        # TODO: should be error collection / list of exceptions?
        self.exception = exception

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(str(self.path))

    @property
    def tree_root(self) -> Path:
        """Calculate tree root path from node path and depth"""
        parents = self.parents
        return parents[0] if parents \
            else self.path  # we are the root

    @property
    def parents(self):
        """List of parent paths in top-down order beginning from the tree root.

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

    def is_symlink(self) -> bool:
        """Check if node path is a symlink"""
        try:
            if self.path.is_symlink():
                return True
        except Exception as ex:
            # could fail because of permission issues etc.
            # in which case we just default to False
            self.exception = CapturedException(ex, level=10)
            return False

    def is_broken_symlink(self) -> bool:
        """If node path is a symlink, check if it points to a nonexisting
        or inaccessible target or to itself (self-referencing link). Raise
        exception if the node path is not a symlink."""
        if not self.is_symlink():
            raise ValueError("Node path is not a symlink, cannot check if "
                             f"symlink is broken: {self.path}")

        try:
            self.path.resolve(strict=True)
            return False
        except FileNotFoundError:  # target does not exist
            return True
        except PermissionError:  # target exists but is not accessible
            return True
        except (RuntimeError, OSError):  # symlink loop (OSError on Windows)
            return True
        except Exception as ex:  # probably broken in some other way
            self.exception = CapturedException(ex, level=10)
            return True

    def get_symlink_target(self) -> str:
        """If node path is a symlink, get link target as string. Otherwise,
        return None. Does not check that target path exists."""
        try:
            if self.is_symlink():
                # use os.readlink() instead of Path.readlink() for
                # Python <3.9 compatibility
                return readlink(str(self.path))
        except Exception as ex:
            self.exception = CapturedException(ex, level=10)


class DirectoryNode(_TreeNode):
    TYPE = "directory"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            # get first child if exists. this is a check for whether
            # we can (potentially) recurse into the directory or
            # if there are any filesystem issues (permissions errors, etc)
            # TODO: replace with generic `Path.stat()`? (follows symlinks)
            any(self.path.iterdir())
        except OSError as ex:
            # permission errors etc. are logged and stored as node
            # attribute so they can be passed to results dict.
            # this will overwrite any exception passed to the constructor.
            self.exception = CapturedException(ex, level=10)  # DEBUG level


class FileNode(_TreeNode):
    TYPE = "file"


class DatasetNode(_TreeNode):
    TYPE = "dataset"

    def __init__(self, *args, **kwargs):
        """Does not check if valid dataset. This needs to be done before
        creating the instance."""
        super().__init__(*args, **kwargs)

        try:
            self.ds = require_dataset(self.path, check_installed=False)
            self.is_installed = self.ds.is_installed()
            self.ds_depth, self.ds_absolute_depth = self.calculate_dataset_depth()
        except Exception as ex:
            if self.exception is not None:
                # only if exception has not already been passed to constructor
                self.exception = CapturedException(ex, level=10)

    @lru_cache
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
            superds = get_superdataset(ds.path)

            if superds is None:
                # it is not a dataset, do nothing
                break
            else:
                if superds == ds:
                    # it is a top-level dataset, we are done
                    break

                ds_absolute_depth += 1
                if is_path_relative_to(superds.pathobj, self.tree_root):
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
        try:
            is_ds = is_dataset(path)  # could fail because of permissions etc.
        except Exception as ex:
            # if dataset detection has failed, we fall back to a
            # `DirectoryNode` with the exception stored as attribute
            ce = CapturedException(ex, level=10)
            return DirectoryNode(path, *args, exception=ce, **kwargs)

        node_cls = DatasetNode if is_ds else DirectoryNode
        return node_cls(path, *args, **kwargs)
