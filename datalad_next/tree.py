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
from functools import (
    wraps,
    lru_cache
)
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

    This command mimics the UNIX/MSDOS 'tree' utility to generate and
    display a directory tree, with DataLad-specific enhancements.

    It can serve the following purposes:

    1. Glorified 'tree' command
    2. Dataset discovery
    3. Programmatic directory traversal

    *Glorified 'tree' command*

    The rendered command output uses 'tree'-style visualization::

        /tmp/mydir
        ├── [DS~0] ds_A/
        │   └── [DS~1] subds_A/
        └── [DS~0] ds_B/
            ├── dir_B/
            │   ├── file.txt
            │   ├── subdir_B/
            │   └── [DS~1] subds_B0/
            └── [DS~1] (not installed) subds_B1/

        5 datasets, 2 directories, 1 file

    Dataset paths are prefixed by a marker indicating subdataset hierarchy
    level, like ``[DS~1]``.
    This is the absolute subdataset level, meaning it may also take into
    account superdatasets located above the tree root and thus not included
    in the output.
    If a subdataset is registered but not installed (such as after a
    non-recursive ``datalad clone``), it will be prefixed by ``(not
    installed)``. Only DataLad datasets are considered, not pure
    git/git-annex repositories.

    The 'report line' at the bottom of the output shows the count of
    displayed datasets, in addition to the count of directories and
    files. In this context, datasets and directories are mutually
    exclusive categories.

    By default, only directories (no files) are included in the tree,
    and hidden directories are skipped. Both behaviours can be changed
    using command options.

    Symbolic links are always followed.
    This means that a symlink pointing to a directory is traversed and
    counted as a directory (unless it potentially creates a loop in
    the tree).

    *Dataset discovery*

    Using the [CMD: ``--recursive`` CMD][PY: ``recursive`` PY] or [CMD:
    ``--recursion-limit`` CMD][PY: ``recursion_limit`` PY]
    option, this command generates the layout of dataset hierarchies based on
    subdataset nesting level, regardless of their location in the
    filesystem.

    In this case, tree depth is determined by subdataset depth. This mode
    is thus suited for discovering available datasets when their
    location is not known in advance.

    By default, only datasets are listed, without their contents. If
    [CMD: ``--depth`` CMD][PY: ``depth`` PY] is specified additionally,
    the contents of each dataset will be included up to [CMD:
    ``--depth`` CMD][PY: ``depth`` PY] directory levels (excluding
    subdirectories that are themselves datasets).

    Tree filtering options such as [CMD: ``--include-hidden`` CMD][PY:
    ``include_hidden`` PY] only affect which directories are
    reported as dataset contents, not which directories are traversed to find
    datasets.

    **Performance note**: since no assumption is made on the location of
    datasets, running this command with the [CMD: ``--recursive`` CMD][PY:
    ``recursive`` PY] or [CMD: ``--recursion-limit`` CMD][PY:
    ``recursion_limit`` PY] option does a full scan of the whole directory
    tree. As such, it can be significantly slower than a call with an
    equivalent output that uses [CMD: ``--depth`` CMD][PY: ``depth`` PY] to
    limit the tree instead.

    *Programmatic directory traversal*

    The command yields a result record for each tree node (dataset,
    directory or file). The following properties are reported, where available:

    "path"
        Absolute path of the tree node

    "type"
        Type of tree node: "dataset", "directory" or "file"

    "depth"
        Directory depth of node relative to the tree root

    "exhausted_levels"
        Depth levels for which no nodes are left to be generated (the
        respective subtrees have been 'exhausted')

    "count"
        Dict with cumulative counts of datasets, directories and files in the
        tree up until the current node. File count is only included if the
        command is run with the [CMD: ``--include-files`` CMD][PY:
        ``include_files`` PY]
        option.

    "dataset_depth"
        Subdataset depth level relative to the tree root. Only included for
        node type "dataset".

    "dataset_abs_depth"
        Absolute subdataset depth level. Only included for node type "dataset".

    "dataset_is_installed"
        Whether the registered subdataset is installed. Only included for node
        type "dataset".

    "symlink_target"
        If the tree node is a symlink, the path to the link target

    "is_broken_symlink"
        If the tree node is a symlink, whether it is a broken symlink

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
            doc="""limit the tree to maximum level of subdirectories.
            If not specified, will generate the full tree with no depth 
            constraint.
            If paired with [CMD: ``--recursive`` CMD][PY: ``recursive`` PY] or
            [CMD: ``--recursion-limit`` CMD][PY: ``recursion_limit`` PY],
            refers to the maximum directory level to output below
            each dataset.""",
            constraints=EnsureInt() & EnsureRange(min=0) | EnsureNone()),
        recursive=Parameter(
            args=("-r", "--recursive",),
            doc="""produce a dataset tree of the full hierarchy of nested 
            subdatasets. *Note*: may have slow performance on large 
            directory trees.""",
            action='store_true'),
        recursion_limit=Parameter(
            args=("-R", "--recursion-limit",),
            metavar="LEVELS",
            doc="""limit the dataset tree to maximum level of nested 
            subdatasets. 0 means include only top-level datasets, 1 means 
            top-level datasets and their immediate subdatasets, etc. *Note*:
            may have slow performance on large directory trees.""",
            constraints=EnsureInt() & EnsureRange(min=0) | EnsureNone()),
        include_files=Parameter(
            args=("--include-files",),
            doc="""include files in the tree""",
            action='store_true'),
        include_hidden=Parameter(
            args=("--include-hidden",),
            doc="""include hidden files/directories in the tree. This 
            option does not affect which directories will be searched for 
            datasets when specifying [CMD: ``--recursive`` CMD][PY: 
            ``recursive`` PY] or [CMD: ``--recursion-limit`` CMD][PY: 
            ``recursion_limit`` PY]. For example, datasets located underneath 
            the hidden folder `.datalad` will be reported even if [CMD: 
            ``--include-hidden`` CMD][PY: ``include_hidden`` PY] is omitted.""",
            action='store_true'),
    )

    _examples_ = [
        dict(text="Show up to 3 levels of subdirectories below the current "
                  "directory, including files and hidden contents",
             code_py="tree(depth=3, include_files=True, include_hidden=True)",
             code_cmd="datalad tree -L 3 --include-files --include-hidden"),
        dict(text="Find all top-level datasets located anywhere under ``/tmp``",
             code_py="tree('/tmp', recursion_limit=0)",
             code_cmd="datalad tree /tmp -R 0"),
        dict(text="Report all subdatasets recursively and their directory "
                  "contents, up to 1 subdirectory deep within each "
                  "dataset",
             code_py="tree(recursive=True, depth=1)",
             code_cmd="datalad tree -r -L 1"),
    ]

    @staticmethod
    @datasetmethod(name='tree')
    @eval_results
    def __call__(
            path='.',
            *,
            depth=None,
            recursive=False,
            recursion_limit=None,
            include_files=False,
            include_hidden=False):

        if recursive or recursion_limit is not None:
            # special tree defined by subdataset nesting depth
            tree_cls = DatasetTree
            dataset_tree_args = {"max_dataset_depth": recursion_limit}
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
        Each node is printed on one line. The string uses the format::

            [<indentation>] [<branch_tip_symbol>] [<ds_marker>] <path>

        Example line::

            │   │   ├── path_dir_level3
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

    def is_excluded(node: _TreeNode):
        return any((
            isinstance(node, FileNode) if not include_files else False,
            node.path.name.startswith(".") if not include_hidden else False
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


def path_depth(path: Path, root: Path):
    """Calculate directory depth of a path relative to the given root.

    Can also be a negative integer if the path is a parent of the
    tree root.

    Returns
    -------
    int
        Number of levels of the given path *below* the root (positive
        integer) or *above* the tree root (negative integer)

    Raises
    ------
    ValueError
        Like ``path.relative_to()``, raises ``ValueError`` if the path is not
        relative to the root
    """
    sign = 1
    try:
        rpath = path.relative_to(root)
    except ValueError:
        try:
            rpath = root.relative_to(path)
            sign = -1
        except ValueError:
            raise ValueError(
                "Could not calculate directory depth: "
                f"'{path}' is not relative to the tree root "
                f"'{root}' (or vice-versa)")
    return sign * len(rpath.parts)


def is_empty_dir(path: Path):
    """Does not check that path is a directory (to avoid extra
    system calls)"""
    return not any(path.iterdir())


@lru_cache()
def is_dataset(path: Path, installed_only=False):
    """Fast dataset detection.

    Infer that a directory is a dataset if it is either:

    - installed, or
    - not installed, but has an installed superdatset (only if argument
      ``installed_only`` is False)

    Only consider datalad datasets, not plain git/git-annex repos.

    Symlinks pointing to datasets are not resolved, so will always return
    False for symlinks. This prevents potentially detecting duplicate datasets
    if the symlink and its target are both included in the tree.

    Results are cached because the check is somewhat expensive and may
    be run multiple times on the same path.

    Parameters
    ----------
    path: Path
        Path to directory to be identified as dataset or non-dataset

    installed_only: bool
        Whether to ignore datasets that are not installed
    """
    try:
        if path.is_symlink():
            # ignore symlinks even if pointing to datasets, otherwise we may
            # get duplicate counts of datasets
            lgr.debug("Path is a symlink, will not check if it points to a "
                      "dataset: %s", path)
            return False

        if (path / ".datalad" / "config").is_file():
            # could also query `ds.id`, but checking just for existence
            # of config file is quicker.
            return True

        # if it is not installed, check if it has an installed superdataset.
        # instead of querying ds.is_installed() (which checks if the
        # directory has the .git folder), we check if the directory
        # is empty (faster) -- as e.g. after a non-recursive `datalad clone`
        if not installed_only:
            if is_empty_dir(path):
                return get_superdataset(path) is not None

    except Exception as ex:
        # if anything fails (e.g. permission denied), we raise exception
        # instead of returning False. this can be caught and handled by the
        # caller.
        raise NoDatasetFound(f"Cannot determine if '{path.name}' is a "
                             f"dataset") from ex

    return False


@lru_cache()
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


@lru_cache()
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
        if is_dataset(potential_ds_root, installed_only=True):
            return potential_ds_root  # it's a match

        # we go one directory higher and try again
        ds_root = (potential_ds_root / "..").resolve(strict=True)
    return ds_root


@lru_cache()
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
    superds_path = None

    while path:
        parent_path = (path / "..").resolve(strict=True)

        sds_path_ = get_dataset_root_datalad_only(parent_path)
        if sds_path_ is None:
            # no more parents, use previous found
            break

        superds = Dataset(sds_path_)

        # test if path is registered subdataset of the parent
        if not str(path) in get_subds_paths(superds.pathobj):
            break

        # That was a good candidate
        superds_path = sds_path_
        path = parent_path
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
        try:
            root = Path(root)
            self.root = root.resolve(strict=False)
            assert self.root.is_dir(), f"path is not a directory: {self.root}"
        except (AssertionError, OSError) as ex:  # could be permission error
            raise ValueError(f"directory not found: '{root}'") from ex

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

    def __repr__(self):
        return self.__class__.__name__ + \
               f"('{self.root}', max_depth={self.max_depth})"

    @staticmethod
    def default_exclude_func(node):
        """By default, exclude files and hidden directories from the tree"""
        return any(
            (isinstance(node, FileNode), node.path.name.startswith("."))
        )

    def path_depth(self, path: Path):
        return path_depth(path, self.root)

    def _generate_tree_nodes(self, dir_path: Path):
        """Recursively yield ``_TreeNode`` objects starting from ``dir_path``

        Parameters
        ----------
        dir_path: Path
            Directory from which to calculate the tree
        """
        # yield current directory/dataset node
        current_depth = self.path_depth(dir_path)
        current_node = Node(dir_path, current_depth)
        yield current_node

        # check that we are within max_depth levels
        # (None means unlimited depth)
        if self.max_depth is None or \
                current_depth < self.max_depth:

            if current_node.is_symlink() and \
                    current_node.is_recursive_symlink(self.max_depth):
                # if symlink points to directory that we may visit or may
                # have visited already, do not recurse into it
                lgr.debug("Symlink is potentially recursive, "
                          "will not traverse target directory: %s", dir_path)
                return

            if current_node.exception is not None:
                # if some exception occurred when instantiating the node
                # (missing permissions etc), do not recurse into directory
                lgr.debug("Node has exception, will not traverse directory: "
                          "%r", current_node)
                return

            # sort child nodes alphabetically
            # needs to be done *before* calling the exclusion function,
            # because the function may depend on sort order
            all_children = sorted(list(dir_path.iterdir()))
            child_depth = current_depth + 1

            # generator to apply exclusion filter
            def children():
                for child_path in all_children:
                    child_node = Node(child_path, child_depth)
                    if not self.exclude_node_func(child_node):
                        yield child_node

            # exclusion function could be expensive to compute, so we
            # use a generator for child nodes. however, we need to be able
            # to detect the last child node within each subtree (needed for
            # displaying special end-of-subtree prefix). so we wrap the
            # generator in another 'lookahead' generator to detect the last
            # item.
            for is_last_child, child in yield_with_last_item(children()):

                if is_last_child:  # last child of its subtree
                    self.exhausted_levels.add(child_depth)
                else:
                    self.exhausted_levels.discard(child_depth)

                # remove exhausted levels that are deeper than the
                # current depth (we don't need them anymore)
                levels = set(self.exhausted_levels)  # copy
                self.exhausted_levels.difference_update(
                    l for l in levels if l > child_depth
                )

                if isinstance(child, (DirectoryNode, DatasetNode)):
                    # recurse into subdirectories
                    yield from self._generate_tree_nodes(child.path)
                else:
                    # it's a file, just yield it
                    yield child

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
    ``DatasetTree`` is a ``Tree`` whose depth is determined primarily
    by the subdataset hierarchy level (parameter ``max_dataset_depth``).

    Here, ``max_depth`` can also be specified, but it refers to the
    depth of each dataset's content. If this depth is 0, only datasets
    are reported, without any files or subdirectories underneath.

    Because of the different semantics of the ``max_depth`` parameter,
    this class is implemented as a separate subclass of ``Tree``.
    """
    def __init__(self, *args, max_dataset_depth=None, **kwargs):
        super().__init__(*args, **kwargs)

        # default max_dataset_depth 'None' means unlimited subdataset deoth
        self.max_dataset_depth = max_dataset_depth
        if self.max_depth is None:
            # by default, do not include datasets' contents
            self.max_depth = 0

        # lazy initialization of list of datasets and their parents,
        # will be computed when generating nodes for the first time
        self.ds_nodes = []

    def __repr__(self):
        return self.__class__.__name__ + \
                f"('{self.root}', " \
                f"max_dataset_depth={self.max_dataset_depth}, " \
                f"max_depth={self.max_depth})"

    @increment_node_count
    def generate_nodes(self):
        # compute full list of dataset nodes and their parents upfront.
        # this requires an unlimited-depth tree traversal, so will
        # be the slowest operation
        if not self.ds_nodes:
            lgr.debug("Started computing dataset nodes for %r", self)
            self.ds_nodes = list(self.generate_dataset_nodes())
            lgr.debug("Finished computing dataset nodes for %r", self)

        if not self.ds_nodes:
            depth = 0  # no datasets to report on, just yield the root
        else:
            depth = max(node.depth for node in self.ds_nodes) + \
                    self.max_depth  # max levels below the deepest dataset

        tree = Tree(
            self.root,
            max_depth=depth,
            exclude_node_func=self.exclude_func,
        )
        # synchronize exhausted levels with the main tree
        self.exhausted_levels = tree.exhausted_levels

        yield from tree.generate_nodes()

    def generate_dataset_nodes(self):
        """
        Generator of dataset nodes and their parent directories starting
        from below the tree root and up to ``max_dataset_depth`` levels.

        The assumption is that (super)datasets could be located at any level
        of the directory tree. Therefore, this function does a full-depth
        tree traversal to discover datasets.

        Returns
        -------
        Generator[DirectoryNode or DatasetNode]
        """

        def is_excluded(n: _TreeNode):
            # assumption: we won't find datasets underneath the git folder
            return isinstance(n, FileNode) or \
                   (isinstance(n, DirectoryNode) and n.path.name == ".git")

        # keep track of traversed nodes
        # (needed to prevent yielding duplicates)
        visited = set([])

        ds_tree = Tree(
            self.root,
            max_depth=None,  # unlimited depth, datasets could be anywhere
            exclude_node_func=is_excluded,
        )
        nodes_below_root = ds_tree.generate_nodes()
        next(nodes_below_root)  # skip root node

        for node in nodes_below_root:
            # for each dataset node, yield its parents first, then
            # yield the dataset itself
            if isinstance(node, DatasetNode) and \
                    (self.max_dataset_depth is None or
                     node.ds_depth <= self.max_dataset_depth) and \
                    not self.exclude_node_func(node):

                # yield parent directories if not already done
                parents_below_root = node.parents[1:]  # first parent is root
                for par_depth, par_path in enumerate(parents_below_root):
                    parent = Node(par_path, par_depth)

                    if parent not in visited:
                        visited.add(parent)
                        yield parent

                visited.add(node)
                yield node

    def exclude_func(self, node):
        """Exclusion function for pruning the main tree"""
        include, exclude = False, True  # prevent headaches

        try:
            if node in self.ds_nodes:
                # we hit a dataset or the parent of a dataset
                return include

            # if `max_depth` is specified for returning dataset contents,
            # exclude non-dataset nodes below a dataset that have
            # depth (relative to parent dataset) > max_depth
            if self.max_depth > 0 and \
                    not isinstance(node, DatasetNode):

                # check that node is the child of a dataset
                ds_parent = self._find_closest_ds_parent(node)
                if ds_parent is not None:
                    rel_depth = node.depth - ds_parent.depth
                    exceeds_max_depth = rel_depth > self.max_depth
                    # also filter by the user-supplied
                    # exclusion logic in `exclude_node_func`
                    return exceeds_max_depth or \
                        self.exclude_node_func(node)

        except Exception as ex:
            CapturedException(ex, level=10)  # DEBUG level
            lgr.debug("Excluding node from tree because "
                      "an exception occurred while applying "
                      "exclusion filter: %r", node)

        return exclude  # exclude by default

    def _find_closest_ds_parent(self, node):
        ds_parent = None
        for parent_path in node.path.parents:  # bottom-up order
            ds_parent = next((n for n in self.ds_nodes
                              if n.path == parent_path and
                              isinstance(n, DatasetNode)), None)
            if ds_parent is not None:
                break

        return ds_parent


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
        self.exception = exception

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(str(self.path))

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.path}', depth={self.depth})"

    @property
    def tree_root(self) -> Path:
        """Calculate tree root path from node path and depth"""
        parents = self.parents
        return parents[0] if parents \
            else self.path  # we are the root

    @property
    # More accurate annotation only from PY3.9 onwards
    # def parents(self) -> list[Path]:
    def parents(self) -> list:
        """List of parent paths in top-down order beginning from the tree root.
        Assumes the node path to be already normalized.

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

    def is_recursive_symlink(self, max_depth) -> bool:
        """Detect symlink pointing to a directory within the same tree
        (directly or indirectly).

        The default behaviour is to follow symlinks when traversing the tree.
        However, we should not follow symlinks to directories that we may
        visit or have visited already, i.e. are also located under the tree
        root or any parent of the tree root (within a distance of
        ``max_depth``).

        Otherwise, the same subtree could be generated multiple times in
        different places, potentially in a recursive loop (e.g. if the
        symlink points to its parent).

        This is similar to the logic of the UNIX 'tree' command, but goes a
        step further to prune all duplicate subtrees.

        Parameters
        ----------
        max_depth
            Max depth of the ``Tree`` to which this node belongs
        """
        if not self.is_symlink():
            raise ValueError("Node path is not a symlink, cannot check if "
                             f"symlink is recursive: {self.path}")

        if isinstance(self, FileNode):
            # we are only interested in symlinks pointing to a directory
            return False

        if self.is_broken_symlink():
            # cannot identify target, no way to know if link is recursive
            return False

        target_dir = self.path.resolve()
        tree_root = self.tree_root

        # either:
        # - target dir is within `max_depth` levels beneath the tree
        #   root, so it will likely be yielded or has already been
        #   yielded (bar any exclusion filters)
        # - target dir is a parent of the tree root, so we may still
        #   get into a loop if we recurse more than `max_depth` levels
        try:
            rel_depth = abs(path_depth(target_dir, tree_root))
            return max_depth is None or \
                rel_depth <= max_depth
        except ValueError:
            # cannot compute path depth because target is outside
            # of the tree root, so no loop is possible
            return False


class Node:
    """
    Factory class for creating a ``_TreeNode`` of a particular subclass.
    Detects whether the path is a file or a directory or dataset,
    and handles any exceptions (permission errors, broken symlinks, etc.)
    """
    def __new__(cls, path: Path, depth: int, **kwargs):
        if not isinstance(path, Path):
            raise ValueError("path must be a Path object")

        node_cls = FileNode
        captured_ex = None
        try:
            if path.is_dir():
                if is_dataset(path):
                    node_cls = DatasetNode
                else:
                    node_cls = DirectoryNode
        except NoDatasetFound as ex:  # means 'is_dataset()' failed
            # default to directory node
            # just log the exception, do not set it as node attribute
            CapturedException(ex, level=10)
            node_cls = DirectoryNode
        except Exception as ex:  # means 'is_dir()' failed
            # default to file node
            # set exception as node attribute
            captured_ex = CapturedException(ex, level=10)

        return node_cls(path, depth, exception=captured_ex, **kwargs)


class DirectoryNode(_TreeNode):
    TYPE = "directory"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        try:
            # get first child if exists. this is a check for whether
            # we can potentially recurse into the directory or
            # if there are any filesystem issues (permissions errors, etc)
            any(self.path.iterdir())
        except OSError as ex:
            # permission errors etc. are logged and stored as node
            # attribute so they can be passed to results dict.
            # this will overwrite any exception passed to the constructor,
            # since we assume that this exception is closer to the root
            # cause.
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

    @lru_cache()
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
            superds = get_superdataset(ds.pathobj)

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
