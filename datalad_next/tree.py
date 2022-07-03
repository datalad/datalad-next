# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad_osf package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""'tree'-like command for visualization of dataset hierarchies"""

__docformat__ = 'restructuredtext'

import json
import logging
import os

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

lgr = logging.getLogger('datalad.local.tree')


@build_doc
class Tree(Interface):
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
            doc="""maximum tree depth to display. Can refer to either
            directory depth or dataset hierarchy depth, depending on
            the value of [CMD: --depth-mode CMD][PY: `depth_mode` PY].""",
            constraints=EnsureInt() & EnsureRange(min=1) | EnsureNone()),
        depth_mode=Parameter(
            args=("--depth-mode",),
            doc="""interpret [CMD: --depth CMD][PY: `depth` PY] parameter to mean either
                directory depth or subdataset hierarchy depth.""",
            constraints=EnsureChoice("directory", "dataset")),
        datasets_only=Parameter(
            args=("--datasets-only",),
            doc="""whether to only list directories that are datasets""",
            action='store_true'),
        include_files=Parameter(
            args=("--include-files",),
            doc="""whether to include files in output display""",
            action='store_true'),
        include_hidden=Parameter(
            args=("-a", "--include-hidden",),
            doc="""whether to include hidden files/directories in output display""",
            action='store_true'),
        full_paths=Parameter(
            args=("--full-paths",),
            doc="""whether to display full paths""",
            action='store_true'),
    )

    _examples_ = [
        dict(
            text="Display up to 3 levels of subdirectories and their "
                 "contents starting from the current directory",
            code_py="tree(depth=3, include_files=True)",
            code_cmd="datalad tree -L 3 --include-files"),
        dict(text="List all first- and second-level subdatasets "
                  "of datasets located anywhere under /tmp (regardless "
                  "of directory depth), displaying their full paths",
             code_py="tree('/tmp', depth=2, depth_mode='dataset', datasets_only=True, full_paths=True)",
             code_cmd="datalad tree /tmp -L 2 --depth-mode dataset --datasets-only --full-paths"),
    ]

    @staticmethod
    @datasetmethod(name='tree')
    @eval_results
    def __call__(path='.', *, depth=None, depth_mode='directory',
                 datasets_only=False, include_files=False, include_hidden=False, full_paths=False):

        # print tree output
        walk = Walk(path, depth, datasets_only=datasets_only, include_files=include_files)
        walk.build_tree()
        print(walk.get_tree())
        print(walk.stats())

        # return a generic OK status
        yield get_status_dict(
            action='tree',
            status='ok',
            path=path,
        )


class Walk(object):

    def __init__(self, root: str, max_depth: int,
                 datasets_only=False, include_files=False,
                 include_hidden=False, full_paths=False):
        if not os.path.isdir(root):
            raise ValueError(f"directory '{root}' not found")
        self.root = root
        self.max_depth = max_depth
        self.datasets_only = datasets_only
        self.include_files = include_files
        self.include_hidden = include_hidden
        self.full_paths = full_paths
        self._output = ""
        self._last_children = []
        self._stats = {'dir_count': 0, 'file_count': 0, 'dataset_count': 0}

    def get_tree(self):
        return self._output

    def _current_depth(self, path: str):
        """Directory depth of current path relative to root of the walk"""
        # directory depth can be safely inferred from the number of
        # path separators in path, since pathsep characters are illegal
        # in file or directory names.
        return path.count(os.path.sep) - self.root.rstrip(os.path.sep).count(os.path.sep)

    def _is_last_child(self, path):
        """Whether an item is the last child within its subtree"""
        return path in self._last_children

    def _is_max_depth_reached(self, path):
        """
        If max depth is reached, it means we will not traverse
        any further directories in the next iteration.
        However, we will still list any directories or files
        below the current level.
        Therefore, we 'reach' when we get 1 level before max_depth.
        """
        return self._current_depth(path) == self.max_depth - 1

    def stats(self):
        """Equivalent of tree command's 'report line'.
        TODO: add dataset count"""
        return f"{self._stats}\n"

    def generate_tree_items(self):
        """Generator of directories/files, traversed in depth-first order."""
        for path, dirs, files in os.walk(self.root):

            # exclude hidden files/directories unless specified by arg.
            # we modify os.walk's output in-place.
            if not self.include_hidden:
                dirs[:] = [d for d in dirs if not d.startswith(".")]
                files[:] = [f for f in files if not f.startswith(".")]

            # sort directories and files alphabetically in-place
            dirs.sort()
            files.sort()

            # check if item is the last child within its subtree
            # (needed for applying special formatting)
            if dirs or files:  # if there is a next level
                # files are listed first, directories come last.
                # so we take the last subdirectory if it exists,
                # otherwise the last file.
                self._last_children.append(
                    os.path.join(path, dirs[-1] if dirs else files[-1])
                )

            current_depth = self._current_depth(path)
            item = DirectoryItem(path, current_depth, self._is_last_child(path))

            if not self.datasets_only or self.datasets_only and item.is_dataset():
                yield item
                if current_depth > 0:
                    self._stats['dir_count'] += 1  # do not count root directory

            if self.include_files:
                for file in files:
                    file_path = os.path.join(path, file)
                    yield FileItem(file_path, current_depth + 1,
                                   self._is_last_child(file_path))
                    self._stats['file_count'] += 1

            if self._is_max_depth_reached(path):
                # generate any remaining directory items, which
                # will not be traversed
                for child_dir in dirs:
                    dir_path = os.path.join(path, child_dir)
                    yield DirectoryItem(dir_path, current_depth + 1,
                                        self._is_last_child(dir_path))
                    self._stats['dir_count'] += 1

                # empty in-place the list of next directories
                # to traverse. this effectively stops os.walk's walking.
                dirs[:] = []

    def build_tree(self):
        """
        Structure of tree output line (express in BNF?):
                [padding]?[prefix]?[path]
        Example:
                `|   |   |– mydir`
        """
        # keep track of levels where subtree is exhaused,
        # i.e. we have reached the last child of the subtree.
        # this is needed to build the padding string for each item,
        # which takes into account whether any parent
        # is the last item of its own subtree.
        levels_with_exhausted_subtree = set([])

        for item in self.generate_tree_items():
            lgr.debug(item)

            if item.is_last_child:  # last child of its subtree
                levels_with_exhausted_subtree.add(item.depth)
            else:
                # 'discard' does not raise exception
                # if value does not exist in set
                levels_with_exhausted_subtree.discard(item.depth)

            path = item.path  # display of item path
            padding = ""  # vertical lines for continuing the parent subtrees
            prefix = ""  # single indentation symbol for the given item

            if item.depth > 0:
                # for non-root items, display the basename
                path = os.path.basename(item.path)

                # build padding string
                padding_symbols_for_levels = [
                    "|   "
                    if level not in levels_with_exhausted_subtree
                    else "    "
                    for level in range(1, item.depth)
                ]
                padding = ''.join(padding_symbols_for_levels)

                # set prefix
                if item.is_last_child:
                    prefix = "└── "
                else:
                    prefix = "├── "

            self._output += (padding + prefix + path + "\n")


class DirectoryWalk(Walk):
    """
    Traverse a hierarchy of directories.
    In this context, 'depth' means directory depth.
    """
    pass


class DatasetWalk(Walk):
    """
    Traverse a hierarchy of DataLad datasets and subdatasets.
    In this context, 'depth' means level of subdataset nesting
    (only for datasets installed as subdatasets).
    Considers only proper DataLad datasets (with a dataset ID),
    not regular git/git-annex repos.
    """

    @staticmethod
    def _current_subdataset_depth(path):
        """Subdataset level relative to the root path.
        For example, if building the tree starting from a direct
        subdataset of a top-level parent dataset, will return
        depth 0 for the subdataset root, depth 1 for the
        sub-subdataset, etc."""

        # TODO: make sure we consider datasets only strictly datalad
        # datasets, not any git repo (may be confusing for users)
        return 0

    def _is_max_depth_reached(self, path):
        return self._current_subdataset_depth(path) > self.max_depth


class _TreeItem(object):
    """
    Base class for a directory or file represented in a single
    line of the 'tree' output.
    """

    def __init__(self, path: str, depth: int, is_last_child):
        self.path = path
        self.depth = depth  # directory depth
        self.is_last_child = is_last_child  # if it is last item of its subtree

    def __str__(self):
        return self.path

    def format(self):
        raise NotImplementedError("implemented by subclasses")


class DirectoryItem(_TreeItem):
    def is_dataset(self):
        try:
            ds = require_dataset(self.path, check_installed=True)
            return ds.id is not None
        except (NoDatasetFound, AttributeError):
            return False


class FileItem(_TreeItem):
    pass


class DatasetItem(_TreeItem):
    def __init__(self, *args, abs_subds_depth=None, **kwargs):
        # absolute subdataset depth:
        # if None, it is not a dataset (or it is a .
        # if 0, it is a top-level dataset.
        self.abs_subds_depth = abs_subds_depth
        super().__init__(*args, **kwargs)

    def _absolute_subdataset_depth(self, path):
        """Subdataset level in the context of the full dataset
        hierarchy.
        For example, if building the tree starting from a direct
        subdataset of a top-level parent dataset, will return depth 1
        for the subdataset, depth 2 for the sub-subdataset, etc."""

        # TODO: check how recursion levels are handled e.g. in datalad status
        pass

