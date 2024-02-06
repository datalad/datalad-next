"""Iterators for particular types of collections

Most importantly this includes different collections (or containers) for files,
such as a file system directory, or an archive (also see the
``ls_file_collection`` command). However, this module is not per-se limited
to file collections.

Most, if not all, implementation come in the form of a function that takes
a collection identifier or a collection location (e.g., a file system path),
and possibly some additional options. When called, an iterator is returned
that produces collection items in the form of data class instances of
a given type. The particular type can be different across different
collections.


.. currentmodule:: datalad_next.iter_collections
.. autosummary::
   :toctree: generated

   iter_annexworktree
   iter_dir
   iter_gitdiff
   iter_gitstatus
   iter_gittree
   iter_gitworktree
   iter_submodules
   iter_tar
   iter_zip
   TarfileItem
   ZipfileItem
   FileSystemItem
   FileSystemItemType
   GitTreeItemType
   GitWorktreeItem
   GitWorktreeFileSystemItem
   GitDiffItem
   GitDiffStatus
   GitContainerModificationType
"""

from .tarfile import (
    # TODO move to datalad_next.types?
    TarfileItem,
    iter_tar,
)
from .zipfile import (
    # TODO move to datalad_next.types?
    ZipfileItem,
    iter_zip,
)
# TODO move to datalad_next.types?
from .utils import (
    # TODO move to datalad_next.types?
    FileSystemItemType,
    # TODO move to datalad_next.types?
    FileSystemItem,
    compute_multihash_from_fp,
)
from .directory import iter_dir
from .gittree import (
    # TODO move to datalad_next.types?
    GitTreeItemType,
    iter_gittree,
)
from .gitworktree import (
    # TODO move to datalad_next.types?
    GitWorktreeItem,
    # TODO move to datalad_next.types?
    GitWorktreeFileSystemItem,
    iter_gitworktree,
    iter_submodules,
)
from .annexworktree import (
    iter_annexworktree,
)
from .gitdiff import (
    # TODO move to datalad_next.types?
    GitDiffItem,
    # TODO move to datalad_next.types?
    GitDiffStatus,
    # TODO move to datalad_next.types?
    GitContainerModificationType,
    iter_gitdiff,
)
from .gitstatus import (
    iter_gitstatus,
)
