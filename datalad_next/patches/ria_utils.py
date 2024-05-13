"""Patch ria_utils.py tp work with abstract RIA-paths

The ORARemote and CreateSiblingRia-patches use an abstract representation of
all paths that are related to elements of a RIA-store, e.g. `ria-layout-version`
or `ria-object-dir`. This patch adapts `ria_utils.py` to this modification.
"""
from __future__ import annotations
import logging
from pathlib import PurePosixPath

from datalad.customremotes.ria_utils import (
    UnknownLayoutVersion,
    get_layout_locations,
)

from . import apply_patch


lgr = logging.getLogger('datalad.customremotes.ria_utils')


# The following two blocks of comments and definitions are verbatim copies from
# `datalad.cutomremotes.ria_utils`

# TODO: Make versions a tuple of (label, description)?
# Object tree versions we introduced so far. This is about the layout within a
# dataset in a RIA store
known_versions_objt = ['1', '2']

# Dataset tree versions we introduced so far. This is about the layout of
# datasets in a RIA store
known_versions_dst = ['1']


# taken from `ria_utils._ensure_version` from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def ria_utils__ensure_version(io, base_path, version):
    """Check a store or dataset version and make sure it is declared

    Parameters
    ----------
    io: SSHRemoteIO or LocalIO
    base_path: PurePosixPath
      root path of a store or dataset
    version: str
      target layout version of the store (dataset tree)
    """
    # PATCH: ensure that `base_path` is an instance of `PurePosixPath`.
    assert base_path.__class__ is PurePosixPath

    # PATCH: convert abstract `ria-layout-version`-path to concrete IO-specific
    # path
    version_file = io.url2transport_path(base_path / 'ria-layout-version')
    if io.exists(version_file):
        existing_version = io.read_file(version_file).split('|')[0].strip()
        if existing_version != version.split('|')[0]:
            # We have an already existing location with a conflicting version on
            # record.
            # Note, that a config flag after pipe symbol is fine.
            raise ValueError("Conflicting version found at target: {}"
                             .format(existing_version))
        else:
            # already exists, recorded version fits - nothing to do
            return
    # Note, that the following does create the base-path dir as well, since
    # mkdir has parents=True:
    # PATCH: convert abstract path `base_path` to concrete IO-specific path
    # before handing it to `mkdir`.
    io.mkdir(io.url2transport_path(base_path))
    io.write_file(version_file, version)


# taken from `ria_utils.create_store` from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def ria_utils_create_store(io, base_path, version):
    """Helper to create a RIA store

    Note, that this is meant as an internal helper and part of intermediate
    RF'ing. Ultimately should lead to dedicated command or option for
    create-sibling-ria.

    Parameters
    ----------
    io: SSHRemoteIO or LocalIO
      Respective execution instance.
      Note: To be replaced by proper command abstraction
    base_path: PurePosixPath
      root url path of the store
    version: str
      layout version of the store (dataset tree)
    """
    # PATCH: ensure that `base_path` is an instance of `PurePosixPath`.
    assert isinstance(base_path, PurePosixPath)

    # At store level the only version we know as of now is 1.
    if version not in known_versions_dst:
        raise UnknownLayoutVersion("RIA store layout version unknown: {}."
                                   "Supported versions: {}"
                                   .format(version, known_versions_dst))
    _ensure_version(io, base_path, version)
    error_logs = base_path / 'error_logs'
    # PATCH: convert abstract path `error_logs` to concrete IO-specific path
    # before handing it to `mkdir`.
    io.mkdir(io.url2transport_path(error_logs))


# taken from `ria_utils.create_ds_in_store` from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732
def ria_utils_create_ds_in_store(io,
                                 base_path,
                                 dsid,
                                 obj_version,
                                 store_version,
                                 alias=None,
                                 init_obj_tree=True
                                 ):
    """Helper to create a dataset in a RIA store

    Note, that this is meant as an internal helper and part of intermediate
    RF'ing. Ultimately should lead to a version option for create-sibling-ria
    in conjunction with a store creation command/option.

    Parameters
    ----------
    io: SSHRemoteIO or LocalIO
      Respective execution instance.
      Note: To be replaced by proper command abstraction
    base_path: PurePosixPath
      root path of the store
    dsid: str
      dataset id
    store_version: str
      layout version of the store (dataset tree)
    obj_version: str
      layout version of the dataset itself (object tree)
    alias: str, optional
      alias for the dataset in the store
    init_obj_tree: bool
      whether or not to create the base directory for an annex objects tree (
      'annex/objects')
    """
    # PATCH: ensure that `base_path` is an instance of `PurePosixPath`.
    assert base_path.__class__ is PurePosixPath

    # TODO: Note for RF'ing, that this is about setting up a valid target
    #       for the special remote not a replacement for create-sibling-ria.
    #       There's currently no git (bare) repo created.

    try:
        # TODO: This is currently store layout version!
        #       Too entangled by current get_layout_locations.
        dsgit_dir, archive_dir, dsobj_dir = \
            get_layout_locations(int(store_version), base_path, dsid)
    except ValueError as e:
        raise UnknownLayoutVersion(str(e))

    if obj_version not in known_versions_objt:
        raise UnknownLayoutVersion("Dataset layout version unknown: {}. "
                                   "Supported: {}"
                                   .format(obj_version, known_versions_objt))

    _ensure_version(io, dsgit_dir, obj_version)

    # PATCH: convert abstract path `archive_dir` to concrete IO-specific path
    # before handing it to `mkdir`.
    io.mkdir(io.url2transport_path(archive_dir))
    if init_obj_tree:
        # PATCH: convert abstract path `dsobj_dir` to concrete IO-specific path
        # before handing it to `mkdir`.
        io.mkdir(io.url2transport_path(dsobj_dir))
    if alias:
        alias_dir = base_path / "alias"
        # PATCH: convert abstract path `alias_dir` to concrete IO-specific path
        # before handing it to `mkdir`.
        io.mkdir(io.url2transport_path(alias_dir))
        try:
            # go for a relative path to keep the alias links valid
            # when moving a store
            io.symlink(
                # PATCH: convert abstract relative path to concrete IO-specific
                # path before handing it to `symlink`.
                io.url2transport_path(
                    PurePosixPath('..') / dsgit_dir.relative_to(base_path)
                ),
                # PATCH: convert abstract alias-path to concrete IO-specific path
                # before handing it to `symlink`.
                io.url2transport_path(alias_dir / alias)
            )
        except FileExistsError:
            lgr.warning("Alias %r already exists in the RIA store, not adding an "
                        "alias.", alias)


_ensure_version = ria_utils__ensure_version


# Overwrite `create_store` to handle paths properly
apply_patch(
    'datalad.customremotes.ria_utils',
    None,
    'create_store',
    ria_utils_create_store,
)


# Overwrite `create_ds_in_store` to handle paths properly
apply_patch(
    'datalad.customremotes.ria_utils',
    None,
    'create_ds_in_store',
    ria_utils_create_ds_in_store,
)


# Overwrite `_ensure_version` to handle paths properly
apply_patch(
    'datalad.customremotes.ria_utils',
    None,
    '_ensure_version',
    ria_utils__ensure_version,
)
