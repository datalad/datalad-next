# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad_osf package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""List content of collection objects like datasets, tar-files, etc"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from urllib.parse import urlparse

import fsspec

from datalad.interface.base import eval_results
from datalad_next.commands import (
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    build_doc,
    generic_result_renderer,
    get_status_dict,
)
from datalad_next.constraints import EnsureChoice
from datalad_next.datasets import datasetmethod
from datalad_next.uis import ui_switcher as ui
from datalad_next.url_operations.fsspec import FsspecUrlOperations


__docformat__ = 'restructuredtext'

lgr = logging.getLogger('datalad.local.list_collection')

action_name = 'list-collection'


# Collection types that are supported. Most are auto-detected. `annex` has to be
# specified, because it always exists in combination with `git`, and  git` takes
# preference. Therefore `annex` is never auto-detected.
#
# DataLad datasets (`dataset`-type) also always exists in combination with `git`
# and take precedence over `git` and `annex`.
#
# A bare git repository is always autodetected as `git-bare`, even if it
# contains a DataLad dataset or an annex
known_types = [
    'tar',
    'zip',
    'dataset',
    'git',
    'git-bare',
    'directory',
    'annex',
    '7z',
]

# File type identification by suffix
known_suffixes = {
    'tar': [['.tar'], ['.tgz'], ['.tar', '.gz']],
    'zip': [['.zip']],
    '7z': [['.7z']],
}

# Directory type identification by child names, the order of the entries matters
identifying_children = {
    'git-bare': [
        'branches', 'config', 'description', 'HEAD',
        'hooks', 'info', 'objects', 'refs'
    ],
    'dataset': ['.datalad'],
    'git': ['.git'],
}


class ListCollectionParamValidator(EnsureCommandParameterization):

    def __init__(self):
        super().__init__(
            param_constraints=dict(
                collection_type=EnsureChoice(*known_types)))


@build_doc
class ListCollection(ValidatedInterface):
    """List content of collection objects

    """
    result_renderer = 'tailored'

    _params_ = dict(
        collection_type=Parameter(
            args=("-t", "--collection-type"),
            doc="""specify the type of the collection that should be listed. 
            This argument will override automatic type detection. Automated
            type detection is based on the collection type, e.g. file or
            directory, and on the name of the collection, e.g. 'xyz.tar'
            (not implemented yet)."""),
        location=Parameter(
            doc="""the location of the object that should be listed. The 
            format defined by `fsspec`"""),
    )

    _examples_ = [
        dict(text='List tar file at an http-location',
             code_py="list_collection('tar:///::http://example.com/archive.tar', collection_type='tar')",
             code_cmd="datalad list-collection -t tar 'tar:///::http://example.com/archive.tar'"),
    ]

    _validator_ = ListCollectionParamValidator()

    @staticmethod
    @datasetmethod(name='credentials')
    @eval_results
    def __call__(location,
                 collection_type=None):

        fsspec_url_ops = FsspecUrlOperations()

        # Try to open the file system without providing credentials first
        filesystem, url_path, properties = fsspec_url_ops._get_fs(location, credential=None)
        for record in show_specfs_tree(filesystem, url_path):
            yield {
                **record,
                'url_path': url_path or location}

    @staticmethod
    def custom_result_renderer(res, **_):
        if res['action'] != action_name:
            generic_result_renderer(res)
            return
        ui.message(json.dumps(res))


def show_specfs_tree(filesystem, current_path=''):
    current_element = filesystem.stat(current_path)
    if current_element['type'] == 'file':
        yield get_status_dict(
            action=action_name,
            status='ok',
            object=current_element)
        return

    for element in filesystem.ls(current_path):
        if isinstance(element, str):
            element = filesystem.stat(element)
        yield get_status_dict(
            action=action_name,
            status='ok',
            object=element)
        if element['type'] == 'directory':
            yield from show_specfs_tree(
                filesystem, element['name'])


def detect_collection_type(url) -> str:

    open_file = fsspec.open(url)
    base_path = Path(urlparse(open_file.full_name).path)

    # We use an appended '/' to detect subdirectories because
    # some file systems signal a file at a directory
    # name. For example, a HTTP server provide
    # some content, e.g. a HTML page that lists the
    # directory. We could work around that by adding
    # a trailing '/' (at least on unix) to the path,
    # but we can also let fsspec do that vie the `ls`
    # file system method. We do the latter, because
    # we need the subdirectories anyway

    if open_file.fs.isdir(open_file.full_name + '/'):

        # For the remaining checks, we need the relative child names.
        children = open_file.fs.ls(open_file.full_name)
        child_names = [Path(urlparse(url).path).name for url in children]

        for type_identifier, children_list in identifying_children.items():
            if all([identifying_child in child_names
                    for identifying_child in children_list]):
                collection_type = type_identifier
                break
        else:
            collection_type = 'directory'

    else:

        for type_identifier, suffixes_list in known_suffixes.items():
            if base_path.suffixes in suffixes_list:
                collection_type = type_identifier
                break
        else:
            collection_type = 'file'

    return collection_type
