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
import os.path

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


class ListCollectionParamValidator(EnsureCommandParameterization):

    known_types = [
        'tar',
        'zip',
        'dataset',
        'git',
        'directory',
        'annex'
    ]

    def __init__(self):
        super().__init__(
            param_constraints=dict(
                collection_type=EnsureChoice(*self.known_types)))


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
