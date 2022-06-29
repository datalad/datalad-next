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

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.support.exceptions import CapturedException
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
    EnsureStr, EnsureInt,
)

lgr = logging.getLogger('datalad.local.tree')


@build_doc
class Tree(Interface):
    """Visualize dataset hierarchy trees

    This command mimics the UNIX/MSDOS 'tree' command to display directory
    trees, highlighting DataLad datasets in the hierarchy.

    """
    result_renderer = 'tailored'

    _params_ = dict(
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify a dataset for which to generate the
            directory tree. If no dataset is given, will generate the
            tree starting from the current directory.""",
            constraints=EnsureDataset() | EnsureNone()),
        path=Parameter(
            args=("path",),
            nargs='?',
            doc="""path to directory from which to generate the tree.
            If empty, will generate the tree starting from the current
            directory.""",
            constraints=EnsureStr() | EnsureNone()),
        level=Parameter(
            args=("-L", "--level",),
            doc="""maximum depth for dataset/directory tree""",
            constraints=EnsureInt() | EnsureNone()),
        # TODO:
        # --include-files (only lists directories by default)
        # --full-paths (equivalent of 'tree -f')
    )

    _examples_ = [
        dict(
            text="Display first-level subdirectories of the current directory, "
                 "with markers highlighting datasets",
            code_py="tree('.')",
            code_cmd="datalad tree -L 1"),
        dict(text="Display the full dataset hierarchy from the current dataset, "
                  "only showing directories that are datasets",
             code_py="tree(dataset='.', full_paths=True)",
             code_cmd="datalad tree -d . --full-paths"),
    ]

    @staticmethod
    @datasetmethod(name='tree')
    @eval_results
    def __call__(path='.', dataset=None, *, level=None):

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='display dataset hierarchy tree')

        yield get_status_dict(
            action='tree',
            status='ok',
            ds=ds,
        )