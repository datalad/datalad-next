# emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 et:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See COPYING file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""High-level interface for creating a combi-target on a WebDAV capable server
 """
import logging
import os
from typing import (
    Optional,
    Union,
)
from urllib.parse import urlparse

from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.downloaders.credentials import UserPassword
from datalad.interface.base import Interface
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.annexrepo import AnnexRepo
from datalad.support.constraints import (
    EnsureNone,
    EnsureStr,
)


__docformat__ = "restructuredtext"

lgr = logging.getLogger('datalad_next.create_sibling_webdav')


class CreateSiblingWebDAV(Interface):
    """

    """
    _examples_ = [
    ]

    _params_ = dict(
        url=Parameter(
            args=("url",),
            metavar="https://<host>[:<port>]/<local part>",
            doc="URL identifying the target WebDAV server",
            constraints=EnsureStr()),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""specify the dataset to process.  If
            no dataset is given, an attempt is made to identify the dataset
            based on the current working directory""",
            constraints=EnsureDataset() | EnsureNone()),
        name=Parameter(
            args=('-s', '--name',),
            metavar='NAME',
            doc="Name of the sibling.",
            constraints=EnsureStr() | EnsureNone(),
            required=True),
        credential=Parameter(
            args=("--credential",),
            doc="""Name of the credentials that should be used to access
            the WebDAV server.""",
        )
    )

    @staticmethod
    @datasetmethod(name='create_sibling_webdav')
    @eval_results
    def __call__(
            url: str,
            *,
            dataset: Optional[Union[str, Dataset]] = None,
            name: Optional[str] = None,
            credential: Optional[str] = None):
        """

        :param url:
        :param dataset:
        :param name:
        :param credential:
        :return: a generator yielding result records
        """

        ds = require_dataset(
            dataset or ".",
            check_installed=True,
            purpose=f'create WebDAV sibling(s)')

        res_kwargs = dict(
            action=f'create_sibling_webdav',
            logger=lgr,
            refds=ds.path,
        )

        if not isinstance(ds.repo, AnnexRepo):
            yield {
                **res_kwargs,
                "status": "error",
                "msg": "require annex repo to create WebDAV sibling"
            }
            return

        parser_url = urlparse(url)
        if parser_url.query:
            yield {
                **res_kwargs,
                "status": "error",
                "msg": "URLs with query are not yet supported"
            }
            return

        name_base = name or parser_url.hostname
        git_name = name_base + "-wd-vcs"
        annex_name = name_base + "-wd-tree"

        datalad_annex_url = (
            "datalad_annex::"
            + url
            + "?type=webdav&url={noquery}&encryption=none&exporttree=yes"
            + "" if credential is None else f"&dlacredential={credential}"
        )

        # Add the datalad_annex:: sibling to datalad
        from datalad.api import siblings

        siblings(
            action="add",
            dataset=ds,
            name=git_name,
            url=datalad_annex_url)

        # Add a git-annex webdav special remote. This might requires to set
        # the webdav environment variables accordingly.

        credential_holder = UserPassword(name=credential)()
        os.environ["WEBDAV_USERNAME"] = credential_holder["user"]
        os.environ["WEBDAV_PASSWORD"] = credential_holder["password"]
        ds.repo.call_annex([
            "initremote",
            annex_name,
            "type=webdav",
            f"url={url}",
            "exporttree=yes",
            "encryption=none"
        ])

        yield {
            **res_kwargs,
            "status": "ok"
        }
