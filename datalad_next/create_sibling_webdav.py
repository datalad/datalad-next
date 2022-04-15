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
from datalad.interface.common_opts import (
    recursion_flag,
    recursion_limit
)
from datalad.interface.utils import eval_results
from datalad.support.param import Parameter
from datalad.support.annexrepo import AnnexRepo
from datalad.support.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr,
)
from datalad.support.exceptions import CapturedException
from datalad_next.credman import CredentialManager


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
            doc="URL identifying the sibling root on the target WebDAV server",
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
            doc="""name of the sibling. If none is given, the hostname-part
            of the WebDAV URL will be used.
            With `recursive`, the same name will be used to label all
            the subdatasets' siblings.""",
            constraints=EnsureStr() | EnsureNone()),
        storage_name=Parameter(
            args=("--storage-name",),
            metavar="NAME",
            doc="""name of the storage sibling (git-annex special remote).
            Must not be identical to the sibling name. If not specified,
            defaults to the sibling name plus '-storage' suffix. If only
            a storage sibling is created, this setting is ignored, and
            the primary sibling name is used.""",
            constraints=EnsureStr() | EnsureNone()),
        credential=Parameter(
            args=("--credential",),
            doc="""name of the credential providing a user/password credential
            to be used for authorization. The credential can be supplied via
            configuration setting 'datalad.credential.<name>.user|password', or
            environment variable DATALAD_CREDENTIAL_<NAME>_USER|PASSWORD, or will
            be queried from the active credential store using the provided
            name. If none is provided, the last-used token for the
            API URL realm will be used.""",
        ),
        existing=Parameter(
            args=("--existing",),
            constraints=EnsureChoice('skip', 'error', 'reconfigure', None),
            metavar='MODE',
            doc="""action to perform, if a (storage) sibling is already
            configured under the given name and/or a target already exists.
            In this case, a dataset can be skipped ('skip'), an existing target
            repository be forcefully re-initialized, and the sibling
            (re-)configured ('reconfigure'), or the command be instructed to
            fail ('error').""", ),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
        storage_sibling=Parameter(
            args=("--storage-sibling",),
            dest='storage_sibling',
            metavar='MODE',
            constraints=EnsureChoice(
                'yes', 'export', 'only', 'only-export', 'no'),
            doc="""Both Git history and file content can be hosted on WEBDAV.
            With 'yes', a storage sibling and a Git repository
            sibling are created ('yes').
            Alternatively, creation of the storage sibling can be disabled
            ('no'),
            or a storage sibling can be created only and no Git sibling
            ('only').
            The storage sibling can be set up as a standard git-annex special
            remote that is capable of storage any number of file versions,
            using a content hash based file tree ('yes'|'only'), or
            as an export-type special remote, that can only store a single
            file version corresponding to one unique state of the dataset,
            but using a human-readable data data organization on the WEBDAV
            remote that matches the file tree of the dataset
            ('export'|'only-export')."""),
    )

    @staticmethod
    @datasetmethod(name='create_sibling_webdav')
    @eval_results
    def __call__(
            url: str,
            *,
            dataset: Optional[Union[str, Dataset]] = None,
            name: Optional[str] = None,
            storage_name: Optional[str] = None,
            storage_sibling: Optional[str] = 'yes',
            credential: Optional[str] = None,
            existing: Optional[str] = None,
            recursive: Optional[bool] = False,
            recursion_limit: Optional[int] = None):

        # TODO catch broken URLs
        parsed_url = urlparse(url)
        if not name:
            # not using .netloc to avoid ports to show up in the name
            name = parsed_url.hostname

        if not name:
            # could happen with broken URLs (e.g. without //)
            raise ValueError(
                f"no sibling name given and none could be derived from the URL {url!r}")

        if storage_sibling.startswith('only') and storage_name:
            lgr.warning(
                "Sibling name will be used for storage sibling in "
                "storage-sibling-only mode, but a storage sibling name "
                "was provided"
            )
        if storage_sibling == 'no' and storage_name:
            lgr.warning(
                "Storage sibling setup disabled, but a storage sibling name "
                "was provided"
            )
        if storage_sibling != 'no' and not storage_name:
            storage_name = "{}-storage".format(name)

        if storage_sibling != 'no' and name == storage_name:
            # leads to unresolvable, circular dependency with publish-depends
            raise ValueError("sibling names must not be equal")

        if parsed_url.query:
            raise ValueError("URLs with a query component are not supported")

        ds = require_dataset(
            dataset,
            check_installed=True,
            purpose='create WebDAV sibling(s)')

        res_kwargs = dict(
            action='create_sibling_webdav',
            logger=lgr,
            refds=ds.path,
        )

        # Query existing siblings upfront in order to fail early on
        # existing=='error', since misconfiguration (particularly of special
        # remotes) only to fail in a subdataset later on with that config, can
        # be quite painful.
        if existing == 'error':
            # even if we have to fail, let's report all conflicting siblings
            # in subdatasets, an outside controller can stop the generator
            # if desired
            failed = False
            for dpath, sname in _yield_ds_w_matching_siblings(
                    ds,
                    (name, storage_name),
                    recursive=recursive,
                    recursion_limit=recursion_limit):
                res = get_status_dict(
                    status='error',
                    message=(
                        "a sibling %r is already configured in dataset %r",
                        sname, dpath),
                    **res_kwargs,
                )
                failed = True
                yield res
            if failed:
                return

        credman = CredentialManager(ds.config)
        # determine the credential upfront
        # can be done once at the start, all siblings will live on the same
        # server
        cred = _get_url_credential(credential, url, credman)
        if not cred:
            raise ValueError(
                f'No suitable credential for {url} found or specified')
        try:
            # take them apart here to avoid needly complexity in _dummy() which
            # has impaired error reporting via foreach_dataset()
            cred_user = cred[1]['user']
            cred_password = cred[1]['secret']
        except Exception as e:
            raise ValueError(
                f'No suitable credential for {url} found or specified') from e

        def _dummy(ds, refds, **kwargs):
            """Small helper to prepare the actual call to _create_sibling_webdav()
            for a particular (sub)dataset.

            We only have kwargs to catch whatever it throws at us.
            """
            relpath = ds.pathobj.relative_to(refds.pathobj) if not ds == refds else None
            if relpath:
                # TODO needs more complicated parsing when the URL does not end with a
                # path, but also has query or fragment
                dsurl = f"{url.rstrip('/')}/{relpath}"
            else:
                dsurl = url

            return _create_sibling_webdav(
                ds,
                dsurl,
                storage_sibling=storage_sibling,
                name=name,
                storage_name=storage_name,
                existing=existing,
                credential=(cred_user, cred_password),
            )

        # [{'action': 'foreach-dataset',
        #   'path': '/tmp/datalad_temp_test_mikej910qtc4',
        #   'type': 'dataset',
        #   'command': <function CreateSiblingWebDAV.__call__.<locals>._dummy at 0x7f8c0ab3e0d0>,
        #   'result': 5,
        #   'status': 'ok'}]
        yield from ds.foreach_dataset(
            _dummy,
            return_type='generator',
            result_renderer='disabled',
        )

        # this went well, update the credential
        try:
            credman.set(
                cred[0],
                _lastused=True,
                # strip internal properties like '_edited'
                **{k: v for k, v in cred[1].items() if not k.startswith('_')},
            )
        except Exception as e:
            # we do not want to crash for any failure to store a
            # credential
            lgr.warn(
                'Exception raised when storing credential %r %r: %s',
                *cred,
                CapturedException(e),
            )
        return
        # TODO the rest should be wrapped into a helper function and be executed with
        # foreach-dataset

        name_base = name or parsed_url.hostname
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


def _get_url_credential(credential_name, url, credman):
    """
    """
    cred = None
    realm = None
    if not credential_name:
        # we don't know which one to get, probe the URL
        # needs www_authenticate package, import locally
        # TODO possibly always probe to make sure that
        # we would be passing the credentials to the correct
        # URL (redirects, etc)
        from datalad_next.http_support import (
            probe_url,
            get_auth_realm,
        )
        url, props = probe_url(url)
        # TODO we should actually check if auth is required
        # but for now assume that noone would run a webdav instance
        # without auth -- I believe git-annex (which will ultimately
        # use the credential), would not even attempt to do anything
        # without a credential being provided
        realm = get_auth_realm(url, props.get('auth'))
        cred = credman.query(realm=realm, _sortby='last-used')
        if cred:
            cred = cred[0]

    if not cred:
        try:
            cred = credman.get(
                # if we don't have a name given, use the full URL
                credential_name or url,
                _prompt=f'User name and password are required for WebDAV access at {url}',
                type='user_password',
                realm=realm,
            )
        except Exception as e:
            lgr.debug('Credential retrieval failed: %s', e)

    return cred


def _create_sibling_webdav(
        ds, url, *,
        storage_sibling='no', name=None, storage_name=None, existing='error',
        credential=None):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    storage_sibling: str, optional
    name: str, optional
    storage_name: str, optional
    existing: str, optional
    credential: tuple, optional
    """
    return ("MAKE", ds, url, storage_sibling, name, storage_name, existing, credential)


# TODO in create-sibling-ria something like this is spaghetti-coded, it should be
# RF'ed eventually
def _yield_ds_w_matching_siblings(
        ds, names, recursive=False, recursion_limit=None):
    """(Recursively) inspect a dataset for siblings with particular name(s)

    Parameters
    ----------
    ds: Dataset
      The dataset to be inspected.
    names: iterable
      Sibling names (str) to test for.
    recursive: bool, optional
      Whether to recurse into subdatasets.
    recursion_limit: int, optional
      Recursion depth limit.

    Yields
    ------
    str, str
      Path to the dataset with a matching sibling, and name of the matching
      sibling in that dataset.
    """
    # in recursive mode this check could take a substantial amount of
    # time: employ a progress bar (or rather a counter, because we don't
    # know the total in advance
    pbar_id = 'check-siblings-{}'.format(id(ds))
    if recursive:
        log_progress(
            lgr.info, pbar_id,
            'Start checking pre-existing sibling configuration %s', ds,
            label='Query siblings',
            unit=' Siblings',
        )
    for r in ds.siblings(result_renderer='disabled',
                         return_type='generator',
                         recursive=recursive,
                         recursion_limit=recursion_limit):
        if recursive:
            log_progress(
                lgr.info, pbar_id,
                'Discovered sibling %s in dataset at %s',
                r['name'], r['path'],
                update=1,
                increment=True)
        if not r['type'] == 'sibling' or r['status'] != 'ok':
            # this is an internal status query that has not consequence
            # for the outside world. Be silent unless something useful
            # can be said
            #yield r
            continue
        if r['name'] in names:
            yield r['path'], r['name']
    if recursive:
        log_progress(
            lgr.info, pbar_id,
            'Finished checking pre-existing sibling configuration %s', ds,
        )
