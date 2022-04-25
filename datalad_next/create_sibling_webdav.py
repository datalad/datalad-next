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
from typing import (
    Optional,
    Union,
)
from urllib.parse import (
    quote as urlquote,
    urlparse,
    urlunparse,
)

from datalad.distribution.dataset import (
    Dataset,
    EnsureDataset,
    datasetmethod,
    require_dataset,
)
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.common_opts import (
    recursion_flag,
    recursion_limit
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import eval_results
from datalad.log import log_progress
from datalad.support.param import Parameter
from datalad.support.constraints import (
    EnsureChoice,
    EnsureNone,
    EnsureStr,
)
from datalad.support.exceptions import CapturedException
from datalad_next.credman import CredentialManager
from datalad_next.utils import (
    get_specialremote_credential_properties,
    update_specialremote_credential,
)


__docformat__ = "restructuredtext"

lgr = logging.getLogger('datalad.distributed.create_sibling_webdav')


@build_doc
class CreateSiblingWebDAV(Interface):
    """Create a sibling(-tandem) on a WebDAV server

    WebDAV is standard HTTP protocol extension for placing files on a server
    that is supported by a number of commercial storage services (e.g.
    4shared.com, box.com), but also instances of cloud-storage solutions like
    Nextcloud or ownCloud. These software packages are also the basis for
    some institutional or public cloud storage solutions, such as EUDAT B2DROP.

    For basic usage, only the URL with the desired dataset location on a WebDAV
    server needs to be specified for creating a sibling. However, the sibling
    setup can be flexibly customized (no storage sibling, or only a storage
    sibling, multi-version storage, or human-browsable single-version storage).

    When creating siblings recursively for a dataset hierarchy, subdatasets
    exports are placed at their corresponding relative paths underneath the
    root location on the WebDAV server.


    Git-annex implementation details

    Storage siblings are presently always configured to be enabled
    automatically on cloning a dataset. Due to a limitation of git-annex, this
    will initially fails (missing credentials), but a command to properly
    enable the storage sibling will be displayed.
    See https://github.com/datalad/datalad/issues/6634 for details.

    This command does not (and likely will not) support embedding credentials
    in the repository (see ``embedcreds`` option of the git-annex ``webdav``
    special remote; https://git-annex.branchable.com/special_remotes/webdav),
    because such credential copies would need to be updated, whenever they
    change or expire. Instead, credentials are retrieved from DataLad's
    credential system. In many cases, credentials are determined automatically,
    based on the HTTP authentication realm identified by a WebDAV server.

    This command does not support setting up encrypted remotes (yet). Neither
    for the storage sibling, nor for the regular Git-remote. However, adding
    support for it is primarily a matter of extending the API of this command,
    and to pass the respective options on to the underlying git-annex
    setup.

    This command does not support setting up chunking for webdav storage
    siblings (https://git-annex.branchable.com/chunking).
    """
    _examples_ = [
       dict(text="Create a WebDAV sibling tandem for storage a dataset's file "
                 "content and revision history. A user will be prompted for "
                 "any required credentials, if they are not yet known.",
             code_py="create_sibling_webdav(url='https://webdav.example.com/myds')",
             code_cmd='datalad create-sibling-webdav "https://webdav.example.com/myds"'),
       dict(text="Such a dataset can be cloned by DataLad via a specially "
                 "crafted URL. Again, credentials are automatically "
                 "determined, or a user is prompted to enter them",
            code_py="clone('datalad-annex::?type=webdav&encryption=none&url=https://webdav.example.com/myds')",
            code_cmd='datalad clone "datalad-annex::?type=webdav&encryption=none&url=https://webdav.example.com/myds"'),
    ]

    _params_ = dict(
        url=Parameter(
            args=("url",),
            metavar='URL',
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
            metavar='NAME',
            doc="""name of the credential providing a user/password credential
            to be used for authorization. The credential can be supplied via
            configuration setting 'datalad.credential.<name>.user|secret', or
            environment variable DATALAD_CREDENTIAL_<NAME>_USER|SECRET, or will
            be queried from the active credential store using the provided
            name. If none is provided, the last-used credential for the
            authentication realm associated with the WebDAV URL will be used.
            Only if a credential name was given, it will be encoded in the
            URL of the created WebDAV Git remote, credential auto-discovery
            will be performed on each remote access.""",
        ),
        existing=Parameter(
            args=("--existing",),
            constraints=EnsureChoice('skip', 'error', 'reconfigure'),
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
            ('export'|'only-export').
            When a storage sibling and a regular sibling are created, a
            publication dependency on the storage sibling is configured
            for the regular sibling in the local dataset clone."""),
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
            storage_sibling: str = 'yes',
            credential: Optional[str] = None,
            existing: str = 'error',
            recursive: bool = False,
            recursion_limit: Optional[int] = None):

        parsed_url = urlparse(url)
        if parsed_url.query:
            raise ValueError(
                f"URLs with query component are not supported: {url!r}")
        if parsed_url.fragment:
            raise ValueError(
                f"URLs with fragment are not supported: {url!r}")
        if not parsed_url.netloc:
            raise ValueError(
                f"URLs without network location are not supported: {url!r}")
        if parsed_url.scheme not in ("http", "https"):
            raise ValueError(
                f"Only 'http'- or 'https'-scheme are supported: {url!r}")
        if parsed_url.scheme == "http":
            lgr.warning(
                f"Using 'http:' ({url!r}) means that WEBDAV credentials might"
                " be sent unencrypted over network links. Consider using "
                "'https:'.")

        if not name:
            # not using .netloc to avoid ports to show up in the name
            name = parsed_url.hostname

        # ensure values of critical switches. this duplicated the CLI processing, but
        # compliance is critical in a python session too.
        # whe cannot make it conditional to apimode == cmdline, because this command
        # might be called by other python code
        for param, value in (('storage_sibling', storage_sibling),
                             ('existing', existing)):
            try:
                CreateSiblingWebDAV._params_[param].constraints(value)
            except ValueError as e:
                # give message a context
                raise ValueError(f"{param!r}: {e}") from e

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

        # determine the credential upfront
        # can be done once at the start, all siblings will live on the same
        # server
        # if all goes well, we'll store a credential (update) at the very end
        credman = CredentialManager(ds.config)
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
                dsurl = f"{urlunparse(parsed_url)}/{relpath}"
            else:
                dsurl = url

            return _create_sibling_webdav(
                ds,
                dsurl,
                # we pass the given, not the discovered, credential name!
                # given a name means "take this particular one", not giving a
                # name means "take what is best". Only if we pass this
                # information on, we achieve maintaining this behavior
                credential_name=credential,
                credential=(cred_user, cred_password),
                storage_sibling=storage_sibling,
                name=name,
                storage_name=storage_name,
                existing=existing,
            )

        # Generate a sibling for dataset "ds", and for sub-datasets if recursive
        # is True.
        for res in ds.foreach_dataset(_dummy,
                                      return_type='generator',
                                      result_renderer='disabled',
                                      recursive=recursive,
                                      recursion_limit=recursion_limit):
            # unwind result generator
            if 'result' in res:
                yield from res['result']

        # this went well, update the credential
        credname, credprops = cred
        update_specialremote_credential(
            'webdav',
            credman,
            credname,
            credprops,
            credtype_hint='user_password',
            duplicate_hint=
            'Specify a credential name via the `credential` parameter '
            ' and/or configure a credential with the datalad-credentials '
            'command{}'.format(
                f' with a `realm={credprops["realm"]}` property'
                if 'realm' in credprops else ''),
        )


def _get_url_credential(name, url, credman):
    """
    Returns
    -------
    (str, dict)
      Credential name (possibly different from the input, when a credential
      was discovered based on the URL), and credential properties
    """
    cred = None
    credprops = {}
    if not name:
        credprops = get_specialremote_credential_properties(
            dict(type='webdav', url=url))
        if credprops:
            creds = credman.query(_sortby='last-used', **credprops)
            if creds:
                name, cred = creds[0]

    if not cred:
        try:
            cred = credman.get(
                # name could be none
                name=name,
                _prompt=f'User name and password are required for WebDAV access at {url}',
                type='user_password',
                realm=credprops.get('realm'),
            )
        except Exception as e:
            lgr.debug('Credential retrieval failed: %s', e)

    return name, cred


def _create_sibling_webdav(
        ds, url, *,
        credential_name, credential,
        storage_sibling='no', name=None, storage_name=None, existing='error'):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    credential_name: str
    credential: tuple
    storage_sibling: str, optional
    name: str, optional
    storage_name: str, optional
    existing: str, optional
    """
    # simplify downstream logic, export yes or no
    export_storage = 'export' in storage_sibling

    if storage_sibling != 'no':
        yield from _create_storage_sibling(
            ds,
            url,
            storage_name,
            existing,
            credential,
            export=export_storage,
        )
    if 'only' not in storage_sibling:
        yield from _create_git_sibling(
            ds,
            url,
            name,
            existing,
            credential_name,
            credential,
            export=export_storage,
            dependency=None if storage_sibling == 'no' else storage_name,
        )


def _create_git_sibling(
        ds, url, name, existing, credential_name, credential, export,
        dependency=None):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    name: str
    existing: {skip, error, reconfigure}
    credential_name: str
    credential: tuple
    export: bool
    """

    remote_url = \
        "datalad-annex::?type=webdav&encryption=none" \
        "&exporttree={export}&url={url}".format(
            export='yes' if export else 'no',
            # urlquote, because it goes into the query part of another URL
            url=urlquote(url))
    if credential_name:
        # we need to quote the credential name too.
        # e.g., it is not uncommon for credentials to be named after URLs
        remote_url += f'&dlacredential={urlquote(credential_name)}'

    yield from ds.siblings(
        # TODO set vs add, consider `existing`
        action="add",
        name=name,
        url=remote_url,
        # TODO probably needed when reconfiguring, but needs credential patch
        fetch=False,
        publish_depends=dependency,
        return_type='generator',
        result_renderer='disabled')


def _create_storage_sibling(ds, url, name, existing, credential, export):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    name: str
    existing: {skip, error, reconfigure}
    credential: tuple
    export: bool
    """
    cmd_args = [
        # TODO not always init, consider existing
        'initremote',
        name,
        "type=webdav",
        f"url={url}",
        f"exporttree={'yes' if export else 'no'}",
        "encryption=none",
        "autoenable=true"
    ]
    # delayed heavy-ish import
    from unittest.mock import patch
    # Add a git-annex webdav special remote. This requires to set
    # the webdav environment variables accordingly.
    with patch.dict('os.environ', {
            'WEBDAV_USERNAME': credential[0],
            'WEBDAV_PASSWORD': credential[1],
    }):
        ds.repo.call_annex(cmd_args)
    yield get_status_dict(
        ds=ds,
        status='ok',
        action='create_sibling_webdav.storage',
    )


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
