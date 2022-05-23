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
from datalad.interface.utils import (
    generic_result_renderer,
    eval_results,
)
from datalad.log import log_progress
from datalad.support.annexrepo import AnnexRepo
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

    WebDAV is a standard HTTP protocol extension for placing files on a server
    that is supported by a number of commercial storage services (e.g.
    4shared.com, box.com), but also instances of cloud-storage solutions like
    Nextcloud or ownCloud. These software packages are also the basis for
    some institutional or public cloud storage solutions, such as EUDAT B2DROP.

    For basic usage, only the URL with the desired dataset location on a WebDAV
    server needs to be specified for creating a sibling. However, the sibling
    setup can be flexibly customized (no storage sibling, or only a storage
    sibling, multi-version storage, or human-browsable single-version storage).

    This command does not check for conflicting content on the WebDAV
    server!

    When creating siblings recursively for a dataset hierarchy, subdataset
    exports are placed at their corresponding relative paths underneath the
    root location on the WebDAV server.


    Collaboration on WebDAV siblings

    The primary use case for WebDAV siblings is dataset deposition, where
    only one site is uploading dataset and file content updates.
    For collaborative workflows with multiple contributors, please make sure
    to consult the documentation on the underlying ``datalad-annex::``
    Git remote helper for advice on appropriate setups:
    http://docs.datalad.org/projects/next/


    Git-annex implementation details

    Storage siblings are presently configured to NOT be enabled
    automatically on cloning a dataset. Due to a limitation of git-annex, this
    would initially fail (missing credentials). Instead, an explicit
    ``datalad siblings enable --name <storage-sibling-name>`` command must be
    executed after cloning. If necessary, it will prompt for credentials.

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
    and passing the respective options on to the underlying git-annex setup.

    This command does not support setting up chunking for webdav storage
    siblings (https://git-annex.branchable.com/chunking).
    """
    _examples_ = [
       dict(text="Create a WebDAV sibling tandem for storage of a dataset's "
                 "file content and revision history. A user will be prompted "
                 "for any required credentials, if they are not yet known.",
             code_py="create_sibling_webdav(url='https://webdav.example.com/myds')",
             code_cmd='datalad create-sibling-webdav "https://webdav.example.com/myds"'),
       dict(text="Such a dataset can be cloned by DataLad via a specially "
                 "crafted URL. Again, credentials are automatically "
                 "determined, or a user is prompted to enter them",
            code_py="clone('datalad-annex::?type=webdav&encryption=none&url=https://webdav.example.com/myds')",
            code_cmd='datalad clone "datalad-annex::?type=webdav&encryption=none&url=https://webdav.example.com/myds"'),
       dict(
           text="A sibling can also be created with a human-readable file "
                 "tree, suitable for data exchange with non-DataLad users, "
                 "but only able to host a single version of each file",
           code_py="create_sibling_webdav(url='https://example.com/browseable', mode='filetree')",
           code_cmd='datalad create-sibling-webdav --mode filetree "https://example.com/browseable"'),
       dict(text="Cloning such dataset siblings is possible via a convenience "
                 "URL",
            code_py="clone('webdavs://example.com/browseable')",
            code_cmd='datalad clone "webdavs://example.com/browseable"'),
       dict(text="In all cases, the storage sibling needs to explicitly "
                 "enabled prior to file content retrieval",
            code_py="siblings('enable', name='example.com-storage')",
            code_cmd='datalad siblings enable --name example.com-storage'),
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
            configured under the given name.
            In this case, sibling creation can be skipped ('skip') or the
            sibling (re-)configured ('reconfigure') in the dataset, or the
            command be instructed to fail ('error').""", ),
        recursive=recursion_flag,
        recursion_limit=recursion_limit,
        mode=Parameter(
            args=("--mode",),
            constraints=EnsureChoice(
                'annex', 'filetree', 'annex-only', 'filetree-only',
                'git-only'),
            doc="""Siblings can be created in various modes:
            full-featured sibling tandem, one for a dataset's Git history
            and one storage sibling to host any number of file versions
            ('annex').
            A single sibling for the Git history only ('git-only').
            A single annex sibling for multi-version file storage only
            ('annex-only').
            As an alternative to the standard (annex) storage sibling setup
            that is capable of storing any number of historical file versions
            using a content hash layout ('annex'|'annex-only'), the 'filetree'
            mode can used.
            This mode offers a human-readable data organization on the WebDAV
            remote that matches the file tree of a dataset (branch).
            However, it can, consequently, only store a single version of each
            file in the file tree.
            This mode is useful for depositing a single dataset
            snapshot for consumption without DataLad. The 'filetree' mode
            nevertheless allows for cloning such a single-version dataset,
            because the full dataset history can still be pushed to the WebDAV
            server.
            Git history hosting can also be turned off for this setup
            ('filetree-only').
            When both a storage sibling and a regular sibling are created
            together, a publication dependency on the storage sibling is
            configured for the regular sibling in the local dataset clone.
            """),
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
            mode: str = 'annex',
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
                f"Using 'http:' ({url!r}) means that WebDAV credentials might"
                " be sent unencrypted over network links. Consider using "
                "'https:'.")

        if not name:
            # not using .netloc to avoid ports to show up in the name
            name = parsed_url.hostname

        # ensure values of critical switches. this duplicated the CLI processing, but
        # compliance is critical in a python session too.
        # we cannot make it conditional to apimode == cmdline, because this command
        # might be called by other python code
        for param, value in (('mode', mode),
                             ('existing', existing)):
            try:
                CreateSiblingWebDAV._params_[param].constraints(value)
            except ValueError as e:
                # give message a context
                raise ValueError(f"{param!r}: {e}") from e

        if mode in ('annex-only', 'filetree-only') and storage_name:
            lgr.warning(
                "Sibling name will be used for storage sibling in "
                "storage-sibling-only mode, but a storage sibling name "
                "was provided"
            )
        if mode == 'git-only' and storage_name:
            lgr.warning(
                "Storage sibling setup disabled, but a storage sibling name "
                "was provided"
            )
        if mode != 'git-only' and not storage_name:
            storage_name = "{}-storage".format(name)

        if mode != 'git-only' and name == storage_name:
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
                    type='sibling',
                    name=sname,
                    ds=ds,
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
                mode=mode,
                name=name,
                storage_name=storage_name,
                existing=existing,
            )

        # Generate a sibling for dataset "ds", and for sub-datasets if recursive
        # is True.
        for res in ds.foreach_dataset(
                _dummy,
                return_type='generator',
                result_renderer='disabled',
                recursive=recursive,
                # recursive False is not enough to disable recursion
                # https://github.com/datalad/datalad/issues/6659
                recursion_limit=0 if not recursive else recursion_limit,
        ):
            # unwind result generator
            for partial_result in res.get('result', []):
                yield dict(res_kwargs, **partial_result)

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

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        from datalad.ui import ui
        from os.path import relpath
        import datalad.support.ansi_colors as ac

        if res['status'] != 'ok' or 'sibling_webdav' not in res['action'] or \
                res['type'] != 'sibling':
            # It's either 'notneeded' (not rendered), an `error`/`impossible` or
            # something unspecific to this command. No special rendering
            # needed.
            generic_result_renderer(res)
            return

        ui.message('{action}({status}): {path} [{name}{url}]'.format(
            action=ac.color_word(res['action'], ac.BOLD),
            path=relpath(res['path'], res['refds'])
            if 'refds' in res else res['path'],
            name=ac.color_word(res.get('name', ''), ac.MAGENTA),
            url=f": {res['url']}" if 'url' in res else '',
            status=ac.color_status(res['status']),
        ))


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
        kwargs = dict(
            # name could be none
            name=name,
            _prompt='User name and password are required for WebDAV access '
                    f'at {url}',
            type='user_password',
        )
        # check if we know the realm, if so include in the credential, if not
        # avoid asking for it interactively (it is a server-specified property
        # users would generally not know, if they do, they can use the
        # `credentials` command upfront.
        realm = credprops.get('realm')
        if realm:
            kwargs['realm'] = realm
        try:
            cred = credman.get(**kwargs)
        except Exception as e:
            lgr.debug('Credential retrieval failed: %s', e)

    return name, cred


def _create_sibling_webdav(
        ds, url, *,
        credential_name, credential,
        mode='git-only', name=None, storage_name=None, existing='error'):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    credential_name: str
    credential: tuple
    mode: str, optional
    name: str, optional
    storage_name: str, optional
    existing: str, optional
    """
    # simplify downstream logic, export yes or no
    export_storage = 'filetree' in mode

    existing_siblings = [
        r[1] for r in _yield_ds_w_matching_siblings(
            ds,
            (name, storage_name),
            recursive=False)
    ]

    if mode != 'git-only':
        yield from _create_storage_sibling(
            ds,
            url,
            storage_name,
            credential,
            export=export_storage,
            existing=existing,
            known=storage_name in existing_siblings,
        )

    if mode not in ('annex-only', 'filetree-only'):
        yield from _create_git_sibling(
            ds,
            url,
            name,
            credential_name,
            credential,
            export=export_storage,
            existing=existing,
            known=name in existing_siblings,
            publish_depends=storage_name if mode != 'git-only'
            else None
        )


def _get_skip_sibling_result(name, ds, type_):
    return get_status_dict(
        action='create_sibling_webdav{}'.format(
            '.storage' if type_ == 'storage' else ''),
        ds=ds,
        status='notneeded',
        message=("skipped creating %r sibling %r, already exists",
                 type_, name),
        name=name,
        type='sibling',
    )


def _create_git_sibling(
        ds, url, name, credential_name, credential, export, existing,
        known, publish_depends=None):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    name: str
    credential_name: str
    credential: tuple
    export: bool
    existing: {skip, error, reconfigure}
    known: bool
        Flag whether the sibling is a known remote (no implied
        necessary existance of content on the remote).
    publish_depends: str or None
        publication dependency to set
    """
    if known and existing == 'skip':
        yield _get_skip_sibling_result(name, ds, 'git')
        return

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

    # announce the sibling to not have an annex (we have a dedicated
    # storage sibling for that) to avoid needless annex-related processing
    # and speculative whining by `siblings()`
    ds.config.set(f'remote.{name}.annex-ignore', 'true', scope='local')

    for r in ds.siblings(
            # action must always be 'configure' (not 'add'), because above we just
            # made a remote {name} known, which is detected by `sibling()`. Any
            # conflict detection must have taken place separately before this call
            # https://github.com/datalad/datalad/issues/6649
            action="configure",
            name=name,
            url=remote_url,
            # this is presently the default, but it may change
            fetch=False,
            publish_depends=publish_depends,
            return_type='generator',
            result_renderer='disabled'):
        if r.get('action') == 'configure-sibling':
            r['action'] = 'reconfigure_sibling_webdav' \
                if known and existing == 'reconfigure' \
                else 'create_sibling_webdav'
        yield r


def _create_storage_sibling(
        ds, url, name, credential, export, existing, known=False):
    """
    Parameters
    ----------
    ds: Dataset
    url: str
    name: str
    credential: tuple
    export: bool
    existing: {skip, error, reconfigure}
        (Presently unused)
    known: bool
        Flag whether the sibling is a known remote (no implied
        necessary existance of content on the remote).
    """
    if known and existing == 'skip':
        yield _get_skip_sibling_result(name, ds, 'storage')
        return

    cmd_args = [
        'enableremote' if known and existing == 'reconfigure'
        else 'initremote',
        name,
        "type=webdav",
        f"url={url}",
        f"exporttree={'yes' if export else 'no'}",
        "encryption=none",
        # for now, no autoenable. It would result in unconditional
        # error messages on clone
        #https://github.com/datalad/datalad/issues/6634
        #"autoenable=true"
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
        action='reconfigure_sibling_webdav.storage'
               if known and existing == 'reconfigure' else
        'create_sibling_webdav.storage',
        name=name,
        type='sibling',
        url=url,
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

    def _discover_all_remotes(ds, refds, **kwargs):
        """Helper to be run on all relevant datasets via foreach
        """
        # Note, that `siblings` doesn't tell us about not enabled special
        # remotes. There could still be conflicting names we need to know
        # about in order to properly deal with the `existing` switch.

        repo = ds.repo
        # list of known git remotes
        if isinstance(repo, AnnexRepo):
            remotes = repo.get_remotes(exclude_special_remotes=True)
            remotes.extend([v['name']
                            for k, v in repo.get_special_remotes().items()]
                           )
        else:
            remotes = repo.get_remotes()
        return remotes

    if not recursive:
        for name in _discover_all_remotes(ds, ds):
            if name in names:
                yield ds.path, name
        return

    # in recursive mode this check could take a substantial amount of
    # time: employ a progress bar (or rather a counter, because we don't
    # know the total in advance
    pbar_id = 'check-siblings-{}'.format(id(ds))
    log_progress(
        lgr.info, pbar_id,
        'Start checking pre-existing sibling configuration %s', ds,
        label='Query siblings',
        unit=' Siblings',
    )

    for res in ds.foreach_dataset(
            _discover_all_remotes,
            recursive=recursive,
            recursion_limit=recursion_limit,
            return_type='generator',
            result_renderer='disabled',
    ):
        # unwind result generator
        if 'result' in res:
            for name in res['result']:
                log_progress(
                    lgr.info, pbar_id,
                    'Discovered sibling %s in dataset at %s',
                    name, res['path'],
                    update=1,
                    increment=True)
                if name in names:
                    yield res['path'], name

    log_progress(
        lgr.info, pbar_id,
        'Finished checking pre-existing sibling configuration %s', ds,
    )
