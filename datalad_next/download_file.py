"""
"""

__docformat__ = 'restructuredtext'

from logging import getLogger
from pathlib import (
    Path,
    PurePosixPath,
)
from typing import Dict
from urllib.parse import urlparse

import datalad
from datalad.distribution.dataset import (
    datasetmethod,
    resolve_path,
)
from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import eval_results
from datalad.support.exceptions import (
    CapturedException,
    DownloadError,
)
from datalad.support.param import Parameter
from datalad.utils import ensure_list
from datalad_next.constraints import (
    EnsureChoice,
    EnsureGeneratorFromFileLike,
    EnsureJSON,
    EnsureListOf,
    EnsureMapping,
    EnsurePath,
    EnsureURL,
    EnsureParsedURL,
    EnsureValue,
)
from datalad_next.constraints.base import AltConstraints
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.http_support import HttpOperations

lgr = getLogger('datalad.local.download_file')


@build_doc
class DownloadFile(Interface):
    """Download file"""
    #
    # argument format specifications
    #
    # any URL that we would take must have a scheme, because we switch
    # protocol handling based on that. It is also crucial for distinguishing
    # stuff like local paths and file names from URLs
    url_constraint = EnsureURL(required=['scheme'])
    # other than a plain URL we take a mapping from a URL to a local path.
    # The special value '-' is used to indicate stdout
    # if given as a single string, we support tab-delimited items: URL\tpath
    url2path_constraint = EnsureMapping(
        key=url_constraint, value=EnsureValue('-') | EnsurePath(),
        delimiter='\t',
        # we disallow length-2 sequences to be able to distinguish from
        # a length-2 list of URLs.
        # the key issue is the flexibility of EnsurePath -- pretty much
        # anything could be a valid unix path
        allow_length2_sequence=False,
    )
    # each specification items is either a mapping url->path, just a url, or a
    # JSON-encoded url->path mapping.  the order is complex-to-simple for the
    # first two (to be able to distinguish a mapping from an encoded URL. The
    # JSON-encoding is tried last, it want match accidentally)
    spec_item_constraint = url2path_constraint | url_constraint \
        | (EnsureJSON() & url2path_constraint)

    # we support reading specification items (matching any format defined
    # above) as
    # - a single item
    # - as a list of items
    # - a list given in a file, or via stdin (or any file-like in Python)
    #
    # Must not OR: https://github.com/datalad/datalad/issues/7164
    #spec=spec_item_constraint | EnsureListOf(spec_item_constraint)# \
    spec_constraint = AltConstraints(
        spec_item_constraint,
        EnsureListOf(spec_item_constraint),
        EnsureGeneratorFromFileLike(spec_item_constraint),
    )

    force_choices = EnsureChoice('overwrite-existing')

    # Interface.validate_args() will inspect this dict for the presence of a
    # validator for particular parameters
    _validators_ = dict(
        spec=spec_constraint,
        # if given, it must also exist as a source for configuration items
        # and/or credentials
        dataset=EnsureDataset(installed=True),
        force=force_choices | EnsureListOf(force_choices),
        # TODO EnsureCredential
        #credential=
        # TODO EnsureHashAlgorithm
        #hash=EnsureHashAlgorithm | EnsureIterableOf(EnsureHashAlgorithm)
    )

    # this is largely here for documentation and CLI parser building
    _params_ = dict(
        spec=Parameter(
            args=('spec',),
            doc="""""",
            nargs='+'),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc=""""""),
        force=Parameter(
            args=("--force",),
            action='append',
            doc=""""""),
        credential=Parameter(
            args=("--credential",),
            metavar='NAME',
            doc="""name of a credential to be used for authorization. If no
            credential is identified, the last-used credential for the
            authentication realm associated with the download target will
            be used. If there is no credential available yet, it will be
            prompted for. Once used successfully, a prompt for entering
            to save such a new credential will be presented.""",
        ),
        hash=Parameter(
            args=("--hash",),
            action='append',
            doc="""Name of a hashing algorithm supported by the Python
            'hashlib' module."""),
    )

    _examples_ = [
    ]

    @classmethod
    def validate_args(cls: Interface, kwargs: Dict, at_default: set) -> Dict:
        validated = {}
        for argname, arg in kwargs.items():
            if argname in at_default:
                # do not validate any parameter where the value matches the
                # default declared in the signature. Often these are just
                # 'do-nothing' settings or have special meaning that need
                # not be communicated to a user. Not validating them has
                # two consequences:
                # - the condition can simply be referred to as "default
                #   behavior" regardless of complexity
                # - a command implementation must always be able to handle
                #   its own defaults directly, and cannot delegate a
                #   default value handling to a constraint
                #
                # we must nevertheless pass any such default value through
                # to make/keep them accessible to the general result handling
                # code
                validated[argname] = arg
                continue
            validator = cls._validators_.get(argname, lambda x: x)
            # TODO option to validate all args despite failure
            try:
                validated[argname] = validator(arg)
            except Exception as e:
                raise ValueError(
                    f'Validation of parameter {argname!r} failed') from e
        return validated

    @staticmethod
    @datasetmethod(name="download_file")
    @eval_results
    def __call__(spec, *, dataset=None, force=None, credential=None,
                 hash=None):
        # which config to inspect for credentials etc
        cfg = dataset.ds.config if dataset else datalad.cfg

        http_handler = HttpOperations(cfg)
        _urlscheme_handlers = dict(
            http=http_handler,
            https=http_handler,
        )

        if isinstance(spec, (str, dict)):
            # input validation allows for a non-list item, turn into
            # list for uniform processing
            spec = [spec]

        # we are not running any tests upfront on the whole spec,
        # because the spec can be a generator and consume from a
        # long-running source (e.g. via stdin)
        for item in spec:
            try:
                url, dest = _get_url_dest_path(item)
            except Exception as e:
                yield get_status_dict(
                    action='download_file',
                    status='impossible',
                    spec=item,
                    exception=CapturedException(e),
                )
                continue

            # we know that any URL has a scheme
            scheme = url.split('://')[0]
            if scheme not in ('http', 'https',):
                yield get_status_dict(
                    action='download_file',
                    status='error',
                    message='unsupported URL scheme',
                    url=url,
                )
                continue

            # turn any path into an absolute path, considering a potential
            # dataset context
            try:
                dest = _prep_dest_path(dest, dataset, force)
            except ValueError as e:
                yield get_status_dict(
                    action='download_file',
                    status='error',
                    exception=CapturedException(e),
                    url=url,
                    path=dest,
                )
                continue

            try:
                download_props = _urlscheme_handlers[scheme].download(
                    url,
                    dest,
                    credential=credential,
                    hash=ensure_list(hash),
                )
                res = get_status_dict(
                    action='download_file',
                    status='ok',
                    url=url,
                    path=dest,
                )
                # take the reported download properties (e.g. any computed
                # hashes a a starting point, and overwrite any potentially
                # conflicting keys with the standard ones)
                res = dict(
                    download_props,
                    **res)
                yield res
            except Exception as e:
                ce = CapturedException(e)
                res = get_status_dict(
                    action='download_file',
                    status='error',
                    message='download failure',
                    url=url,
                    path=dest,
                    exception=ce,
                )
                if issubclass(type(e), DownloadError):
                    res['status_code'] = e.status
                yield res


def _prep_dest_path(dest, dataset, force):
    if dest == '-':
        # nothing to prep for stdout
        return
    dest = resolve_path(
        dest,
        ds=dataset.original if dataset else None,
        ds_resolved=dataset.ds if dataset else None,
    )
    # make sure we can replace any existing target path later
    # on. but do not remove here, we might not actually be
    # able to download for other reasons
    if _lexists(dest) and (
            not force or 'overwrite-existing' not in force):
        raise ValueError('target path already exists')

    # create parent directory if needed
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def _get_url_dest_path(spec_item):
    # we either have a string (single URL), or a mapping
    if isinstance(spec_item, dict):
        return spec_item.popitem()
    else:
        # derive a destination path from the URL.
        # we take the last element of the 'path' component
        # of the URL, or fail
        parsed = urlparse(spec_item)
        filename = PurePosixPath(parsed.path).name
        if not filename:
            raise ValueError('cannot derive file name from URL')
        return spec_item, filename


def _lexists(path: Path):
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
