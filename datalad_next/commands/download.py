"""
"""

__docformat__ = 'restructuredtext'

from logging import getLogger
from pathlib import (
    Path,
    PurePosixPath,
)
from urllib.parse import urlparse

import datalad
from datalad_next.commands import (
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    build_doc,
    datasetmethod,
    eval_results,
    get_status_dict,
)
from datalad_next.exceptions import (
    CapturedException,
    UrlOperationsRemoteError,
)
from datalad_next.utils import ensure_list
from datalad_next.constraints import (
    AnyOf,
    EnsureChoice,
    EnsureGeneratorFromFileLike,
    EnsureJSON,
    EnsureListOf,
    EnsureMapping,
    EnsurePath,
    EnsureURL,
    EnsureValue,
    WithDescription,
)
from datalad_next.constraints.dataset import EnsureDataset
from datalad_next.url_operations.any import AnyUrlOperations

lgr = getLogger('datalad.local.download')


class EnsureURLFilenamePairFromURL(EnsureURL):
    """Accept a URL and derive filename from it path component

    Return original URL and filename as a mapping
    """
    def __init__(self):
        super().__init__(required=['scheme', 'path'])

    def __call__(self, value):
        url = super().__call__(value)
        # derive a filename from the URL.
        # we take the last element of the 'path' component
        # of the URL, or fail
        parsed = urlparse(url)
        filename = PurePosixPath(parsed.path).name
        return {url: filename}


@build_doc
class Download(ValidatedInterface):
    """Download from URLs

    This command is the front-end to an extensible framework for performing
    downloads from a variety of URL schemes. Built-in support for the schemes
    'http', 'https', 'file', and 'ssh' is provided. Extension packages
    may add additional support.

    In contrast to other downloader tools, this command integrates with the
    DataLad credential management and is able to auto-discover credentials.
    If no credential is available, it automatically prompts for them, and
    offers to store them for re-use after a successful authentication.

    Simultaneous hashing (checksumming) of downloaded content is supported
    with user-specified algorithms.

    The command can process any number of downloads (serially). it can read
    download specifications from (command line) arguments, files, or STDIN.
    It can deposit downloads to individual files, or stream to STDOUT.

    Implementation and extensibility

    Each URL scheme is processed by a dedicated handler. Additional
    schemes can be supported by sub-classing
    :class:`datalad_next.url_operations.UrlOperations` and implementing
    the `download()` method. Extension packages can register new handlers,
    by patching them into the `datalad_next.download._urlscheme_handlers`
    registry dict.
    """
    #
    # argument format specifications
    #
    # any URL that we would take must have a scheme, because we switch
    # protocol handling based on that. It is also crucial for distinguishing
    # stuff like local paths and file names from URLs
    url_constraint = EnsureURL(required=['scheme'])
    # other than a plain URL we take a mapping from a URL to a local path.
    # The special value '-' is used to indicate stdout
    # if given as a single string, we support single-space-delimited items:
    # "<url> <path>"
    url2path_constraint = WithDescription(
        EnsureMapping(
            key=url_constraint,
            value=EnsureValue('-') | EnsurePath(),
            delimiter=' ',
            # we disallow length-2 sequences to be able to distinguish from
            # a length-2 list of URLs.
            # the key issue is the flexibility of EnsurePath -- pretty much
            # anything could be a valid unix path
            allow_length2_sequence=False,
        ),
        error_message=f'not a dict, length-2-iterable, or space-delimited str',
    )
    # each specification items is either a mapping url->path, just a url, or a
    # JSON-encoded url->path mapping.  the order is complex-to-simple for the
    # first two (to be able to distinguish a mapping from an encoded URL. The
    # JSON-encoding is tried last, it want match accidentally)
    urlonly_item_constraint = WithDescription(
        EnsureURLFilenamePairFromURL() & url2path_constraint,
        error_message='not a URL with a path component '
        'from which a filename can be derived',
    )
    json_item_constraint = WithDescription(
        EnsureJSON() & url2path_constraint,
        error_message='not a JSON-encoded str with an object or length-2-array',
    )
    any_item_constraint = WithDescription(
        AnyOf(
            # TODO explain
            url2path_constraint,
            urlonly_item_constraint,
            json_item_constraint,
        ),
        error_message='not a single item\n{__itemized_causes__}',
    )

    # we support reading specification items (matching any format defined
    # above) as
    # - a single item
    # - as a list of items
    # - a list given in a file, or via stdin (or any file-like in Python)
    spec_constraint = WithDescription(
        AnyOf(
            any_item_constraint,
            WithDescription(
                EnsureListOf(any_item_constraint),
                error_message='not a list of any such item',
            ),
            WithDescription(
                EnsureGeneratorFromFileLike(
                    any_item_constraint,
                    exc_mode='yield',
                ),
                error_message="not a path to a file with one such item per-line, "
                "nor '-' to read any such item from STDIN",
            ),
        ),
        error_message="does not provide URL->(PATH|-) mapping(s)\n{__itemized_causes__}"
    )

    force_choices = EnsureChoice('overwrite-existing')

    # Interface.validate_args() will inspect this dict for the presence of a
    # validator for particular parameters
    _validator_ = EnsureCommandParameterization(dict(
        spec=spec_constraint,
        # if given, it must also exist as a source for configuration items
        # and/or credentials
        dataset=EnsureDataset(installed=True),
        force=force_choices | EnsureListOf(force_choices),
        # TODO EnsureCredential
        #credential=
        # TODO EnsureHashAlgorithm
        #hash=EnsureHashAlgorithm | EnsureIterableOf(EnsureHashAlgorithm)
    ))

    # this is largely here for documentation and CLI parser building
    _params_ = dict(
        spec=Parameter(
            args=('spec',),
            metavar='<path>|<url>|<url-path-pair>',
            doc="""Download sources and targets can be given in a variety of
            formats: as a URL, or as a URL-path-pair that is mapping a source
            URL to a dedicated download target path. Any number of URLs or
            URL-path-pairs can be provided, either as an argument list, or
            read from a file (one item per line). Such a specification input
            file can be given as a path to an existing file (as a single
            value, not as part of a URL-path-pair). When the special path
            identifier '-' is used, the download is written to STDOUT.
            A specification can also be read in JSON-lines encoding (each line
            being a string with a URL or an object mapping a URL-string to a
            path-string).  [PY: In addition, specifications can also be given
            as a list or URLs, or as a list of dicts with a URL to
            path mapping. Paths are supported in string form, or as `Path`
            objects. PY]""",
            nargs='+'),
        dataset=Parameter(
            args=("-d", "--dataset"),
            doc="""Dataset to be used as a configuration source. Beyond
            reading configuration items, this command does not interact with
            the dataset."""),
        force=Parameter(
            args=("--force",),
            action='append',
            # TODO only here because datalad-core CLI generates docs from this
            choices=force_choices._allowed,
            doc="""By default, a target path for a download must not exist yet.
            'force-overwrite' disabled this check."""),
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
            metavar='ALGORITHM',
            doc="""Name of a hashing algorithm supported by the Python
            'hashlib' module, e.g. 'md5' or 'sha256'.
            [CMD: This option can be given more than once CMD]
            """),
    )

    _examples_ = [
        {'text': 'Download webpage to "myfile.txt"',
         'code_cmd': 'datalad download "http://example.com myfile.txt"',
         'code_py': 'download({"http://example.com": "myfile.txt"})'},
        {'text': 'Read download specification from STDIN (e.g. JSON-lines)',
         'code_cmd': 'datalad download -',
         'code_py': 'download("-")'},
        {'text':
         'Simultaneously hash download, hexdigest reported in result record',
         'code_cmd':
         'datalad download --hash sha256 http://example.com/data.xml"',
         'code_py':
         'download("http://example.com/data.xml", hash=["sha256"])'},
        {'text': 'Download from SSH server',
         'code_cmd': 'datalad download "ssh://example.com/home/user/data.xml"',
         'code_py': 'download("ssh://example.com/home/user/data.xml")'},
        {'text': 'Stream a download to STDOUT',
         'code_cmd': 'datalad -f disabled download "http://example.com -"'},
    ]

    @staticmethod
    @datasetmethod(name="download")
    @eval_results
    def __call__(spec, *, dataset=None, force=None, credential=None,
                 hash=None):
        # which config to inspect for credentials etc
        cfg = dataset.ds.config if dataset else datalad.cfg

        if isinstance(spec, (str, dict)):
            # input validation allows for a non-list item, turn into
            # list for uniform processing
            spec = [spec]

        # cache of already used handlers
        url_handler = AnyUrlOperations(cfg=cfg)

        # we are not running any tests upfront on the whole spec,
        # because the spec can be a generator and consume from a
        # long-running source (e.g. via stdin)
        for item in spec:
            if isinstance(item, CapturedException):
                # the generator encountered an exception for a particular
                # item and is relaying it as per instructions
                # exc_mode='yield'. We report and move on. Outside
                # flow logic will decide if processing continues
                yield get_status_dict(
                    action='download',
                    status='impossible',
                    exception=item,
                )
                continue

            url, dest = item.popitem()
            # we know that any URL has a scheme
            if not url_handler.is_supported_url(url):
                yield get_status_dict(
                    action='download',
                    status='error',
                    message='unsupported URL '
                    '(custom URL handlers can be declared via DataLad '
                    'configuration)',
                    url=url,
                )
                continue

            # ready destination path
            try:
                dest = _prep_dest_path(dest, force)
            except ValueError as e:
                yield get_status_dict(
                    action='download',
                    status='error',
                    exception=CapturedException(e),
                    url=url,
                    path=dest,
                )
                continue

            try:
                download_props = url_handler.download(
                    url,
                    dest,
                    credential=credential,
                    hash=ensure_list(hash),
                )
                res = get_status_dict(
                    action='download',
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
                    action='download',
                    status='error',
                    message='download failure',
                    url=url,
                    path=dest,
                    exception=ce,
                )
                if issubclass(type(e), UrlOperationsRemoteError):
                    res['status_code'] = e.status_code
                yield res


def _prep_dest_path(dest, force):
    if dest == '-':
        # nothing to prep for stdout
        return
    # make sure we can replace any existing target path later
    # on. but do not remove here, we might not actually be
    # able to download for other reasons
    if _lexists(dest) and (
            not force or 'overwrite-existing' not in force):
        raise ValueError('target path already exists')

    # create parent directory if needed
    dest.parent.mkdir(parents=True, exist_ok=True)
    return dest


def _lexists(path: Path):
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
