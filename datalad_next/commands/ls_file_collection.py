"""
"""

from __future__ import annotations

__docformat__ = 'restructuredtext'

from dataclasses import (
    asdict,
    dataclass,
)
from datetime import datetime
from humanize import (
    naturalsize,
    naturaldate,
    naturaltime,
)
from logging import getLogger
from pathlib import Path
from stat import filemode
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
)

from datalad_next.commands import (
    EnsureCommandParameterization,
    ValidatedInterface,
    Parameter,
    ParameterConstraintContext,
    build_doc,
    eval_results,
    get_status_dict,
)
from datalad_next.constraints import (
    EnsureChoice,
    EnsurePath,
    EnsureURL,
)
from datalad_next.uis import (
    ansi_colors as ac,
    ui_switcher as ui,
)
from datalad_next.utils import ensure_list

from datalad_next.iter_collections.directory import iterdir
from datalad_next.iter_collections.tarfile import itertar
from datalad_next.iter_collections.utils import FileSystemItemType


lgr = getLogger('datalad.local.ls_file_collection')


# hand-maintain a list of collection type names that should be
# advertised and supported. it makes little sense to auto-discover
# them, because each collection type likely needs some custom glue
# code, and some iterators may not even be about *file* collections
_supported_collection_types = (
    'directory',
    'tarfile',
)


@dataclass  # sadly PY3.10+ only (kw_only=True)
class CollectionSpec:
    """Internal type for passing a collection specification to
    ``ls_file_collection``. it is created by the command validator
    transparently.
    """
    orig_id: Any
    iter: Iterator
    item2res: Callable


class LsFileCollectionParamValidator(EnsureCommandParameterization):
    """Parameter validator for the ``ls_file_collection`` command"""
    _collection_types = EnsureChoice(*_supported_collection_types)

    def __init__(self):
        super().__init__(
            param_constraints=dict(
                type=self._collection_types,
                collection=EnsurePath(lexists=True) | EnsureURL(),
                # TODO EnsureHashAlgorithm
                # https://github.com/datalad/datalad-next/issues/346
                #hash=None,
            ),
            joint_constraints={
                ParameterConstraintContext(('type', 'collection', 'hash'),
                                           'collection iterator'):
                self.get_collection_iter,
            },
        )

    def get_collection_iter(self, **kwargs):
        type = kwargs['type']
        collection = kwargs['collection']
        hash = ensure_list(kwargs['hash'])
        iter_fx = None
        iter_kwargs = None
        if type in ('directory', 'tarfile'):
            if not isinstance(collection, Path):
                self.raise_for(
                    kwargs,
                    "{type} collection requires a Path-type identifier",
                    type=type,
                )
            iter_kwargs = dict(path=collection, hash=hash)
            item2res = fsitem_to_dict
        if type == 'directory':
            iter_fx = iterdir
        elif type == 'tarfile':
            iter_fx = itertar
        else:
            raise RuntimeError('unhandled condition')
        assert iter_fx is not None
        return dict(
            collection=CollectionSpec(
                orig_id=collection,
                iter=iter_fx(**iter_kwargs),
                item2res=item2res),
        )


def fsitem_to_dict(item) -> Dict:
    keymap = {'name': 'item'}
    # FileSystemItemType is too fine-grained to be used as result type
    # directly, map some cases!
    fsitem_type_to_res_type = {
        'specialfile': 'file',
    }

    # TODO likely could be faster by moving the conditional out of the
    # dict-comprehension and handling them separately upfront/after
    d = {
        keymap.get(k, k):
        # explicit str value access, until we can use `StrEnum`
        v if k != 'type' else fsitem_type_to_res_type.get(v.value, v.value)
        for k, v in asdict(item).items()
        # strip pointless symlink target reports for anything but symlinks
        if item.type is FileSystemItemType.symlink or k != 'link_target'
    }
    hashes = d.pop('hash', None)
    if hashes is not None:
        for k, v in hashes.items():
            d[f'hash-{k}'] = v
    return d


@build_doc
class LsFileCollection(ValidatedInterface):
    """Report information on files in a collection

    This is a utility that can be used to query information on files in
    different file collections. The type of information reported varies across
    collection types. However, each result at minimum contains some kind of
    identifier for the collection ('collection' property), and an identifier
    for the respective collection item ('item' property). Each result
    also contains a ``type`` property that indicates particular type of file
    that is being reported on. In most cases this will be ``file``, but
    other categories like ``symlink`` or ``directory`` are recognized too.

    Regardless of the collection type this command can compute one or more
    hashes (checksums) for any file in a collection. If the collection
    itself does not readily provide a particular hash, file content needs
    to be read, and possibly retrieved first.

    Supported file collection types are:

    ``directory``
      Reports on the content of a given directory (non-recursively). The
      collection identifier is the path of the directory. Item identifiers
      are the name of a file within that directory. Standard properties like
      ``size``, ``mtime``, or ``link_target`` are included in the report.

    ``tarfile``
      Reports on members of a TAR archive. The collection identifier is the
      path of the TAR file. Item identifiers are the relative paths
      of archive members within the archive. Reported properties are similar
      to the ``directory`` collection type.
    """
    _validator_ = LsFileCollectionParamValidator()

    # this is largely here for documentation and CLI parser building
    _params_ = dict(
        type=Parameter(
            args=("type",),
            choices=_supported_collection_types,
            doc="""Name of the type of file collection to report on"""),
        collection=Parameter(
            args=('collection',),
            metavar='ID/LOCATION',
            doc="""identifier or location of the file collection to report on.
            Depending on the type of collection to process, the specific
            nature of this parameter can be different. A common identifier
            for a file collection is a path (to a directory, to an archive),
            but might also be a URL. See the documentation for details on
            supported collection types."""),
        hash=Parameter(
            args=("--hash",),
            action='append',
            metavar='ALGORITHM',
            doc="""One or more names of algorithms to be used for reporting
            file hashes. They must be supported by the Python 'hashlib' module,
            e.g. 'md5' or 'sha256'. Reporting file hashes typically
            implies retrieving/reading file content. This processing
            may also enable reporting of additional properties that
            may otherwise not be readily available.
            [CMD: This option can be given more than once CMD]
            """),
    )

    _examples_: List = [
        {'text': 'Report on the content of a directory',
         'code_cmd': 'datalad -f json ls-file-collection directory /tmp',
         'code_py': 'records = ls_file_collection("directory", "/tmp")'},
        {'text': 'Report on the content of a TAR archive with '
                 'MD5 and SHA1 file hashes',
         'code_cmd': 'datalad -f json ls-file-collection'
                     ' --hash md5 --hash sha1 tarfile myarchive.tar.gz',
         'code_py': 'records = ls_file_collection("tarfile",'
                    ' "myarchive.tar.gz", hash=["md5", "sha1"])'},
        {'text': "Register URLs for files in a directory that is"
                 " also reachable via HTTP. This uses ``ls-file-collection``"
                 " for listing files and computing MD5 hashes,"
                 " then using ``jq`` to filter and transform the output"
                 " (just file records, and in a JSON array),"
                 " and passes them to ``addurls``, which generates"
                 " annex keys/files and assigns URLs."
                 " When the command finishes, the dataset contains no"
                 " data, but can retrieve the files after confirming"
                 " their availability (i.e., via `git annex fsck`)",
         'code_cmd':
         'datalad -f json ls-file-collection directory wwwdir --hash md5 \\\n'
         ' | jq \'. | select(.type == "file")\' \\\n'
         ' | jq --slurp . \\\n'
         " | datalad addurls --key 'et:MD5-s{size}--{hash-md5}' - 'https://example.com/{item}'"},
    ]

    @staticmethod
    @eval_results
    def __call__(
            type: str,
            collection: CollectionSpec,
            *,
            hash: str | List[str] | None = None,
    ):
        for item in collection.iter:
            res = collection.item2res(item)
            res.update(get_status_dict(
                action='ls_file_collection',
                status='ok',
                collection=collection.orig_id,
            ))
            yield res

    @staticmethod
    def custom_result_renderer(res, **kwargs):
        # given the to-be-expected diversity, this renderer only
        # outputs identifiers and type info. In almost any real use case
        # either no rendering or JSON rendering will be needed

        type = res.get('type', None)

        # if there is no mode, produces '?---------'
        mode = filemode(res.get('mode', 0))

        size = None
        if type in ('file', 'hardlink'):
            size = res.get('size', None)
        size = '-' if size is None else naturalsize(size, gnu=True)

        mtime = res.get('mtime', None)
        if mtime is None:
            mtime = ''
        else:
            dt = datetime.fromtimestamp(mtime)
            hts = naturaldate(dt)
            if hts == 'today':
                hts = naturaltime(dt)
                hts = hts.replace(
                    'minutes ago', 'min ago').replace(
                    'seconds ago', 'sec ago')

        ui.message('{mode} {size: >6} {hts: >11} {item} ({type})'.format(
            mode=mode,
            size=size,
            hts=hts,
            item=ac.color_word(
                res.get('item', '<missing-item-identifier>'),
                ac.BOLD),
            type=ac.color_word(
                res.get('type', '<missing-type>'),
                ac.MAGENTA),
        ))
