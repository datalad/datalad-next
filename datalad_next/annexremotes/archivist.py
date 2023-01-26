"""
*archivist* git-annex external special remote
=============================================

Replacement for the `datalad-archive` URL
"""
from __future__ import annotations

from pathlib import (
    Path,
    PurePosixPath,
)
import re
from typing import (
    Dict,
)

# we intentionally limit ourselves to the most basic interface
# and even that we only need to get a `ConfigManager` instance.
# If that class would support a plain path argument, we could
# avoid it entirely
from datalad_next.datasets import LegacyAnnexRepo

from datalad_next.exceptions import (
    CapturedException,
    CommandError,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)
from datalad_next.url_operations.any import AnyUrlOperations
from datalad_next.utils import ensure_list

from . import (
    RemoteError,
    SpecialRemote,
    UnsupportedRequest,
    super_main
)


# TODO reuse as many pieces of uncurl as possible
class ArchivistRemote(SpecialRemote):
    """ """
    # be relatively permissive
    recognized_urls = re.compile('^dl\+archive:(?P<key>.*)#(?P<props>.*)')
    # each archive member is identified by a (relative) path inside
    # the archive.
    archive_member_props = re.compile(
        # a path may contain any char but '&'
        # TODO check that something in the machinery ensures proper
        # quoting
        'path=(?P<path>[^&]+)'
        # size info (in bytes) is optional
        '(&size=(?P<size>[0-9]+)|)'
        # archive type label is optional
        '(&atype=(?P<atype>[a-z0-9]+)|)'
    )
    # BACKEND[-sNNNN][-mNNNN][-SNNNN-CNNNN]--NAME
    key_props = re.compile(
        '(?P<backend>[A-Z0-9]+)'
        '(|-s(?P<size>[0-9]+))'
        '(|-m(?P<mtime>[0-9]+))'
        '(|-S(?P<chunksize>[0-9]+)-C(?P<chunknumber>[0-9]+))'
        '--(?P<name>.*)$'
    )

    def __init__(self, annex):
        super().__init__(annex)
        # will be tried in prepare()
        self._fsspec_handler = None
        self.repo = None
        self._akeys = None

    def initremote(self):
        # at present there is nothing that needs to be done on init/enable.
        # the remote is designed to work without any specific setup too
        pass

    def prepare(self):
        self.repo = LegacyAnnexRepo(self.annex.getgitdir())
        # central archive key cache
        self._akeys = ArchiveKeys(
            self.annex,
            self.repo,
        )

        # try to work without fsspec
        try:
            from datalad_next.url_operations.fsspec import FsspecUrlOperations
            # TODO support passing constructur arguments from configuration
            self._fsspec_handler = FsspecUrlOperations()
        except ImportError:
            self.message('FSSPEC support disabled, dependency not available',
                         type='debug')

    def claimurl(self, url):
        """Needs to check if want to handle a given URL

        Parameters
        ----------
        url : str

        Returns
        -------
        bool
            True if this is a ``dl+archive:`` URL, else False.
        """
        if ArchivistRemote.recognized_urls.match(url):
            return True
        else:
            return False

    def checkurl(self, url):
        """
        When running `git-annex addurl`, this is called after CLAIMURL
        indicated that we could handle a URL. It can return information
        on the URL target (e.g., size of the download, a target filename,
        or a sequence thereof with additional URLs pointing to individual
        components that would jointly make up the full download from the
        given URL. However, all of that is optional, and a simple `True`
        returned is sufficient to make git-annex call `TRANSFER RETRIEVE`.
        """
        try:
            akey, member_props = self._akeys.from_url(url)
        except ValueError as e:
            self.message(f'Invalid URL {url!r}: {e}', type='debug')
            return False

        # TODO possible make additonal checks of the particular properties
        # reported

        # we should be able to work with this.
        # do not actually test whether the archive is around or whether
        # the path actually points to a member in the archive,
        # leave to transfer_retrieve
        # Do not give detailed info to git-annex for now
        # https://github.com/Lykos153/AnnexRemote/issues/60
        #if member_props.get('size'):
        #    return dict(
        #        filename=member_props['path'].name,
        #        size=member_props['size'],
        #    )
        #else:
        #    return dict(filename=member_props['path'].name)
        return True

    def transfer_retrieve(self, key, localfilename):
        # this is all URL-based. Let's see what we have on record
        urls = self._get_key_dlarchive_urls(key)
        if not urls:
            raise RemoteError(f'No dl+archive: URLs on record for key {key!r}')

        if self._try_multiple_urls(
                urls,
                f"Try retrieving {key!r} from {{url}}",
                self._get_from_url,
                dst_path=Path(localfilename),
        ):
            return

        raise RemoteError(f'Could not obtain {key!r} from any URL')

    def checkpresent(self, key):
        # must check all URLs until the first hit
        urls = self._get_key_dlarchive_urls(key)
        if not urls:
            # no info, definitely not available
            return False

        try:
            return self._try_multiple_urls(
                urls,
                f"Checking {key!r} presence at {{url}}",
                self._check_at_url,
            )
        except Exception as e:
            raise RemoteError(f'Can verify presence of {key!r}: {e}')

    def transfer_store(self, key, filename):
        raise UnsupportedRequest('This special remote cannot store content')

    def remove(self, key):
        raise UnsupportedRequest('This special remote cannot remove content')

    #
    # Helpers
    #
    def _handle_request_w_fsspec(self, akey):
        # check first global, non-key-specific criteria
        if not self._fsspec_handler:
            # could not, even if desired
            return False

        ainfo = self._akeys[akey]
        if 'use_fsspec' not in ainfo:
            # assume yes and try to find counter-evidence
            use_fsspec = True
            if self._akeys.get_archive_type(akey) not in ('zip', 'tar'):
                # TODO if it turns out that we have to download
                # a currently absent, we would/should repeat the type
                # detection, because we would not have to (solely)
                # rely on an annotation, but could actually determine
                # the archive type
                use_fsspec = False
            ainfo['use_fsspec'] = use_fsspec
            self._akeys[akey] = ainfo
        return ainfo['use_fsspec']

    def _try_multiple_urls(self, urls, msg_tmpl, worker, **kwargs):
        for url in urls:
            self.message(msg_tmpl.format(url=url), type='debug')
            try:
                if worker(url, **kwargs):
                    # success
                    return True
            except ValueError as e:
                self.message(f"Invalid URL {url}: {e}", type='info')
        return False

    def _get_key_dlarchive_urls(self, key):
        return self.annex.geturls(key, prefix='dl+archive:')

    def _check_at_url(self, url):
        # the idea here is that: as long as the archive declared to contain
        # the key is still accessible, we declare CHECKPRESENT.
        # In other words: we trust the archive member annotation to be
        # correct/valid.
        # not trsuting it would have sever consequences. depending on
        # where the archive is located (local/remote) and what format it
        # is (fsspec-inspectable), we might need to download it completely
        # in order to verify a matching archive member. Moreover, an archive
        # might also reference another archive as a source, leading to a
        # multiplication of transfer demands

        akey, member_props = self._akeys.from_url(url)
        # we leave all checking logic to git-annex
        try:
            # if it exits clean, the key is still present at at least one
            # remote
            self.repo.call_annex(['checkpresentkey', akey])
            return True
        except CommandError:
            return False

    def _get_from_url(self, url, dst_path):
        """Returns True on success"""
        akey, member_props = self._akeys.from_url(url)

        # TODO add config option to perform legacy approach (download
        # entire archive key. Also add legacy fallback to
        # _extract_from_local_archive() for this scenario
        if self._handle_request_w_fsspec(akey):
            getter = self._get_member_fsspec
        # no fsspec: do we have that key locally?
        elif self._akeys.get_contentpath(akey):
            getter = self._get_member_local_archive
        else:
            return False

        getter(akey, member_props['path'], dst_path)
        return True

    #
    # FSSPEC implementations
    #
    def _get_fsspec_url_(self, akey, amember_path):
        atype = self._akeys.get_archive_type(akey)
        # knowing the archive type is a precondition to get here
        # assert that
        assert atype is not None

        # TODO inject caching approach into URL, based on config
        cache_spec = ''

        # are we lucky and have the archive locally?
        apath = self._akeys.get_contentpath(akey)
        if apath:
            # generate (zip|tar)://...::file:// URL and give to fsspec
            yield f'{atype}://{amember_path}{cache_spec}::{apath.as_uri()}'
            # we are not falling back on remote locations.
            # if extraction from a local archive key did not work.
            # In general this would indicate more sever issues:
            # broken archive, invalid archive member annotation.
            # not worth poking around the network in such cases.
            return

        # query for remote URLs for the akey in case
        # it is not around locally.
        # no prefix to get any urls on record
        for aurl in self.annex.geturls(akey, prefix=''):
            yield f'{atype}://{amember_path}{cache_spec}::{aurl}'

    def _get_member_fsspec(
            self, akey: str, amember_path: Path, dst_path: Path,
    ):
        # with FSSPEC we can handle remotely located archives.
        # this also means that for any such archive, multiple
        # URLs for such remote locations might exist, and would need
        # to be tried. Hence we have to wrap this in a loop
        for fsspec_url in self._get_fsspec_url_(akey, amember_path):
            try:
                self._fsspec_handler.download(fsspec_url, dst_path)
                # whichever one is working is enough of a success
                return
            except Exception as e:
                ce = CapturedException(e)
                self.message(
                    f'Failed to retrieve key from {fsspec_url!r}: {ce}',
                    type='debug')

        raise RemoteError('Failed to access archive member via FSSPEC')

    #
    # Fallback implementations
    #
    def _get_member_local_archive(
            self,
            akey,
            amember_props: Dict,
            dst_path: Path,
    ):
        # TODO make local extraction strategy configurable
        # TODO be able to fall back on legacy code from datalad-core
        raise NotImplementedError


class ArchiveKeys:
    """Cache for information on archive keys"""
    def __init__(self, annex, repo):
        # mapping of archive keys to an info dict
        self._db = {}
        # for talking to the git-annex parent process
        self.annex = annex
        # for running git-annex queries against the repo
        self.repo = repo

    def __contains__(self, key):
        return key in self._db

    def __getitem__(self, key):
        return self._db[key]

    def __setitem__(self, key, value):
        self._db[key] = value

    def get(self, key, default=None):
        return self._db.get(key, default)

    def update(self, key, *args, **kwargs):
        self._db[key].update(*args, **kwargs)

    def from_url(self, url: str) -> str:
        url_props = _decode_dlarchive_url(url)
        # get the key of the containing archive
        key_props = url_props['archive_key']
        key = key_props['key']
        props = self.get(
            key,
            dict(
                backend=key_props['backend'],
                name=key_props['name'],
            )
        )
        if 'atype' not in props:
            props['atype'] = _get_archive_type(
                url_props['archive_key'],
                url_props['member'],
            )
        self[key] = props
        return key, url_props['member']

    def get_archive_type(self, key):
        return self[key]['atype']

    def get_contentpath(self, key):
        props = self.get(key, {})
        if 'path' not in props:
            try:
                # if it exits clean, there will be a content location
                # and the content can be found at the location
                loc = next(self.repo.call_annex_items_([
                    'contentlocation', key]))
                # convert to path. git-annex will report a path relative to the
                # dotgit-dir
                # TODO platform-native?
                loc = self.repo.dot_git / Path(loc)
            except CommandError:
                loc = None
            props['path'] = loc
            # cache
            self[key] = props
        return props['path']


def _get_archive_type(
        akey_props: Dict,
        amember_props: Dict) -> str | None:
    # figure out the archive type, prefer direct annotation.
    # fall back on a recognized file extension for E-type annex backends.
    atype = amember_props.get('atype')
    if not atype and akey_props.get('backend', '').endswith('E'):
        # try by key name extension
        suf = PurePosixPath(akey_props['name']).suffixes
        if '.zip' in suf:
            atype = 'zip'
        elif '.tar' in suf:
            atype = 'tar'

    # TODO perform a mimetype-inspection of the local archive file at `apath`

    # unrecognized, return None
    return atype


def _decode_dlarchive_url(url):
    """Turn a ``dl+archive:`` URL into a property specification

    Returns
    -------
    dict
      None is returned if the URL is invalid.

    Raises
    ------
    ValueError
      For any unsupported condition.
    """
    url_matched = ArchivistRemote.recognized_urls.match(url)
    if not url_matched:
        raise ValueError('Unrecognized dl+archives URL syntax')
    url_matched = url_matched.groupdict()
    # we must have a key, and we must have at least a path property
    # pointing to an archive member
    if any(p not in url_matched for p in ('key', 'props')):
        raise ValueError('Unrecognized dl+archives URL syntax')
    key_matched = ArchivistRemote.key_props.match(url_matched['key'])
    if not key_matched:
        # without a sensible key there is no hope
        raise ValueError(
            f'dl+archives URL with invalid annex key: {url_matched["key"]!r}')
    key_matched = key_matched.groupdict()
    # archive member properties
    props_matched = ArchivistRemote.archive_member_props.match(
        url_matched['props'])
    if not props_matched:
        # without at least a 'path' there is nothing we can do here
        raise ValueError(
            'dl+archives URL contains invalid archive member specification: '
            f'{url_matched["props"]!r}')
    props_matched = props_matched.groupdict()
    try:
        amember_path = PurePosixPath(props_matched['path'])
    except Exception as e:
        # not a valid path specification, die
        ce = CapturedException(e)
        raise ValueError(
            f'dl+archives URL contains invalid archive member path: {ce}')
        return

    # we should be able to work with this return specification
    return dict(
        archive_key=dict(
            key_matched,
            key=url_matched['key'],
        ),
        member=dict(
            props_matched,
            # relay directly as a PurePath object
            path=amember_path,
        ),
    )


def main():
    """cmdline entry point"""
    super_main(
        cls=ArchivistRemote,
        remote_name='archivist',
        description=\
        "access to annex keys stored within other archive-type annex keys ",
    )
