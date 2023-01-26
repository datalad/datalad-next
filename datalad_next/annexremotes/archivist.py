"""
git-annex special remote *archivist* for obtaining files from archives
======================================================================

Replacement for the `datalad-archive` URL


Syntax of recognized URLs
-------------------------

This special remote only works with particular URLs, originally introduced by
the ``datalad-archives`` special remote. They take the following minimal form::

    dl+archive:<archive-key>#path=<path-in-archive>

where ``<archive-key>`` is a regular git-annex key (known to the repository)
of an archive, and ``<path-in-archive>`` is a POSIX-style relative path
pointing to a member within the archive.

Two optional, additional attributes ``size`` and ``atype`` are recognized
(only ``size`` is also understood by the ``datalad-archives`` special remote).

``size`` declares the size of the (extracted) archive member in bytes::

    dl+archive:<archive-key>#path=<path-in-archive>&size=<size-in-bytes>

``atype`` declares the type of the containing archive using a label. Currently
recognized labels are ``tar`` (a TAR archive, compressed or not), and ``zip``
(a ZIP archive). This optional type annotation enables decision making
regarding the ability for remote access to archives without downloading them.
If no type information is given, and no type can be inferred from the archive
key (via ``*E``-type git-annex backends, such as DataLad's default ``MD5E``),
no such remote access will be attempted, and archives are downloaded in full
when keys are requested from them::

    dl+archive:<archive-key>#path=<path-in-archive>&atype=<tar|zip>

Order in the fragment part of the URL (after ``#``) is significant. ``path``
must come first, followed by ``size`` or ``atype``. If both ``size`` and
``atype`` are present, ``size`` must be declared first. A complete example
of a URL is::

    dl+archive:MD5-s389--e9f624eb778e6f945771c543b6e9c7b2#path=dir/file.csv&size=234&atype=tar


Configuration
-------------

The behavior of this special remote can be tuned via a number of
configuration settings.

`datalad.archivist.legacymode=yes|[no]`
  If enabled, all special remote operations fall back onto the
  legacy ``datalad-archives`` special remote implementation. This mode is
  only provided for backward-compatibility. This legacy implementation
  unconditionally downloads archive files completely, and keeps an
  internal cache of the full extracted archive around. The implied
  200% (or more) storage cost overhead for obtaining a complete dataset
  can be prohibitive for datasets tracking large amount of data
  (in archive files).
  If there are multiple ``archivist`` special remotes in use for a
  single repository, their behavior can be tuned individually by
  setting a corresponding, remote-specific configuration item::

    remote.<remotename>.archivist-legacymode=yes|[no]

  which takes precedence over the general configuration switch.


Implementation details
----------------------

*CHECKPRESENT*

When performing a non-download test for the (continued) presence of an annex
key (as triggered via ``git annex fsck --fast`` or ``git annex
checkpresentkey``), the underlying archive containing a key will NOT be
inspected. Instead, only the continued availability of the annex key for the
containing archive will be tested.  In other words: this implementation trust
the archive member annotation to be correct/valid, and it also trusts the
archive content to be unchanged. The latter will be generally the case, but may
no with URL-style keys.

Not implementing such a trust-approach *would* have a number of consequences.
Depending on where the archive is located (local/remote) and what format it is
(fsspec-inspectable or not), we would need to download it completely in order
to verify a matching archive member.  Moreover, an archive might also reference
another archive as a source, leading to a multiplication of transfer demands.
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
)

from . import (
    RemoteError,
    SpecialRemote,
    UnsupportedRequest,
    super_main
)


class ArchivistRemote(SpecialRemote):
    """ """
    # be relatively permissive
    recognized_urls = re.compile(r'^dl\+archive:(?P<key>.*)#(?P<props>.*)')
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
        # the following members will be initialized on prepare()
        # as they require access to the underlying repository
        self._repo = None
        # fsspec operations handler
        self._fsspec_handler = None
        # central archive key cache, initialized on-prepare
        self._akeys = None
        # a potential instance of the legacy datalad-archives implementation
        self._legacy_special_remote = None

    def __getattribute__(self, name: str):
        """Redirect top-level API calls to legacy implementation, if needed"""
        lsr = SpecialRemote.__getattribute__(self, '_legacy_special_remote')
        if lsr is None or name not in (
            'initremote',
            'prepare',
            'claimurl',
            'checkurl',
            'checkpresent',
            'remove',
            'whereis',
            'transfer_retrieve',
            'stop',
        ):
            # we are not in legacy mode or this is no top-level API call
            return SpecialRemote.__getattribute__(self, name)

        return getattr(lsr, name)

    def initremote(self):
        """``git annex initremote|enableremote`` configuration implementation

        This method does nothing, because the special remote requires no
        particular setup.
        """
        pass

    def prepare(self):
        """Prepare the special remote for requests by git-annex

        If the special remote is instructed to run in "legacy mode", all
        subsequent operations will be processed by the ``datalad-archives``
        special remote implementation!
        """
        self._repo = LegacyAnnexRepo(self.annex.getgitdir())
        remotename = self.annex.getgitremotename()
        # are we in legacy mode?
        # let remote-specific setting take priority (there could be
        # multiple archivist-type remotes configured), and use unspecific switch
        # as a default, with a general default of NO
        if self._repo.config.getbool(
                f'remote.{remotename}', 'archivist-legacymode',
                default=self._repo.config.getbool(
                    'datalad.archivist', 'legacymode', default=False)):
            # ATTENTION DEBUGGERS!
            # If we get here, we will bypass all of the archivist
            # implementation! Check __getattribute__() -- pretty much no
            # other code in this file will run!!!
            # __getattribute__ will relay all top-level operations
            # to an instance of the legacy implementation
            from datalad.customremotes.archives import ArchiveAnnexCustomRemote
            lsr = ArchiveAnnexCustomRemote(self.annex)
            lsr.prepare()
            # we can skip everything else, it won't be triggered anymore
            self._legacy_special_remote = lsr
            return

        # central archive key cache
        self._akeys = _ArchiveKeys(
            self.annex,
            self._repo,
        )

        # try to work without fsspec
        try:
            from datalad_next.url_operations.fsspec import FsspecUrlOperations
            # TODO support passing constructur arguments from configuration
            # pass the repo's config manager to the handler to enable
            # dataset-specific customization via configuration
            self._fsspec_handler = FsspecUrlOperations(cfg=self._repo.config)
        except ImportError:
            self.message('FSSPEC support disabled, dependency not available',
                         type='debug')

    def claimurl(self, url: str) -> bool:
        """Claims (returns True for) ``dl+archive:`` URLs

        Only a lexical check is performed. Any other URL will result in
        ``False`` to be returned.
        """
        if ArchivistRemote.recognized_urls.match(url):
            return True
        else:
            return False

    def checkurl(self, url: str) -> bool:
        """Parse ``dl+archive:`` URL

        Returns ``True`` for any syntactically correct URL with all
        required properties.

        The archive key related outcomes of the parsing are kept
        in an internal cache to speed up future property retrieval.
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

    def transfer_retrieve(self, key: str, localfilename: str):
        """Retrieve an archive member from a (remote) archive

        All URLs recorded for the requested ``key`` will be tried in order.
        For each URL a decision is made whether to attempt (possibly
        remote) partial extraction via FSSPEC. If possible, and allowed by
        configuration, archives are accessed directly at their (remote)
        location without requiring a download, and without performing
        a full extraction of an archive. If multiple access URLs are on-record
        for a particular archive, all URLs will be tried in order until
        access is successful, or the list is exhausted.
        """
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

    def checkpresent(self, key: str) -> bool:
        """Verifies continued availability of the archive referenced by the key

        No content verification of the archive, or of the particular archive
        member is performed. See "Implementation details" of this module
        for a rational.

        Returns
        -------
        bool
            True if the referenced archive key is present on any remote.
            False if not.

        Raises
        ------
        RemoteError
            If the presence of the key couldn't be determined, eg. in case of
            connection error.
        """
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

    def transfer_store(self, key: str, filename: str):
        """Raises ``UnsupportedRequest``. This operation is not supported."""
        raise UnsupportedRequest('This special remote cannot store content')

    def remove(self, key: str):
        """Raises ``UnsupportedRequest``. This operation is not supported."""
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
        # not trusting it would have sever consequences. depending on
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
            self._repo.call_annex(['checkpresentkey', akey])
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


class _ArchiveKeys:
    """Cache for information on archive keys"""
    def __init__(self, annex, repo):
        # mapping of archive keys to an info dict
        self._db = {}
        # for running git-annex queries against the repo
        self._repo = repo

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
                loc = next(self._repo.call_annex_items_([
                    'contentlocation', key]))
                # convert to path. git-annex will report a path relative to the
                # dotgit-dir
                # TODO platform-native?
                loc = self._repo.dot_git / Path(loc)
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
    """CLI entry point installed as ``git-annex-remote-archivist``"""
    super_main(
        cls=ArchivistRemote,
        remote_name='archivist',
        description=\
        "access to annex keys stored within other archive-type annex keys ",
    )
