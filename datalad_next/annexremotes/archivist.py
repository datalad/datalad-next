"""git-annex special remote *archivist* for obtaining files from archives
"""

from __future__ import annotations

# General TODO for future improvements
#
# `datalad.archivist.archive-cache-mode=<name>`
#   Choice of archive (access) caching behavior. ``<name>`` can be any of
#
#   ``persistent-whole``
#     This causes an archive to be downloaded completely on first access to any
#     archive member. A regular ``annex get`` is performed and an archive is
#     placed at its standard location in the local annex. Any archive member
#     will be extracted from this local copy.
#
# Some ideas on optional additional cache modes related to dropping as much as
# possible after the special remote is done. However, these modes also come
# with potential issues re parallel access (what if another remote process
# is still using a particular archive... Think about that when there is a
# real need
#
#  ``keep-downloads``
#    No caching will be performed per se. However, when archive member access
#    happens to require a full archive download, a downloaded archive will
#    not be removed after member extraction. In such cases, this mode will
#    behave like ``persistent-whole``.
#
#  ``none``
#    This is behaving like ``keep-downloads``, but any downloaded archive
#    will be dropped again after extraction is complete.

from collections.abc import Iterable
from dataclasses import dataclass

from pathlib import Path
from shutil import copyfileobj
from typing import (
    Dict,
    Generator,
    List,
    Tuple,
)

from datalad_next.archive_operations import ArchiveOperations

# we intentionally limit ourselves to the most basic interface
# and even that we only need to get a `ConfigManager` instance.
# If that class would support a plain path argument, we could
# avoid it entirely
from datalad_next.datasets import LegacyAnnexRepo

from datalad_next.exceptions import CommandError
from datalad_next.types.annexkey import AnnexKey
from datalad_next.types.archivist import ArchivistLocator
from datalad_next.types.enums import ArchiveType

from . import (
    RemoteError,
    SpecialRemote,
    UnsupportedRequest,
    super_main
)


class ArchivistRemote(SpecialRemote):
    """git-annex special remote *archivist* for obtaining files from archives

    Successor of the `datalad-archive` special remote. It claims and acts on
    particular archive locator "URLs", registered for individual annex keys
    (see :class:`datalad_next.types.archivist.ArchivistLocator`). These
    locators identify another annex key that represents an archive (e.g., a
    tarball or a zip files) that contains the respective annex key as a member.
    This special remote trigger the extraction of such members from any
    candidate archive when retrieval of a key is requested.

    This special remote cannot store or remove content. The desired usage
    is to register a locator "URL" for any relevant key via
    ``git annex addurl|registerurl`` or ``datalad addurls``.


    Configuration
    -------------

    The behavior of this special remote can be tuned via a number of
    configuration settings.

    `datalad.archivist.legacy-mode=yes|[no]`
      If enabled, all special remote operations fall back onto the
      legacy ``datalad-archives`` special remote implementation. This mode is
      only provided for backward-compatibility. This legacy implementation
      unconditionally downloads archive files completely, and keeps an
      internal cache of the full extracted archive around. The implied
      200% (or more) storage cost overhead for obtaining a complete dataset
      can be prohibitive for datasets tracking large amount of data
      (in archive files).


    Implementation details
    ----------------------

    *CHECKPRESENT*

    When performing a non-download test for the (continued) presence of an
    annex key (as triggered via ``git annex fsck --fast`` or ``git annex
    checkpresentkey``), the underlying archive containing a key will NOT be
    inspected. Instead, only the continued availability of the annex key for
    the containing archive will be tested.  In other words: this implementation
    trust the archive member annotation to be correct/valid, and it also trusts
    the archive content to be unchanged. The latter will be generally the case,
    but may no with URL-style keys.

    Not implementing such a trust-approach *would* have a number of
    consequences.  Depending on where the archive is located (local/remote) and
    what format it is (fsspec-inspectable or not), we would need to download it
    completely in order to verify a matching archive member.  Moreover, an
    archive might also reference another archive as a source, leading to a
    multiplication of transfer demands.
    """
    def __init__(self, annex):
        super().__init__(annex)
        # the following members will be initialized on prepare()
        # as they require access to the underlying repository
        self._repo = None
        # name of the (git) remote archivist is operating under
        # (for querying the correct configuration)
        self._remotename = None
        # central archive handler cache, initialized on-prepare
        self._ahandlers = None
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
        """This method does nothing, because the special remote requires no
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
        self._remotename = self.annex.getgitremotename()
        # are we in legacy mode?
        # let remote-specific setting take priority (there could be
        # multiple archivist-type remotes configured), and use unspecific switch
        # as a default, with a general default of NO
        if self._getcfg('legacy-mode', default='no').lower() == 'yes':
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

        # central archive key handler coordination
        self._ahandlers = _ArchiveHandlers(
            self._repo,
            # TODO
            #cache_mode=self._getcfg(
            #    'archive-cache-mode',
            #    default='').lower(),
        )

    def claimurl(self, url: str) -> bool:
        """Returns True for :class:`~datalad_next.types.archivist.ArchivistLocator`-style URLs

        Only a lexical check is performed. Any other URL will result in
        ``False`` to be returned.
        """
        try:
            ArchivistLocator.from_str(url)
            return True
        except Exception:
            return False

    def checkurl(self, url: str) -> bool:
        """Parses :class:`~datalad_next.types.archivist.ArchivistLocator`-style URLs

        Returns ``True`` for any syntactically correct URL with all
        required properties.

        The implementation is identical to ``claimurl()``.
        """
        try:
            ArchivistLocator.from_str(url)
        except Exception as e:
            self.message(f'Invalid URL {url!r}: {e}', type='debug')
            return False

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

    def checkpresent(self, key: str) -> bool:
        """Verifies continued availability of the archive referenced by the key

        No content verification of the archive, or of the particular archive
        member is performed. See "Implementation details" of this class
        for a rational.

        Returns
        -------
        bool
            True if the referenced archive key is present on any remote.
            False if not.
        """
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

        # get all associated archive keys, turn into set because any key might
        # map to multiple archive keys, and we only need to check them once
        akeys = set(
            str(ArchivistLocator.from_str(url).akey)
            for url in self._get_key_dlarchive_urls(key)
        )
        # As with transfer_retrieve blindly checking akeys in arbitrary
        # order is stupid. We should again sort by (local) availability.
        # if we have an archive locally we can check faster, we could check
        # more precisely (actually look into it).
        # We only need to find one archive with a hit, if we search clever
        # we can exit earlier.
        # So let's do a two-pass approach, first check local availability
        # for any archive key, and only if that does not find us an archive
        # go for the remotes
        if any(_get_key_contentpath(self._repo, akey) for akey in akeys):
            # any one is good enough
            # TODO here we could actually look into the archive and
            # verify member presence without relatively little cost
            return True

        for akey in akeys:
            # we leave all checking logic to git-annex
            try:
                # if it exits clean, the key is still present at at least one
                # remote
                self._repo.call_annex(['checkpresentkey', akey])
                return True
            except CommandError:
                self.message(
                    f'Archive key candidate {akey} for key {key} '
                    'not present in any known remote or here',
                    type='debug')

        # when we end up here, we have tried all known archives keys and
        # found none to be present in any known location
        return False

    def transfer_retrieve(self, key: str, localfilename: str):
        """Retrieve an archive member from a (remote) archive

        All registered locators for a requested key will be sorted by
        availability and size of the references archives. For each archive
        the most suitable handler will be initialized, and extraction
        of the identified member will be attempted. If that fails, the next
        handler is tried until all candidate handlers are exhausted.
        Depending on the archive availability and type, archives may need
        to be retrieved from remote sources.
        """
        # rely on from_locators() to bring the candidate archives
        # in some intelligent order to try one after the other.
        # break ASAP to prevent unnecessary processing
        msgs = []
        try:
            for handler, locs in self._ahandlers.from_locators([
                    ArchivistLocator.from_str(url)
                    for url in self._get_key_dlarchive_urls(key)]):
                with Path(localfilename).open('wb') as dst_fp:
                    for loc in locs:
                        try:
                            with handler.open(loc.member) as fp:
                                # TODO progress reporting
                                # but what progress? the extraction
                                # may just be one part, there could also
                                # be file retrieval
                                copyfileobj(fp, dst_fp)
                            return
                        except Exception as e:
                            msg = f'Failed to extract {key!r} from ' \
                                  f'{handler} ({loc.member}): {e}'
                            self.message(msg, type='debug')
                            msgs.append(msg)
        except Exception as e:
            raise RemoteError(f'Could not obtain {key!r}') from e

        raise RemoteError(f'Could not obtain {key!r} from any archive')

    def transfer_store(self, key: str, filename: str):
        """Raises ``UnsupportedRequest``. This operation is not supported."""
        raise UnsupportedRequest('This remote cannot store content')

    def remove(self, key: str):
        """Raises ``UnsupportedRequest``. This operation is not supported."""
        raise UnsupportedRequest('This remote cannot remove content')

    #
    # Helpers
    #
    # TODO this could be promoted to SpecialRemote as a generic helper
    # would need standardization of remote name query in `prepare()`
    def _getcfg(self, name: str, default=None):
        """Get a particular special remote configuration item value

        Parameters
        ----------
        name: str
          The name of the "naked" configuration item, without any
          sub/sections. Must be a valid git-config variable name, i.e.,
          case-insensitive, only alphanumeric characters and -, and
          must start with an alphabetic character.
        default:
          A default value to be returned if there is no configuration.
        """
        cfgget = self._repo.config.get
        rname = self._remotename
        return cfgget(
            f'remote.{rname}.archivist-{name}',
            default=cfgget(
                f'datalad.archivist.{name}',
                default=default,
            )
        )

    def _get_key_dlarchive_urls(self, key):
        return self.annex.geturls(key, prefix='dl+archive:')


def main():
    """CLI entry point installed as ``git-annex-remote-archivist``"""
    super_main(
        cls=ArchivistRemote,
        remote_name='archivist',
        description=\
        "access to annex keys stored within other archive-type annex keys ",
    )


#
# Internal helpers
#

@dataclass
class _ArchiveInfo:
    """Representation of an archive used internally by ``_ArchiveHandlers``"""
    local_path: Path | None
    handler: ArchiveOperations | None = None
    type: ArchiveType | None = None


class _ArchiveHandlers:
    """Wraps annex repo to provide access to keys given by ArchivistLocator(s)

    The main functionality is provided by ``from_locators()``.
    """
    # TODO make archive access caching behavior configurable from the outside
    def __init__(self, repo):
        # mapping of archive keys to an info dict
        self._db: Dict[AnnexKey, _ArchiveInfo] = {}
        # for running git-annex queries against the repo
        self._repo = repo

    def from_locators(
            self, locs: List[ArchivistLocator]
    ) -> Generator[Tuple[ArchiveOperations, Iterable[ArchivistLocator]],
                   None, None]:
        """Produce archive handlers for the given locators

        Yield them one-by-one in a maximally intelligent order for efficient
        retrieval (i.e., handlers for archives that are already available
        locally first. Each handlers is yielded fully prepared, i.e.
        if necessary an archive is retrieved before the handler is yielded.
        Therefore a consumer should not fully consume the returned
        generator when an operation can be completed before all handlers
        are exhausted.

        Parameters
        ----------
        locs: List[ArchivistLocator]
          Any number of locators that must all refer to the same annex key
          (key, not archive annex key!).

        Yields
        ------
        ArchiveOperations, Iterable[ArchivistLocator]
          The referenced archive annex keys are de-duplicated and sorted by
          (local) availability and size.  For each archive key a suitable
          ``ArchiveOperations`` handler is yielded together with the locators
          matching the respective archive.
        """
        # determine all candidate source archive keys
        akeys = set(loc.akey for loc in locs)
        # determine which of the known handlers point to a local archive,
        # yield those
        for akey, kh in {
                akey: self._db[akey]
                for akey in akeys
                if akey in self._db and self._db[akey].handler
        }.items():
            # local_path will be None now, if not around
            if kh.local_path:
                # we found one with a local archive.
                # yield handler and all matching locators
                yield kh.handler, [loc for loc in locs if loc.akey == akey]
                # if we get here, this did not work, do not try again
                akeys.remove(akey)
        # of the handlers we do not yet know, which ones have local data,
        # yield those
        for akey in [k for k in akeys if k not in self._db]:
            ainfo = self._get_archive_info(akey, locs)
            # cache for later
            self._db[akey] = ainfo
            if not ainfo.local_path:
                # do not try a local handler, but keep the akey itself in the
                # race, we might need to try "remote" access later on
                continue

            handler = self._get_local_handler(ainfo)
            # store for later
            ainfo.handler = handler
            # yield handler and all matching locators
            yield handler, [loc for loc in locs if loc.akey == akey]
            # if we get here, this did not work, do not try again
            akeys.remove(akey)

        # of the handlers we do know, but do not have local data,
        # possibly obtain the archive, yield those
        #
        # this is the same as the first loop, but this time all local
        # paths are checked, and some akeys might already have been
        # removed
        for akey, kh in {
                akey: self._db[akey]
                for akey in akeys
                if akey in self._db and self._db[akey].handler
        }.items():
            yield handler, [loc for loc in locs if loc.akey == akey]
            # if we get here, this did not work, do not try again
            akeys.remove(akey)

        # all that is left is to create "remote" handlers and yield them.
        # collect any exceptions to report them at the end, if needed
        exc = []
        # but this time sort the keys to start with the smallest ones
        # (just in case a download is involved)
        for akey in sorted(akeys, key=lambda x: x.size):
            # at this point we must have an existing _ArchiveInfo record
            # for this akey
            ainfo = self._db[akey]
            # but we do not have a handler yet
            assert ainfo.handler is None
            try:
                handler = self._get_remote_handler(akey, ainfo)
            except Exception as e:
                exc.append(e)
                continue
            # if this worked, store the handler for later
            ainfo.handler = handler
            yield handler, [loc for loc in locs if loc.akey == akey]

        # if we get here we can stop -- everything was tried. If there were
        # exceptions, make sure to report them
        if exc:
            # TODO better error
            e = RuntimeError(
                'Exhausted all candidate archive handlers '
                f'(previous failures {exc})')
            e.errors = exc
            raise e

    def _get_archive_info(
            self,
            akey: AnnexKey,
            locs: Iterable[ArchivistLocator],
    ) -> _ArchiveInfo:
        # figure out if the archive is local first
        local_path = _get_key_contentpath(self._repo, str(akey))

        # get all reported archive types
        akey_atypes = set(
            loc.atype for loc in locs if loc.akey == akey and loc.atype
        )
        # if we have (consistent) information, pick the type, if not
        # set to None/ignore and wait for type detection by handler
        akey_atype = None if len(akey_atypes) != 1 else akey_atypes.pop()

        ainfo = _ArchiveInfo(
            local_path=local_path,
            type=akey_atype,
        )
        # cache for later
        self._db[akey] = ainfo
        return ainfo

    def _get_local_handler(self, ainfo: _ArchiveInfo) -> ArchiveOperations:
        if not ainfo.type:
            # TODO we could still do mime-type detection. We have the
            # archive file present locally.
            # check datalad-core how it is done in archive support
            raise NotImplementedError

        if ainfo.type == ArchiveType.tar:
            from datalad_next.archive_operations.tarfile import (
                TarArchiveOperations)
            return TarArchiveOperations(
                ainfo.local_path,
                cfg=self._repo.config,
            )
        else:
            raise NotImplementedError

    def _get_remote_handler(
            self,
            akey: AnnexKey,
            ainfo: _ArchiveInfo,
    ) -> ArchiveOperations:
        # right now we have no remote handlers available
        # TODO: use akey to ask the repo for URLs from which the key
        # would be available and select a remote handler to work
        # with that URL
        # instead we retrieve the archive
        res = self._repo.get(str(akey), key=True)
        # if the akey was already around, `res` could be an empty list.
        # however, under these circumstances we should not have ended
        # up here. assert to alert on logic error in that case
        assert isinstance(res, dict)
        if res.pop('success', None) is not True:
            # TODO better error
            raise RuntimeError(f'Failed to download archive key: {res!r}')
        # now we have the akey locally
        ainfo.local_path = _get_key_contentpath(self._repo, str(akey))
        return self._get_local_handler(ainfo)


def _get_key_contentpath(repo: LegacyAnnexRepo, key: str):
    """Return ``Path`` to a locally present annex key, or ``None``

    ``None`` is return when there is not such key present locally.
    """
    try:
        # if it exits clean, there will be a content location
        # and the content can be found at the location
        loc = next(repo.call_annex_items_(['contentlocation', key]))
        # convert to path. git-annex will report a path relative to the
        # dotgit-dir
        # TODO platform-native?
        loc = repo.dot_git / Path(loc)
    except CommandError:
        loc = None
    return loc
