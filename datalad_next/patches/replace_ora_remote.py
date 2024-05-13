""" Patch ``datalad.distributed.ora_remote.ORARemote``

This patch replaces the class :class:`datalad.distributed.ora_remote.ORARemote`
with an updated version that should work properly on Linux, OSX, and Windows.

The main difference to the original code is that all path-operations are
performed on URL-paths. Those are represented by instances of `PurePosixPath`.
All subclasses of :class:`BaseIO`, i.e. :class:`LocalIO`, :class:`SSHRemoteIO`,
and :class:`HTTPRemoteIO`, are extended to contain the method
:meth:`url2transport_path`. This method converts an URL-path into the correct
path for the transport, i.e. the IO abstraction.

Before methods on a subclass of :class:`BaseIO` that require a path are called,
the generic URL-path is converted into the correct path for the IO-class by
calling :meth:`url2transport_path` on the respective IO-class.

The patch keeps changes to the necessary minimum. That means the source
is mostly identical to the original. Besides the changes described above,
more debug output was added.

NOTE: this patch only provides :class:`ORARemote`. The patches that add a
:meth:`url2transport_path`-method to :class:`LocalIO` and to :class:`HTTPRemoteIO`
are contained in module ``datalad_next.patches.add_method_url2localpath``. The
reason to keep them separate is that the patch from module
``datalad_next.patches.replace_create_sibling_ria`` require them as well.
For :class:`SSHRemoteIO` the method is included in the patch definition of
:class:`SSHRemoteIO`, which is contained in the module
``datalad_next.patches.replace_sshremoteio``.
"""
from __future__ import annotations

import re
import urllib.parse
from pathlib import (
    Path,
    PurePosixPath,
)
from shlex import quote as sh_quote
import logging

from datalad.config import anything2bool
from datalad.customremotes import (
    ProtocolError,
    SpecialRemote,
)
from datalad.distributed.ora_remote import (
    HTTPRemoteIO,
    LocalIO,
    RIARemoteError,
    SSHRemoteIO,
    _get_datalad_id,
    _get_gitcfg,
    handle_errors,
    NoLayoutVersion,
)
from datalad.support.annex_utils import _sanitize_key
from datalad.support.annexrepo import AnnexRepo
from datalad.customremotes.ria_utils import (
    get_layout_locations,
    UnknownLayoutVersion,
    verify_ria_url,
)
from datalad.utils import on_windows

from . import apply_patch


lgr = logging.getLogger('datalad.customremotes.ria_remote')

drive_letter_matcher = re.compile('^[A-Z]:')
slash_drive_letter_matcher = re.compile('^/[A-Z]:')

DEFAULT_BUFFER_SIZE = 65536


def canonify_url(url: str | None):
    """For file URLs on windows: put the drive letter into the path component"""
    if not on_windows or url is None:
        return url
    url_parts = urllib.parse.urlparse(url)
    if url_parts.scheme not in ('ria+file', 'file'):
        return url

    match = drive_letter_matcher.match(url_parts.netloc)
    if not match:
        return url

    return f'{url_parts.scheme}:///{match.string}{url_parts.path}'


def de_canonify_url(url: str | None):
    """For file URLs on windows: put the drive letter into the netloc component"""
    if not on_windows or url is None:
        return url
    url_parts = urllib.parse.urlparse(url)
    if url_parts.scheme not in ('ria+file', 'file'):
        return url

    match = slash_drive_letter_matcher.match(url_parts.path)
    if not match:
        return url

    return f'{url_parts.scheme}://{url_parts.path[1:3]}{url_parts.path[3:]}'


# `ORARemote` taken from datalad-core@864dc4ae24c8aac0ec4003604543b86de4735732:
class ORARemote(SpecialRemote):
    """This is the class of RIA remotes.
    """

    dataset_tree_version = '1'
    object_tree_version = '2'
    # TODO: Move known versions. Needed by creation routines as well.
    known_versions_objt = ['1', '2']
    known_versions_dst = ['1']

    @handle_errors
    def __init__(self, annex):
        super(ORARemote, self).__init__(annex)
        if hasattr(self, 'configs'):
            # introduced in annexremote 1.4.2 to support LISTCONFIGS
            self.configs['url'] = "RIA store to use"
            self.configs['push-url'] = "URL for pushing to the RIA store. " \
                                       "Optional."
            self.configs['archive-id'] = "Dataset ID (fallback: annex uuid. " \
                                         "Should be set automatically by " \
                                         "datalad"
        # the local repo
        self._repo = None
        self.gitdir = None
        self.name = None  # name of the special remote
        self.gitcfg_name = None  # name in respective git remote

        self.ria_store_url = None
        self.ria_store_pushurl = None
        # machine to SSH-log-in to access/store the data
        # subclass must set this
        self.storage_host = None
        self.storage_host_push = None
        # must be absolute, and POSIX (will be instance of PurePosixPath)
        # subclass must set this
        self.store_base_path = None
        self.store_base_path_push = None
        # by default we can read and write
        self.read_only = False
        self.force_write = None
        self.ignore_remote_config = None
        self.remote_log_enabled = None
        self.remote_dataset_tree_version = None
        self.remote_object_tree_version = None

        # for caching the remote's layout locations:
        self.remote_git_dir = None
        self.remote_archive_dir = None
        self.remote_obj_dir = None
        # lazy IO:
        self._io = None
        self._push_io = None

        # cache obj_locations:
        self._last_archive_path = None
        self._last_keypath = (None, None)

        # SSH "streaming" buffer
        self.buffer_size = DEFAULT_BUFFER_SIZE

    # PATCH: add a helper to assert the type of a path.
    @staticmethod
    def _assert_pure_posix_path(path):
        assert path.__class__ is PurePosixPath

    # PATCH: add a close function to ensure that all IO-abstraction objects are
    # closed.
    def close(self):
        if self._io:
            self._io.close()
            self._io = None
        if self._push_io:
            self._push_io.close()
            self._push_io = None

    def verify_store(self):
        """Check whether the store exists and reports a layout version we
        know

        The layout of the store is recorded in base_path/ria-layout-version.
        If the version found on the remote end isn't supported and `force-write`
        isn't configured, sets the remote to read-only operation.
        """
        # THE PATCH: assert path type and perform operation on abstract path
        self._assert_pure_posix_path(self.store_base_path)
        dataset_tree_version_file = self.store_base_path / 'ria-layout-version'

        # check dataset tree version
        try:
            self.remote_dataset_tree_version = \
                self._get_version_config(dataset_tree_version_file)
        except Exception as exc:
            raise RIARemoteError("RIA store unavailable.") from exc
        if self.remote_dataset_tree_version not in self.known_versions_dst:
            # Note: In later versions, condition might change in order to
            # deal with older versions.
            raise UnknownLayoutVersion(f"RIA store layout version unknown: "
                                       f"{self.remote_dataset_tree_version}")

    def verify_ds_in_store(self):
        """Check whether the dataset exists in store and reports a layout
        version we know

        The layout is recorded in
        'dataset_somewhere_beneath_base_path/ria-layout-version.'
        If the version found on the remote end isn't supported and `force-write`
        isn't configured, sets the remote to read-only operation.
        """

        object_tree_version_file = self.remote_git_dir / 'ria-layout-version'

        # check (annex) object tree version
        try:
            self.remote_object_tree_version =\
                self._get_version_config(object_tree_version_file)
        except Exception as e:
            raise RIARemoteError("Dataset unavailable from RIA store.")
        if self.remote_object_tree_version not in self.known_versions_objt:
            raise UnknownLayoutVersion(f"RIA dataset layout version unknown: "
                                       f"{self.remote_object_tree_version}")

    def _load_local_cfg(self):

        # this will work, even when this is not a bare repo
        # but it is not capable of reading out dataset/branch config
        self._repo = AnnexRepo(self.gitdir)

        cfg_map = {"ora-force-write": "force_write",
                   "ora-ignore-ria-config": "ignore_remote_config",
                   "ora-buffer-size": "buffer_size",
                   "ora-url": "ria_store_url",
                   "ora-push-url": "ria_store_pushurl"
                   }

        # in initremote we may not have a reliable name of the git remote config
        # yet. Go with the default.
        gitcfg_name = self.gitcfg_name or self.name
        if gitcfg_name:
            for cfg, att in cfg_map.items():
                value = self._repo.config.get(f"remote.{gitcfg_name}.{cfg}")
                if value is not None:
                    self.__setattr__(cfg_map[cfg], value)
                    if cfg == "ora-url":
                        self.ria_store_url_source = 'local'
                    elif cfg == "ora-push-url":
                        self.ria_store_pushurl_source = 'local'
            if self.buffer_size:
                try:
                    self.buffer_size = int(self.buffer_size)
                except ValueError:
                    self.message(f"Invalid value of config "
                                 f"'remote.{gitcfg_name}."
                                 f"ora-buffer-size': {self.buffer_size}")
                    self.buffer_size = DEFAULT_BUFFER_SIZE

        if self.name:
            # Consider deprecated configs if there's no value yet
            if self.force_write is None:
                self.force_write = self._repo.config.get(
                    f'annex.ora-remote.{self.name}.force-write')
                if self.force_write:
                    self.message("WARNING: config "
                                 "'annex.ora-remote.{}.force-write' is "
                                 "deprecated. Use 'remote.{}.ora-force-write' "
                                 "instead.".format(self.name, self.gitcfg_name))
                    try:
                        self.force_write = anything2bool(self.force_write)
                    except TypeError:
                        raise RIARemoteError("Invalid value of config "
                                             "'annex.ora-remote.{}.force-write'"
                                             ": {}".format(self.name,
                                                           self.force_write))

            if self.ignore_remote_config is None:
                self.ignore_remote_config = self._repo.config.get(
                    f"annex.ora-remote.{self.name}.ignore-remote-config")
                if self.ignore_remote_config:
                    self.message("WARNING: config "
                                 "'annex.ora-remote.{}.ignore-remote-config' is"
                                 " deprecated. Use "
                                 "'remote.{}.ora-ignore-ria-config' instead."
                                 "".format(self.name, self.gitcfg_name))
                    try:
                        self.ignore_remote_config = \
                            anything2bool(self.ignore_remote_config)
                    except TypeError:
                        raise RIARemoteError(
                            "Invalid value of config "
                            "'annex.ora-remote.{}.ignore-remote-config': {}"
                            "".format(self.name, self.ignore_remote_config))

    def _load_committed_cfg(self, fail_noid=True):

        # which repo are we talking about
        self.gitdir = self.annex.getgitdir()

        # go look for an ID
        self.archive_id = self.annex.getconfig('archive-id')
        if fail_noid and not self.archive_id:
            # TODO: Message! "archive ID" is confusing. dl-id or annex-uuid
            raise RIARemoteError(
                "No archive ID configured. This should not happen.")

        # what is our uuid?
        self.uuid = self.annex.getuuid()

        # RIA store URL(s)
        self.ria_store_url = self.annex.getconfig('url')
        if self.ria_store_url:
            self.ria_store_url_source = 'annex'
        self.ria_store_pushurl = self.annex.getconfig('push-url')
        if self.ria_store_pushurl:
            self.ria_store_pushurl_source = 'annex'

        # TODO: This should prob. not be done! Would only have an effect if
        #       force-write was committed annex-special-remote-config and this
        #       is likely a bad idea.
        self.force_write = self.annex.getconfig('force-write')
        if self.force_write == "":
            self.force_write = None

        # Get the special remote name
        # TODO: Make 'name' a property of `SpecialRemote`;
        #       Same for `gitcfg_name`, `_repo`?
        self.name = self.annex.getconfig('name')
        if not self.name:
            self.name = self.annex.getconfig('sameas-name')
        if not self.name:
            # TODO: Do we need to crash? Not necessarily, I think. We could
            #       still find configs and if not - might work out.
            raise RIARemoteError(
                "Cannot determine special remote name, got: {}".format(
                    repr(self.name)))
        # Get the name of the remote entry in .git/config.
        # Note, that this by default is the same as the stored name of the
        # special remote, but can be different (for example after
        # git-remote-rename). The actual connection is the uuid of the special
        # remote, not the name.
        try:
            self.gitcfg_name = self.annex.getgitremotename()
        except (ProtocolError, AttributeError):
            # GETGITREMOTENAME not supported by annex version or by annexremote
            # version.
            # Lets try to find ourselves: Find remote with matching annex uuid
            response = _get_gitcfg(self.gitdir,
                                   r"^remote\..*\.annex-uuid",
                                   regex=True)
            response = response.splitlines() if response else []
            candidates = set()
            for line in response:
                k, v = line.split()
                if v == self.annex.getuuid():  # TODO: Where else? self.uuid?
                    candidates.add(''.join(k.split('.')[1:-1]))
            num_candidates = len(candidates)
            if num_candidates == 1:
                self.gitcfg_name = candidates.pop()
            elif num_candidates > 1:
                self.message("Found multiple used remote names in git "
                             "config: %s" % str(candidates))
                # try same name:
                if self.name in candidates:
                    self.gitcfg_name = self.name
                    self.message("Choose '%s'" % self.name)
                else:
                    self.gitcfg_name = None
                    self.message("Ignore git config")
            else:
                # No entry found.
                # Possible if we are in "initremote".
                self.gitcfg_name = None

    def _load_cfg(self, gitdir, name):
        # Whether or not to force writing to the remote. Currently used to
        # overrule write protection due to layout version mismatch.
        self.force_write = self._repo.config.get(
            f'annex.ora-remote.{name}.force-write')

        # whether to ignore config flags set at the remote end
        self.ignore_remote_config = \
            self._repo.config.get(
                f'annex.ora-remote.{name}.ignore-remote-config')

        # buffer size for reading files over HTTP and SSH
        self.buffer_size = self._repo.config.get(
            f"remote.{name}.ora-buffer-size")

        if self.buffer_size:
            self.buffer_size = int(self.buffer_size)

    def _verify_config(self, fail_noid=True):
        # try loading all needed info from (git) config

        # first load committed config
        self._load_committed_cfg(fail_noid=fail_noid)
        # now local configs (possible overwrite of committed)
        self._load_local_cfg()
        # PATCH: use canonified URLs
        self.ria_store_url = canonify_url(self.ria_store_url)
        self.ria_store_pushurl = canonify_url(self.ria_store_pushurl)

        # get URL rewriting config
        url_cfgs = {k: v for k, v in self._repo.config.items()
                    if k.startswith('url.')}

        if self.ria_store_url:
            self.storage_host, self.store_base_path, self.ria_store_url = \
                verify_ria_url(self.ria_store_url, url_cfgs)

        else:
            # There's one exception to the precedence of local configs:
            # Age-old "ssh-host" + "base-path" configs are only considered,
            # if there was no RIA URL (local or committed). However, issue
            # deprecation warning, if that situation is encountered:
            host = None
            path = None

            if self.name:
                host = self._repo.config.get(
                    f'annex.ora-remote.{self.name}.ssh-host') or \
                       self.annex.getconfig('ssh-host')
                # Note: Special value '0' is replaced by None only after checking
                # the repository's annex config. This is to uniformly handle '0' and
                # None later on, but let a user's config '0' overrule what's
                # stored by git-annex.
                self.storage_host = None if host == '0' else host
                path = self._repo.config.get(
                    f'annex.ora-remote.{self.name}.base-path') or \
                       self.annex.getconfig('base-path')
                self.store_base_path = path.strip() if path else path

            if path or host:
                self.message("WARNING: base-path + ssh-host configs are "
                             "deprecated and won't be considered in the future."
                             " Use 'git annex enableremote {} "
                             "url=<RIA-URL-TO-STORE>' to store a ria+<scheme>:"
                             "//... URL in the special remote's config."
                             "".format(self.name),
                             type='info')


        if not self.store_base_path:
            raise RIARemoteError(
                "No base path configured for RIA store. Specify a proper "
                "ria+<scheme>://... URL.")

        # the base path is ultimately derived from a URL, always treat as POSIX
        self.store_base_path = PurePosixPath(self.store_base_path)
        if not self.store_base_path.is_absolute():
            raise RIARemoteError(
                'Non-absolute RIA store base path configuration: %s'
                '' % str(self.store_base_path))

        if self.ria_store_pushurl:
            if self.ria_store_pushurl.startswith("ria+http"):
                raise RIARemoteError("Invalid push-url: {}. Pushing over HTTP "
                                     "not implemented."
                                     "".format(self.ria_store_pushurl))
            self.storage_host_push, \
            self.store_base_path_push, \
            self.ria_store_pushurl = \
                verify_ria_url(self.ria_store_pushurl, url_cfgs)
            self.store_base_path_push = PurePosixPath(self.store_base_path_push)

    def _get_version_config(self, path):
        """ Get version and config flags from RIA store's layout file
        """

        if self.ria_store_url:
            # construct path to ria_layout_version file for reporting
            # PATCH: use abstract path
            local_store_base_path = self.store_base_path
            target_ri = (
                self.ria_store_url[4:]
                + "/"
                + path.relative_to(local_store_base_path).as_posix()
            )
        elif self.storage_host:
            target_ri = "ssh://{}{}".format(self.storage_host, path.as_posix())
        else:
            target_ri = path.as_uri()

        try:
            # PATCH: convert abstract path to io-specific concrete path
            file_content = self.io.read_file(
                self.io.url2transport_path(path)
            ).strip().split('|')

        # Note, that we enhance the reporting here, as the IO classes don't
        # uniformly operate on that kind of RI (which is more informative
        # as it includes the store base address including the access
        # method).
        except FileNotFoundError as exc:
            raise NoLayoutVersion(
                f"{target_ri} not found, "
                f"self.ria_store_url: {self.ria_store_url}, "
                f"self.store_base_path: {self.store_base_path}, "
                f"self.store_base_path_push: {self.store_base_path_push}, "
                f"path: {type(path)} {path}") from exc
        except PermissionError as exc:
            raise PermissionError(f"Permission denied: {target_ri}") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to access {target_ri}") from exc

        if not (1 <= len(file_content) <= 2):
            self.message("invalid version file {}".format(path),
                         type='info')
            return None

        remote_version = file_content[0]
        remote_config_flags = file_content[1] \
            if len(file_content) == 2 else None
        if not self.ignore_remote_config and remote_config_flags:
            # Note: 'or', since config flags can come from toplevel
            #       (dataset-tree-root) as well as from dataset-level.
            #       toplevel is supposed flag the entire tree.
            self.remote_log_enabled = self.remote_log_enabled or \
                                      'l' in remote_config_flags

        return remote_version

    def get_store(self):
        """checks the remote end for an existing store and dataset

        Furthermore reads and stores version and config flags, layout
        locations, etc.
        If this doesn't raise, the remote end should be fine to work with.
        """
        # make sure the base path is a platform path when doing local IO
        # the incoming Path object is a PurePosixPath
        # XXX this else branch is wrong: Incoming is PurePosixPath
        # but it is subsequently assumed to be a platform path, by
        # get_layout_locations() etc. Hence it must be converted
        # to match the *remote* platform, not the local client

        # cache remote layout directories
        # PATCH: use the abstract `self.store_base_path` to calculate RIA-store
        # directory paths.
        self.remote_git_dir, self.remote_archive_dir, self.remote_obj_dir = \
            self.get_layout_locations(self.store_base_path, self.archive_id)

        read_only_msg = "Treating remote as read-only in order to " \
                        "prevent damage by putting things into an unknown " \
                        "version of the target layout. You can overrule this " \
                        "by setting 'annex.ora-remote.<name>.force-write=true'."
        try:
            self.verify_store()
        except UnknownLayoutVersion:
            reason = "Remote dataset tree reports version {}. Supported " \
                     "versions are: {}. Consider upgrading datalad or " \
                     "fix the 'ria-layout-version' file at the RIA store's " \
                     "root. ".format(self.remote_dataset_tree_version,
                                     self.known_versions_dst)
            self._set_read_only(reason + read_only_msg)
        except NoLayoutVersion:
            reason = "Remote doesn't report any dataset tree version. " \
                     "Consider upgrading datalad or add a fitting " \
                     "'ria-layout-version' file at the RIA store's " \
                     "root."
            self._set_read_only(reason + read_only_msg)

        try:
            self.verify_ds_in_store()
        except UnknownLayoutVersion:
            reason = "Remote object tree reports version {}. Supported" \
                     "versions are {}. Consider upgrading datalad or " \
                     "fix the 'ria-layout-version' file at the remote " \
                     "dataset root. " \
                     "".format(self.remote_object_tree_version,
                               self.known_versions_objt)
            self._set_read_only(reason + read_only_msg)
        except NoLayoutVersion:
            reason = "Remote doesn't report any object tree version. " \
                     "Consider upgrading datalad or add a fitting " \
                     "'ria-layout-version' file at the remote " \
                     "dataset root. "
            self._set_read_only(reason + read_only_msg)

    @handle_errors
    def initremote(self):
        self._verify_config(fail_noid=False)
        if not self.archive_id:
            self.archive_id = _get_datalad_id(self.gitdir)
            if not self.archive_id:
                # fall back on the UUID for the annex remote
                self.archive_id = self.annex.getuuid()

        self.get_store()

        self.annex.setconfig('archive-id', self.archive_id)
        # Make sure, we store the potentially rewritten URL. But only, if the
        # source was annex as opposed to a local config.
        if self.ria_store_url and self.ria_store_url_source == 'annex':
            self.annex.setconfig('url', self.ria_store_url)
        if self.ria_store_pushurl and self.ria_store_pushurl_source == 'annex':
            self.annex.setconfig('push-url', self.ria_store_pushurl)

    def _local_io(self):
        """Are we doing local operations?"""
        # let's not make this decision dependent on the existence
        # of a directory the matches the name of the configured
        # store tree base dir. Such a match could be pure
        # coincidence. Instead, let's do remote whenever there
        # is a remote host configured
        #return self.store_base_path.is_dir()

        # TODO: Isn't that wrong with HTTP anyway?
        #       + just isinstance(LocalIO)?
        # XXX isinstance(LocalIO) would not work, this method is used
        # before LocalIO is instantiated
        return not self.storage_host

    def _set_read_only(self, msg):

        if not self.force_write:
            self.read_only = True
            self.message(msg, type='info')
        else:
            self.message("Was instructed to force write", type='info')

    def _ensure_writeable(self):
        if self.read_only:
            raise RIARemoteError("Remote is treated as read-only. "
                                 "Set 'ora-remote.<name>.force-write=true' to "
                                 "overrule this.")
        if isinstance(self.push_io, HTTPRemoteIO):
            raise RIARemoteError("Write access via HTTP not implemented")

    @property
    def io(self):
        if not self._io:
            if self._local_io():
                self._io = LocalIO()
            elif self.ria_store_url.startswith("ria+http"):
                # TODO: That construction of "http(s)://host/" should probably
                #       be moved, so that we get that when we determine
                #       self.storage_host. In other words: Get the parsed URL
                #       instead and let HTTPRemoteIO + SSHRemoteIO deal with it
                #       uniformly. Also: Don't forget about a possible port.

                url_parts = self.ria_store_url[4:].split('/')
                # we expect parts: ("http(s):", "", host:port, path)
                self._io = HTTPRemoteIO(
                    url_parts[0] + "//" + url_parts[2],
                    self.buffer_size
                )
            elif self.storage_host:
                self._io = SSHRemoteIO(self.storage_host, self.buffer_size)
                from atexit import register
                register(self._io.close)
            else:
                raise RIARemoteError(
                    "Local object tree base path does not exist, and no SSH"
                    "host configuration found.")
        return self._io

    @property
    def push_io(self):
        # Instance of an IOBase subclass for execution based on configured
        # 'push-url' if such exists. Otherwise identical to `self.io`.
        # Note, that once we discover we need to use the push-url (that is on
        # TRANSFER_STORE and REMOVE), we should switch all operations to that IO
        # instance instead of using different connections for read and write
        # operations. Ultimately this is due to the design of annex' special
        # remote protocol - we don't know which annex command is running and
        # therefore we don't know whether to use fetch or push URL during
        # PREPARE.

        if not self._push_io:
            if self.ria_store_pushurl:
                self.message("switching ORA to push-url")
                # Not-implemented-push-HTTP is ruled out already when reading
                # push-url, so either local or SSH:
                if not self.storage_host_push:
                    # local operation
                    self._push_io = LocalIO()
                else:
                    self._push_io = SSHRemoteIO(self.storage_host_push,
                                                self.buffer_size)

                # We have a new instance. Kill the existing one and replace.
                from atexit import register, unregister
                if hasattr(self.io, 'close'):
                    unregister(self.io.close)
                    self.io.close()

                # XXX now also READ IO is done with the write IO
                # this explicitly ignores the remote config
                # that distinguishes READ from WRITE with different
                # methods
                self._io = self._push_io
                if hasattr(self.io, 'close'):
                    register(self.io.close)

                self.storage_host = self.storage_host_push
                self.store_base_path = self.store_base_path_push

                # delete/update cached locations:
                self._last_archive_path = None
                self._last_keypath = (None, None)

                self.remote_git_dir, \
                self.remote_archive_dir, \
                self.remote_obj_dir = \
                    self.get_layout_locations(
                        # PATCH: use abstract path to calculate RIA-store dirs
                        self.store_base_path,
                        self.archive_id
                    )

            else:
                # no push-url: use existing IO
                self._push_io = self._io

        return self._push_io

    @handle_errors
    def prepare(self):

        gitdir = self.annex.getgitdir()
        self._repo = AnnexRepo(gitdir)
        self._verify_config()

        self.get_store()

        # report active special remote configuration/status
        self.info = {
            'store_base_path': str(self.store_base_path),
            'storage_host': 'local'
            if self._local_io() else self.storage_host,
        }

        # TODO: following prob. needs hasattr instead:
        if not isinstance(self.io, HTTPRemoteIO):
            self.info['7z'] = ("not " if not self.io.get_7z() else "") + \
                              "available"

    @handle_errors
    def transfer_store(self, key, filename):
        self._ensure_writeable()

        # we need a file-system compatible name for the key
        key = _sanitize_key(key)

        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        key_path = dsobj_dir / key_path

        # PATCH: convert abstract path `key_path` to io-specific concrete path
        # and use that.
        transport_key_path = self.push_io.url2transport_path(key_path)
        if self.push_io.exists(transport_key_path):
            # if the key is here, we trust that the content is in sync
            # with the key
            return

        self.push_io.mkdir(transport_key_path.parent)

        # We need to copy to a temp location to let checkpresent fail while the
        # transfer is still in progress and furthermore not interfere with
        # administrative tasks in annex/objects.
        # In addition include uuid, to not interfere with parallel uploads from
        # different clones.
        transfer_dir = \
            self.remote_git_dir / "ora-remote-{}".format(self._repo.uuid) / "transfer"
        # PATCH: convert abstract path `transfer_dir` to io-specific concrete
        # path and use that
        transport_transfer_dir = self.push_io.url2transport_path(transfer_dir)
        self.push_io.mkdir(transport_transfer_dir)

        tmp_path = transfer_dir / key
        # PATCH: convert abstract path `transport_tmp_path` to io-specific
        # concrete path and use that
        transport_tmp_path = self.push_io.url2transport_path(tmp_path)
        try:
            self.push_io.put(filename, transport_tmp_path, self.annex.progress)
            # copy done, atomic rename to actual target
            self.push_io.rename(transport_tmp_path, transport_key_path)
        except Exception as e:
            # whatever went wrong, we don't want to leave the transfer location
            # blocked
            self.push_io.remove(transport_tmp_path)
            raise e

    @handle_errors
    def transfer_retrieve(self, key, filename):
        # we need a file-system compatible name for the key
        key = _sanitize_key(key)

        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        abs_key_path = dsobj_dir / key_path
        # PATCH: convert abstract path `abs_key_path` to io-specific
        # concrete path and use that
        transport_abs_key_path = self.io.url2transport_path(abs_key_path)

        # sadly we have no idea what type of source gave checkpresent->true
        # we can either repeat the checks, or just make two opportunistic
        # attempts (at most)
        try:
            self.io.get(transport_abs_key_path, filename, self.annex.progress)
        except Exception as e1:
            if isinstance(self.io, HTTPRemoteIO):
                # no client-side archive access over HTTP
                # Note: This is intentional, as it would mean one additional
                # request per key. However, server response to the GET can
                # consider archives on their end.
                raise
            # catch anything and keep it around for a potential re-raise
            try:
                # PATCH: convert abstract path `archive_path` to io-specific
                # concrete path and use that
                transport_archive_path = self.io.url2transport_path(
                    archive_path
                )
                self.io.get_from_archive(
                    transport_archive_path,
                    key_path,
                    filename,
                    self.annex.progress
                )
            except Exception as e2:
                # TODO properly report the causes
                raise RIARemoteError('Failed to obtain key: {}'
                                     ''.format([str(e1), str(e2)]))

    @handle_errors
    def checkpresent(self, key):
        # we need a file-system compatible name for the key
        key = _sanitize_key(key)

        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        abs_key_path = dsobj_dir / key_path
        # PATCH: convert abstract path `abs_key_path` to io-specific concrete
        # path and use that
        transport_abs_key_path = self.io.url2transport_path(abs_key_path)
        if self.io.exists(transport_abs_key_path):
            # we have an actual file for this key
            return True
        if isinstance(self.io, HTTPRemoteIO):
            # no client-side archive access over HTTP
            return False
        # do not make a careful check whether an archive exists, because at
        # present this requires an additional SSH call for remote operations
        # which may be rather slow. Instead just try to run 7z on it and let
        # it fail if no archive is around
        # TODO honor future 'archive-mode' flag
        # PATCH: convert abstract path `archive_path` to io-specific concrete
        # path and use that
        transport_archive_path = self.io.url2transport_path(archive_path)
        return self.io.in_archive(transport_archive_path, key_path)

    @handle_errors
    def remove(self, key):
        # we need a file-system compatible name for the key
        key = _sanitize_key(key)

        self._ensure_writeable()

        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        key_path = dsobj_dir / key_path
        # PATCH: convert abstract path `key_path` to io-specific concrete path
        # and use that
        transport_key_path = self.push_io.url2transport_path(key_path)
        if self.push_io.exists(transport_key_path):
            self.push_io.remove(transport_key_path)
        key_dir = key_path
        # remove at most two levels of empty directories
        for level in range(2):
            key_dir = key_dir.parent
            try:
                # PATCH: convert abstract path `key_dir` to io-specific concrete
                # path and use that
                transport_key_dir = self.push_io.url2transport_path(key_dir)
                self.push_io.remove_dir(transport_key_dir)
            except Exception:
                break

    @handle_errors
    def getcost(self):
        # 100 is cheap, 200 is expensive (all relative to Config/Cost.hs)
        # 100/200 are the defaults for local and remote operations in
        # git-annex
        # if we have the object tree locally, operations are cheap (100)
        # otherwise expensive (200)
        return '100' if self._local_io() else '200'

    @handle_errors
    def whereis(self, key):
        # we need a file-system compatible name for the key
        key = _sanitize_key(key)

        dsobj_dir, archive_path, key_path = self._get_obj_location(key)
        if isinstance(self.io, HTTPRemoteIO):
            # display the URL for a request
            # TODO: method of HTTPRemoteIO
            # in case of a HTTP remote (unchecked for others), storage_host
            # is not just a host, but a full URL without a path
            return f'{self.storage_host}{dsobj_dir}/{key_path}'

        return str(dsobj_dir / key_path) if self._local_io() \
            else '{}: {}:{}'.format(
                self.storage_host,
                self.remote_git_dir,
                sh_quote(str(key_path)),
        )

    @staticmethod
    def get_layout_locations(base_path, dsid):
        # PATCH: type of `base_path` is `PurePosixPath`
        ORARemote._assert_pure_posix_path(base_path)
        return get_layout_locations(1, base_path, dsid)

    def _get_obj_location(self, key):
        # Notes: - Changes to this method may require an update of
        #          ORARemote._layout_version
        #        - archive_path is always the same ATM. However, it might depend
        #          on `key` in the future. Therefore build the actual filename
        #          for the archive herein as opposed to `get_layout_locations`.

        if not self._last_archive_path:
            # PATCH: type of `base_path` is `PurePosixPath`
            self._assert_pure_posix_path(self.remote_archive_dir)
            self._last_archive_path = self.remote_archive_dir / 'archive.7z'

        if self._last_keypath[0] != key:
            if self.remote_object_tree_version == '1':
                # PATCH: dir-hashes are always in platform format. We convert it
                # to a platform-specific `Path` and then to `PurePosixPath`.
                key_dir = PurePosixPath(Path(self.annex.dirhash_lower(key)))

            # If we didn't recognize the remote layout version, we set to
            # read-only and promised to at least try and read according to our
            # current version. So, treat that case as if remote version was our
            # (client's) version.
            else:
                # PATCH: dir-hashes are always in platform format. We convert it
                # to a platform-specific `Path` and then to `PurePosixPath`.
                key_dir = PurePosixPath(Path(self.annex.dirhash(key)))
            # double 'key' is not a mistake, but needed to achieve the exact
            # same layout as the annex/objects tree
            # PATCH: use the abstract `key_dir` path
            self._last_keypath = (key, key_dir / key / key)

        self._assert_pure_posix_path(self.remote_obj_dir)
        return self.remote_obj_dir, self._last_archive_path, \
            self._last_keypath[1]


apply_patch(
    'datalad.distributed.ora_remote',
    None,
    'ORARemote',
    ORARemote,
)
