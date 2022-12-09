#!/usr/bin/env python
## emacs: -*- mode: python; py-indent-offset: 4; tab-width: 4; indent-tabs-mode: nil -*-
# ex: set sts=4 ts=4 sw=4 noet:
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
#
#   See LICENSE file distributed along with the datalad package for the
#   copyright and license terms.
#
# ## ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ### ##
"""git-remote-datalad-annex to fetch/push via any git-annex special remote

In essence, this Git remote helper bootstraps a utility repository in order
to push/fetch the state of a repository to any location accessible by any
git-annex special remote implementation. All information necessary for this
bootstrapping is taken from the remote URL specification. The internal utility
repository is removed again after every invocation. Therefore changes to the
remote access configuration can be made any time by simply modifying the
configured remote URL.

When installed, this remote helper is invoked for any "URLs" that start with
the prefix ``datalad-annex::``. Following this prefix, two types of
specifications are support.

1. Plain parameters list::

    datalad-annex::?type=<special-remote-type>&[...][exporttree=yes]

   In this case the prefix is followed by a URL query string that comprises all
   necessary (and optional) parameters that would be normally given to the
   ``git annex initremote`` command. It is required to specify the special
   remote ``type``, and it is possible to request "export" mode for any special
   remote that supports it. Depending on the chosen special remote additional
   parameters may be required or supported. Please consult the git-annex
   documentation at https://git-annex.branchable.com/special_remotes/

2. URL::

    datalad-annex::<url>[?...]

   Alternatively, an actual URL can be given after the prefix. In this case,
   the, now optional, URL query string can still be used to specify arbitrary
   parameters for special remote initialization. In addition, the query string
   specification can use Python-format-style placeholder to reference
   particular URL components as parameters values, in order to avoid
   double-specification.

   The list of supported placeholders is ``scheme``, ``netloc``, ``path``,
   ``fragment``, ``username``, ``password``, ``hostname``, ``port``,
   corresponding to the respective URL components. In addition, a ``noquery``
   placeholder is supported, which resolves to the entire URL except any
   query string. An example of such a URL specification is::

    datalad-annex::file:///tmp/example?type=directory&directory={path}&encryption=none'

   which would initialize a ``type=directory`` special remote pointing
   at ``/tmp/example``.


Caution with collaborative workflows

There is no protection against simultaneous, conflicting repository state
uploads from two different locations! Similar to git-annex's "export"
feature, this feature is most appropriately used as a dataset deposition
mechanism, where uploads are conducted from a single site only -- deposited
for consumption by any number of parties.

If this Git remote helper is to be used for multi-way collaboration, with two
or more parties contributing updates, it is advisable to employ a separate
``datalad-annex::`` target site for each contributor, such that only one site
is pushing to any given location. Updates are exchanged by the remaining
contributors adding the respective other ``datalad-annex::`` sites as
additional Git remotes, analog to forks of a repository.


Special remote type support

In addition to the regular list of special remotes, plain http(s) access
via URLs is also supported via the 'web' special remote. For such cases,
only the base URL and the 'type=web' parameter needs to be given, e.g::

    git clone 'datalad-annex::https://example.com?type=web&url={noquery}'

When a plain URL is given, with no parameter specification in a query
string, the parameters ``type=web`` and ``exporttree=yes`` are added
automatically by default. This means that this remote helper can clone
from any remote deposit accessible via ``http(s)`` that matches the layout
depicted in the next section.


Remote layout

The representation of a repository at a remote depends on the chosen type of
special remote. In general, two files will be deposited. One text file
containing a list of Git ``refs`` contained in the deposit, and one ZIP file
with a (compressed) archive of a bare Git repository. Beside the idiosyncrasies
of particular special remotes, to major modes determine the layout of a remote
deposit. In "normal" mode, two annex keys (``XDLRA--refs``,
``XDLRA--repo-export``) will be deposited. In "export" mode, a directory tree is
created that is designed to blend with arbitrary repository content, such that
a git remote and a git-annex export can be pushed to the same location without
conflicting with each other. The aforementioned files will be represented like
this::

    .datalad
    └── dotgit  # named to not be confused with an actual Git repository
        ├── refs
        └── repo.zip

The default LZMA-compression of the ZIP file (in both export and normal mode)
can be turned off with the ``dladotgit=uncompressed`` URL parameter.


Credential handling

Some git-annex special remotes require the specification of credentials via
environment variables. With the URL parameter ``dlacredential=<name>`` it
is possible to query DataLad for a user/password credential to be used for
this purpose. This convenience functionality is supported for the special
remotes ``glacier``, ``s3``, and ``webdav``.

When a credential of the given name does not exist, or no credential name
was specified, an attempt is made to determine a suitable credential based
on, for example, a detected HTTP authentication realm. If no matching
credential could be found, the user will be prompted to enter a credential.
After having successfully established access, the entered credential will
be saved in the local credential store.

DataLad-based credentials are only utilized, when the native git-annex
credential setup via environment variables is not in use (see the documentation
of a particular special remote implementation for more information).


Implementation details

This Git remote implementation uses *two* extra repositories, besides the
repository (R) it is used with, to do its work:

(A) A tiny repository that is entirely bootstrapped from the remote URL,
    and is used to retrieve/deposit a complete state of the actual repo
    an a remote site, via a git-annex special remote setup.

(B) A local, fully functional mirror repo of the remotely stored
    repository state.

On fetch/push the existence of both additional repositories is ensured. The
remote state of retrieved via repo (A), and unpacked to repo (B).  The actual
fetch/push Git operations are performed locally between the repo (R) and
repo (B). On push, repo (B) is then packed up again, and deposited on the
remote site via git-annex transfer in repo (A).

Due to a limitation of this implementation, it is possible that when the last
upload step fails, Git nevertheless advances the pushed refs, making it appear
as if the push was completely successful. That being said, Git will still issue
a message (``error: failed to push some refs to..``) and the git-push process
will also exit with a non-zero status. In addition, all of the remote's refs
will be annotated with an additional ref named
``refs/dlra-upload-failed/<remote-name>/<ref-name>`` to indicate the upload
failure. These markers will be automatically removed after the next successful
upload.

.. note::

   Confirmed to work with git-annex version 8.20211123 onwards.

.. todo::

   - At the moment, only one format for repository deposition is supported
     (a ZIP archive of a working bare repository). However this is not
     a good format for the purpose of long-term archiving, because it
     require a functional Git installation to work with. It would be fairly
     doable to make the deposited format configurable, and support additional
     formats. An interesting one would be a fast-export stream, basically
     a plain text serialization of an entire repository.
   - recognize that a different repo is being pushed over an existing
     one at the remote
   - think about adding additional information into the header of `refs`
     maybe give it some kind of stamp that also makes it easier to validate
     by the XDLRA backend
   - think about preventing duplication between the repo and its local
     mirror could they safely share git objects? If so, in which direction?
"""


__all__ = ['RepoAnnexGitRemote']

import datetime
import logging
import os
import sys
import zipfile
from pathlib import Path
from shutil import make_archive
from unittest.mock import patch
from urllib.parse import (
    unquote,
    urlparse,
)

from datalad.core.local.repo import repo_from_path
from datalad.runner import (
    NoCapture,
    StdOutCapture,
)
from datalad_next.datasets import (
    LegacyAnnexRepo as AnnexRepo,
    LegacyGitRepo as GitRepo,
)
from datalad_next.exceptions import (
    CapturedException,
    CommandError,
)
from datalad_next.constraints import EnsureInt
from datalad_next.uis import ui_switcher as ui
from datalad_next.utils import (
    external_versions,
    on_windows,
    rmtree,
)

from datalad_next.utils import (
    CredentialManager,
    get_specialremote_credential_envpatch,
    get_specialremote_credential_properties,
    needs_specialremote_credential_envpatch,
    specialremote_credential_envmap,
    update_specialremote_credential,
)
from datalad_next.utils.consts import PRE_INIT_COMMIT_SHA

lgr = logging.getLogger('datalad.gitremote.datalad_annex')


class RepoAnnexGitRemote(object):
    """git-remote-helper implementation

    ``communicate()`` is the entrypoint.
    """
    # hard code relevant keynames for the XDLRA backend
    # this will always have the refs list
    refs_key = 'XDLRA--refs'
    # this will have the repository archive
    repo_export_key = 'XDLRA--repo-export'
    xdlra_key_locations = {
        refs_key: dict(
            prefix='3f7/4a3', loc='.datalad/dotgit/refs'),
        repo_export_key: dict(
            prefix='eb3/ca0', loc='.datalad/dotgit/repo.zip'),
    }
    # all top-level content in a repo archive
    # this is used as a positive-filter when extracting downloaded
    # archives (to avoid writing to undesirable locations from
    # high-jacked archives)
    safe_content = [
        'branches', 'hooks', 'info', 'objects', 'refs', 'config',
        'packed-refs', 'description', 'HEAD',
    ]
    # define all supported options, including their type-checker
    support_githelper_options = {
        'verbosity': EnsureInt(),
    }
    # supported parameters that can come in via the URL, but must not
    # be relayed to `git annex initremote`
    internal_parameters = ('dladotgit=uncompressed', 'dlacredential=')

    def __init__(self,
                 gitdir,
                 remote,
                 url,
                 instream=sys.stdin,
                 outstream=sys.stdout,
                 errstream=sys.stderr):
        """
        Parameters
        ----------
        gitdir : str
          Path to the GITDIR of the repository to operate on (provided
          by Git).
        remote : str
          Remote label to use (provided by Git).
        url : str
          URL of the remote (provided by Git).
        instream :
          Stream to read communication from Git from.
        outstream :
          Stream to communicate outcomes to Git.
        errstream :
          Stream for logging.
        """
        self.repo = GitRepo(gitdir)
        # this is the key piece, take special remote params from
        # URL
        # this function yields a type= parameter in any case
        self.initremote_params = get_initremote_params_from_url(url)
        self.remote_name = remote
        # internal logic relies on workdir to be an absolute path
        self.workdir = Path(gitdir, 'dl-repoannex', remote).resolve()
        self._repoannexdir = self.workdir / 'repoannex'
        if self._repoannexdir.exists():
            # whatever existed here before is an undesirable
            # leftover of a previous crash
            rmtree(str(self._repoannexdir), ignore_errors=True)
        self._repoannex = None
        self._mirrorrepodir = self.workdir / 'mirrorrepo'
        self._mirrorrepo = None

        # cache for remote refs, to avoid repeated queries
        self._cached_remote_refs = None

        self.instream = instream
        self.outstream = outstream
        self.errstream = errstream

        # options communicated by Git
        # https://www.git-scm.com/docs/gitremote-helpers#_options
        self.options = {}

        # ID of the tree to export, if needed
        self.exporttree = None

        self.credman = None
        self.pending_credential = None
        # must come after the two above!
        self.credential_env = self._get_credential_env()

        annex_version = external_versions['cmd:annex']
        if annex_version < '8.20211123':
            self.log(
                f'git-annex version {annex_version} is unsupported, '
                'please upgrade',
                level=1
            )

    def _get_credential_env(self):
        """
        Returns
        -------
        dict or None
          A dict with all required items to patch the environment, or None
          if not enough information is available.

        Raises
        ------
        ValueError
          If a credential retrieval is requested for an unsupported special
          remote type.
        """
        credential_name = [
            p[14:] for p in self.initremote_params
            if p.startswith('dlacredential=')
        ] or None
        if credential_name:
            credential_name = credential_name[0]
        remote_type = self._get_remote_type()
        supported_remote_type = remote_type in specialremote_credential_envmap
        if credential_name and not supported_remote_type:
            # we have no idea how to deploy credentials for this remote type
            raise ValueError(
                f"Deploying credentials for type={remote_type[0]} special "
                "remote is not supported. Remove dlacredential= parameter from "
                "the remote URL and provide credentials according to the "
                "documentation of this particular special remote.")

        if not needs_specialremote_credential_envpatch(remote_type):
            return

        cred = self._retrieve_credential(credential_name)

        if not cred:
            lgr.debug(
                'Could not find a matching credential for special remote %s',
                self.initremote_params)
            return

        return get_specialremote_credential_envpatch(remote_type, cred)

    def _retrieve_credential(self, name):
        """Retrieve a credential

        Successfully retrieved credentials are also placed in
        self.pending_credential to be picked up by `_store_credential()`.

        Returns
        -------
        dict or None
          If a credential could be retrieved, a dict with 'user' and
          'secret' keys will be return, or None otherwise.
        """
        if not self.credman:
            self.credman = CredentialManager(self.repo.config)
        cred = None
        credprops = {}
        if name:
            # we can ask blindly first, caller seems to know what to do
            cred = self.credman.get(
                name=name,
                # give to make legacy credentials accessible
                _type_hint='user_password',
            )
        if not cred:
            # direct lookup failed, try query.
            credprops = get_specialremote_credential_properties(
                self.initremote_params) or {}
            if credprops:
                creds = self.credman.query(_sortby='last-used', **credprops)
                if creds:
                    name, cred = creds[0]
        if not cred:
            # credential query failed too, enable manual entry
            credprops['type'] = 'user_password'
            cred = self.credman.get(
                # this might still be None
                name=name,
                _type_hint='user_password',
                _prompt=f'A credential is required for access',
                # inject anything we already know to make sure we store it
                # at the very end, and can use it for discovery next time
                **credprops
            )

        if not cred:
            return
        # stage for eventual (re-)storage after having proven to work
        self.pending_credential = (name, cred)
        return {k: cred[k] for k in ('user', 'secret')}

    def _get_remote_type(self):
        remote_type = [
            p[5:] for p in self.initremote_params
            if p.startswith('type=')
        ]
        if not remote_type:
            return
        return remote_type[0]

    def _store_credential(self):
        """Look for a pending credential and store it

        Safe to call unconditionally.
        """
        if self.pending_credential and self.credman:
            name, cred = self.pending_credential
            update_specialremote_credential(
                self._get_remote_type(),
                self.credman,
                name,
                cred,
                credtype_hint='user_password',
                duplicate_hint=
                'Specify a credential name via the dlacredential= '
                'remote URL parameter, and/or configure a credential '
                'with the datalad-credentials command{}'.format(
                    f' with a `realm={cred["realm"]}` property'
                    if 'realm' in cred else ''),
            )

    def _ensure_workdir(self):
        self.workdir.mkdir(parents=True, exist_ok=True)

    @property
    def repoannex(self):
        """Repo annex repository

        If accessed when there is no repo annex, as new one is created
        automatically. It is bootstrapped entirely from the parameters
        encoded in the remote URL.

        Returns
        -------
        AnnexRepo
          This is always an annex repository. It is configured with
          a single special remote, parameterized from the Git repo URL.

        Raises
        ------
        CommandError
        ValueError
        """
        if self._repoannex:
            return self._repoannex

        self._ensure_workdir()
        try:
            # check if there is one already, would only be due to a prior
            # RUD (rapid unscheduled disassembly)
            ra = repo_from_path(self._repoannexdir)
        except ValueError:
            # funny dance to get to a bare annexrepo
            ra = GitRepo(
                self._repoannexdir,
                create=not GitRepo.is_valid(self._repoannexdir),
                bare=True,
            )
        try:
            # send annex into private mode, if supported
            # this repo will never ever be shared
            ra.call_git(['config', 'annex.private', 'true'])
            ra.call_git(['annex', 'init'])
            ra = AnnexRepo(self._repoannexdir)
            if 'type=web' in self.initremote_params:
                self._init_repoannex_type_web(ra)
            else:
                # let git-annex-initremote take over
                with patch.dict('os.environ', self.credential_env or {}):
                    ra.call_annex(
                        ['initremote', 'origin'] + [
                            p for p in self.initremote_params
                            if not any(p.startswith(ip)
                                       for ip in self.internal_parameters)
                        ])
                # make the new remote config known in the repo instance
                ra.config.reload()
            if 'exporttree=yes' in self.initremote_params:
                # conflicts with type=web, but we checked that above already.
                # plant the to-be-exported tree, still leaving the underlying
                # keys unfulfilled
                self.exporttree = make_export_tree(ra)

        except (CommandError, ValueError):
            # something blew up. clean up and blow again
            rmtree(ra.path, ignore_errors=True)
            raise

        self._repoannex = ra
        return ra

    def _init_repoannex_type_web(self, repoannex):
        """Uses registerurl to utilize the omnipresent type=web remote

        Raises
        ------
        ValueError
          When there is no `url=` parameter or when there are other
          parameters than the additional `type=web` and `exporttree=yes`,
          indicating an unsupported setup.
        """
        # for type=web we have to add URLs by hand
        baseurl = [
            v for v in self.initremote_params
            if v.startswith('url=')]
        if not len(baseurl) == 1:
            raise ValueError(
                "'web'-type remote requires 'url' parameter")
        # validate the rest of the params, essentially there
        # must not be any other
        if not all(p in ('type=web', 'exporttree=yes') or p.startswith('url=')
                   for p in self.initremote_params):
            raise ValueError(
                "'web'-type remote only supports 'url' "
                "and 'exporttree' parameters")
        baseurl = baseurl[0][4:]
        for key, kinfo in self.xdlra_key_locations.items():
            repoannex.call_annex([
                'registerurl',
                key,
                f'{baseurl}/{kinfo["loc"]}'
                if 'exporttree=yes' in self.initremote_params else
                f'{baseurl}/{kinfo["prefix"]}/{key}/{key}'
            ])

    @property
    def mirrorrepo(self):
        """Local remote mirror repository

        If accessed when there is no local mirror repo, as new one
        is created automatically, either from the remote state (if there is
        any), or an empty one.

        Returns
        -------
        GitRepo
          This is always only a plain Git repository (bare).
        """
        if self._mirrorrepo:
            return self._mirrorrepo

        # ensure we have a mirror repo, either fresh or existing
        self._ensure_workdir()
        if not self.get_remote_refs():
            existing_repo = False
            # there is nothing at the remote, hence we must wipe
            # out the local state, whatever it was to make git
            # report subsequent pushes properly, and prevent
            # "impossible" fetches
            if self._mirrorrepodir.exists():
                # if we extract, we cannot tollerate left-overs
                rmtree(str(self._mirrorrepodir), ignore_errors=True)
                # null the repohandle to be reconstructed later on-demand
                self._mirrorrepo = None
        elif GitRepo.is_valid(self._mirrorrepodir):
            # so we have remote refs and we also have a local mirror
            # create an instance, assume it is set up how we need it
            # must also have bare=True, or the newly created one below
            # will inherit the config
            # https://github.com/datalad/datalad/issues/6347
            mr = GitRepo(self._mirrorrepodir, bare=True)
            # make sure any recursion back in here is prevented
            self._mirrorrepo = mr
            # this will trigger a download if possible (remote has refs)
            self.replace_mirrorrepo_from_remote_deposit_if_needed()
            # reevaluate
            existing_repo = GitRepo.is_valid(self._mirrorrepodir)
        else:
            # we have nothing local, pull from the remote, because it
            # reports stuff to exist
            self.replace_mirrorrepo_from_remote_deposit()
            existing_repo = True

        # (re-)create an instance
        mr = GitRepo(
            self._mirrorrepodir,
            # if the remote had no refs, there would still be no repo
            create=not existing_repo,
            bare=True)

        self.log('Established mirror')
        self._mirrorrepo = mr
        return mr

    def log(self, *args, level=2):
        """Send log messages to the errstream"""
        # A value of 0 for <n> means that processes operate quietly,
        # and the helper produces only error output.
        # 1 is the default level of verbosity,
        # and higher values of <n> correspond to the number of -v flags
        # passed on the command line
        if self.options.get('verbosity', 1) >= level:
            print('[DATALAD-ANNEX]', *args, file=self.errstream)

    def send(self, msg):
        """Communicate with Git"""
        print(msg, end='', file=self.outstream, flush=True)

    def communicate(self):
        """Implement the necessary pieces of the git-remote-helper protocol

        Uses the input, output and error streams configured for the
        class instance.
        """
        self.log('Git remote startup: '
                 f'{self.remote_name} [{self.initremote_params}]')
        for line in self.instream:
            self.log(f'Received Git remote command: {repr(line)}', level=4)
            if line == '\n':
                # orderly exit command
                return
            elif line == 'capabilities\n':
                self.send(
                    'option\n'
                    'list\n'
                    'connect\n'
                    '\n'
                )
            elif line == 'connect git-receive-pack\n':
                self.log('Connecting git-receive-pack\n')
                self.send('\n')
                # we assume the mirror repo is in-sync with the remote at
                # this point
                pre_refs = sorted(self.mirrorrepo.for_each_ref_(),
                                  key=lambda x: x['refname'])
                # must not capture -- git is talking to it directly from here
                self.mirrorrepo._git_runner.run(
                    ['git', 'receive-pack', self.mirrorrepo.path],
                    protocol=NoCapture,
                )
                post_refs = sorted(self.mirrorrepo.for_each_ref_(),
                                   key=lambda x: x['refname'])
                if pre_refs != post_refs \
                        or (post_refs != self.get_remote_refs()):
                    # there was a change in the refs of the mirror repo
                    # OR
                    # the mirror is out-of-sync with the remote (could be a
                    # slightly more expensive test)
                    # we must upload it.
                    try:
                        self.replace_remote_deposit_from_mirrorrepo()
                    except Exception:
                        # the bad thing is that we have no way of properly
                        # signaling to git that this happended,
                        # the refs for this remote will look as if the upload
                        # was successfull

                        # we do not need to roll-back the refs in the
                        # mirrorrepo as it will be rsync'ed to the remote on
                        # next access
                        self.log('Remote update failed, flagging refs',
                                 post_refs)
                        for ref in post_refs:
                            # best MIH can think of is to leave behind another
                            # ref to indicate the unsuccessful upload
                            self.repo.call_git([
                                'update-ref',
                                # strip 'refs/heads/' from refname
                                f'refs/dlra-upload-failed/{self.remote_name}/'
                                f'{ref["refname"][11:]}',
                                ref['objectname']])
                        raise

                # clean-up potential upload failure markers for this particular
                # remote. whatever has failed before, we just uploaded a mirror
                # that was freshly sync'ed with the remote state before
                for ref in self.repo.for_each_ref_(
                        fields=('refname',),
                        pattern=f'refs/dlra-upload-failed/{self.remote_name}'):
                    self.repo.call_git(['update-ref', '-d', ref['refname']])
                # we do not need to update `self._cached_remote_refs`,
                # because we end the remote-helper process here
                # everything has worked, if we used a credential, update it
                self._store_credential()
                return
            elif line == 'connect git-upload-pack\n':
                self.log('Connecting git-upload-pack\n')
                self.send('\n')
                # must not capture -- git is talking to it directly from here.
                # the `self.mirrorrepo` access will ensure that the mirror
                # is uptodate
                self.mirrorrepo._git_runner.run(
                    ['git', 'upload-pack', self.mirrorrepo.path],
                    protocol=NoCapture,
                )
                # everything has worked, if we used a credential, update it
                self._store_credential()
                return
            elif line.startswith('option '):
                key, value = line[7:].split(' ', maxsplit=1)
                if key not in self.support_githelper_options:
                    self.send('unsupported\n')
                else:
                    try:
                        self.options[key] = \
                            self.support_githelper_options[key](
                                value.rstrip('\n'))
                        self.send('ok\n')
                    except ValueError as e:
                        # ensure no-multiline message
                        excstr = str(e).replace('\n', '\\n')
                        # git may not communicate reason for error, do log
                        self.log(
                            f'Type-checking of "{line[:-1]}" failed: {excstr}')
                        self.send(f'error {excstr}\n')
            else:
                self.log('UNKNOWN COMMAND', line)
                # unrecoverable error
                return

    def replace_remote_deposit_from_mirrorrepo(self):
        """Package the local mirrorrepo up, and copy to the special remote

        The mirror is assumed to be ready/complete. It will be cleaned with
        `gc` to minimize the upload size. The mirrorrepo is then compressed
        into an LZMA ZIP archive, and a separate refs list for it is created
        in addition. Both are then copied to the special remote.
        """
        self.log('Replace remote from mirror')
        mirrorrepo = self.mirrorrepo
        repoannex = self.repoannex

        # trim it down, as much as possible
        mirrorrepo.call_git(['gc'])

        # update the repo state keys
        # it is critical to drop the local keys first, otherwise
        # `setkey` below will not replace them with new content
        # however, git-annex fails to do so in some edge cases
        # https://git-annex.branchable.com/bugs/Fails_to_drop_key_on_windows___40__Access_denied__41__/?updated
        # no regular `drop` works, nor does `dropkeys`
        #self.log(repoannex.call_annex(['drop', '--force', '--all']))
        # nuclear option remains, luckily possible in this utility repo
        if on_windows:
            objdir = self.repoannex.dot_git / 'annex' / 'objects'
            if objdir.exists():
                rmtree(str(objdir), ignore_errors=True)
                objdir.mkdir()
        else:
            # more surgical for the rest
            self.log(repoannex.call_annex([
                'dropkey', '--force', self.refs_key, self.repo_export_key]))

        # use our zipfile wrapper to get an LZMA compressed archive
        # via the shutil convenience layer
        with patch('zipfile.ZipFile',
                   UncompressedZipFile
                   if 'dladotgit=uncompressed' in self.initremote_params
                   else LZMAZipFile):
            # TODO exclude hooks (the mirror is always plain-git),
            # would we ever need any
            archive_file = make_archive(
                str(self.workdir / 'repoarchive'),
                'zip',
                root_dir=str(mirrorrepo.path),
                base_dir=os.curdir,
            )
            # hand over archive to annex
            repoannex.call_annex([
                'setkey',
                self.repo_export_key,
                archive_file
            ])
        # generate a list of refs
        # write to file
        refs_file = self.workdir / 'reporefs'
        refs_file.write_text(_format_refs(mirrorrepo))
        self.log(refs_file.read_text())
        # hand over reflist to annex
        self.log(repoannex.call_annex([
            'setkey',
            self.refs_key,
            str(refs_file),
        ]))
        if 'exporttree=yes' in self.initremote_params:
            # we want to "force" an export, because the content of our
            # keys can change, but this is not possible.
            # we cheat be exporting "nothing" (an empty tree) first,
            # and then reexport
            try:
                self.log(repoannex.call_annex(
                    ['export', PRE_INIT_COMMIT_SHA, '--to=origin']))
            except Exception as e:
                # some remotes will error out if we unexport something that
                # wasn't actually exported (e.g. webdav)
                CapturedException(e)
                pass
            self.log(repoannex.call_annex(
                ['export', self.exporttree, '--to=origin']))
        else:
            # it is critical to drop the keys from the remote first, otherwise
            # `copy` below will fail to replace their content
            self.log(repoannex.call_annex(
                ['drop', '--force', '-f', 'origin', '--all']))
            self.log(repoannex.call_annex(
                ['copy', '--fast', '--to', 'origin', '--all']))
        # update remote refs from local ones
        # we just updated the remote from local
        self._cached_remote_refs = self.get_mirror_refs()

    def replace_mirrorrepo_from_remote_deposit_if_needed(self):
        """Replace the mirror if the remote has refs and they differ

        Parameters
        ----------
        mirror_refs: str, optional
          If given, must be formatted like get_mirror_refs() would do.
        """
        self.log("Check if mirror needs to be replaced with remote state")
        remote_refs = self.get_remote_refs()
        mirror_refs = self.get_mirror_refs()
        if remote_refs and remote_refs != mirror_refs:
            self.log(repr(remote_refs), repr(mirror_refs))
            # we must replace the local mirror with the
            # state of the remote
            self.replace_mirrorrepo_from_remote_deposit()
        return remote_refs, mirror_refs

    def replace_mirrorrepo_from_remote_deposit(self):
        """Replaces the local mirror repo with one obtained from the remote

        This method assumes that the remote does have one. This should be
        checked by inspecting `get_remote_refs()` before calling this method.
        """
        self.log('Set mirror to remote state')
        ra = self.repoannex
        # we have to get the key with the repo archive
        # because the local repoannex is likely a freshly bootstrapped one
        # without any remote awareness, claim that the remote has this key
        sremotes = ra.get_special_remotes()
        if len(sremotes) == 1:
            # in case of the 'web' special remote, we have no actual special
            # remote, but URLs for the two individual keys
            ra.call_annex(['setpresentkey', self.repo_export_key,
                           sremotes.popitem()[0], '1'])
        # drop locally to ensure re-downlad, the keyname never changes,
        # even when the content does
        self.log(
            ra.call_annex([
                'drop', '--force',
                '--key', self.repo_export_key])
        )
        # download the repo archive
        self.log(
            ra.call_annex(['get', '--key', self.repo_export_key])
        )
        # locate it in the local annex, use annex function to do this in order
        # to cope with any peculiar repo setups we might face across platforms
        repoexportkeyloc = ra.call_annex_oneline([
            'contentlocation', self.repo_export_key])
        repoexportkeyloc = ra.dot_git / repoexportkeyloc

        if self._mirrorrepodir.exists():
            # if we extract, we cannot tollerate left-overs
            rmtree(str(self._mirrorrepodir), ignore_errors=True)
            # null the repohandle to be reconstructed later on-demand
            self._mirrorrepo = None

        self.log('Extracting repository archive')
        with zipfile.ZipFile(repoexportkeyloc) as zip:
            zip.extractall(
                self._mirrorrepodir,
                # a bit of a safety-net, exclude all unexpected content
                members=[
                    m for m in zip.namelist()
                    if any(m.startswith(prefix)
                           for prefix in self.safe_content)],
            )

    def get_remote_refs(self):
        """Report remote refs

        The underlying special remote is asked whether it has the key
        containing the refs list for the remote. If it does, it is retrieved
        and reported.

        Returns
        -------
        str or None
          If the remote has refs, they are returned as a string, formatted like
          a refs file in a Git directory. Otherwise, `None` is returned.
        """
        if self._cached_remote_refs:
            # this process already queried them once, return cache
            return self._cached_remote_refs

        self.log("Get refs from remote")
        ra = self.repoannex

        # in case of the 'web' special remote, we have no actual special
        # remote, but URLs for the two individual keys
        sremotes = ra.get_special_remotes()
        # if we do not have a special remote reported, fall back on
        # possibly recorded URLs for the XDLRA--refs key
        sremote_id = sremotes.popitem()[0] if sremotes else 'web'

        # we want to get the latest refs from the remote under all
        # circumstances, and transferkey will not attempt a download for
        # a key that is already present locally -> drop first
        ra.call_annex([
            'drop', '--force', '--key', self.refs_key])
        # now get the key from the determined remote
        try:
            ra.call_annex([
                'transferkey', self.refs_key, f'--from={sremote_id}'])
        except CommandError as e:
            CapturedException(e)
            self.log("Remote appears to have no refs")
            # download failed, we have no refs
            return

        refskeyloc = ra.call_annex_oneline([
            'contentlocation', self.refs_key])
        # read, cache, return
        refs = (ra.dot_git / refskeyloc).read_text()
        self._cached_remote_refs = refs
        return refs

    def get_mirror_refs(self):
        """Return the refs of the current mirror repo

        Returns
        -------
        str
        """
        self.log("Get refs from mirror")
        return _format_refs(self.mirrorrepo)


# TODO propose as addition to AnnexRepo
# https://github.com/datalad/datalad/issues/6316
def call_annex_success(self, args, files=None):
    """Call git-annex and return true if the call exit code of 0.

    All parameters match those described for `call_annex`.

    Returns
    -------
    bool
    """
    try:
        self.call_annex(args, files)
    except CommandError:
        return False
    return True


class LZMAZipFile(zipfile.ZipFile):
    """Tiny wrapper to monkey-patch zipfile in order to have
    shutil.make_archive produce an LZMA-compressed ZIP"""
    def __init__(self, *args, **kwargs):
        kwargs.pop('compression', None)
        return super().__init__(
            *args, compression=zipfile.ZIP_LZMA, **kwargs)


class UncompressedZipFile(zipfile.ZipFile):
    """Tiny wrapper to monkey-patch zipfile in order to have
    shutil.make_archive produce an uncompressed ZIP"""
    def __init__(self, *args, **kwargs):
        kwargs.pop('compression', None)
        return super().__init__(
            *args, compression=zipfile.ZIP_STORED, **kwargs)


def _format_refs(repo, refs=None):
    """Helper to format a standard refs list from for_each_ref() output

    Parameters
    ----------
    repo: GitRepo
      Repo which to query for the 'HEAD' symbolic ref
    refs: iterable or None
      If `None`, `repo.for_each_ref()` is called. Otherwise, an iterable
      from a previous `for_each_ref()` call is expected.

    Returns
    -------
    str
      Formatted refs list
    """
    if refs is None:
        refs = repo.for_each_ref_()

    # generate a list of refs
    refstr = '\n'.join(
        "{objectname} {refname}".format(**r)
        for r in refs
    )
    if refstr:
        refstr += '\n'
    refstr += '@{} HEAD\n'.format(
        repo.call_git(['symbolic-ref', 'HEAD']).strip()
    )
    return refstr


def get_initremote_params_from_url(url):
    """Parse a remote URL for initremote parameters

    Parameters are taken from a URL's query string. In the query
    parameters can be defined directly, or via placeholder
    for all URL components (using Python's format language).

    The following placeholders are supported: 'scheme', 'netloc',
    'path', 'fragment', 'username', 'password', 'hostname',
    'port'. Their values are determined by urlparse(). There is
    no placeholder for the 'query' component, but a 'noquery'
    placeholder is supported, which provides the original
    (reassembled) URL without the query string.

    Parameters
    ----------
    url : str

    Returns
    -------
    list
      git-annex initremote parameter list. Each value string has the format
      'key=value'.
    """
    if url.startswith('datalad-annex::'):
        url = url[15:]
    if not url:
        raise ValueError("Given URL only contained 'datalad-annex::' prefix")
    pu = urlparse(url)
    expansion = {
        p: getattr(pu, p)
        for p in (
            'scheme',
            'netloc',
            'path',
            # we do not extract the 'query', because it is the very
            # thing we iterate over below
            'fragment',
            'username',
            'password',
            'hostname',
            'port')
    }
    expansion['noquery'] = pu._replace(query='').geturl()
    # expand all parameters in the query
    params = [
        # unquote any string -- should be safe, because
        # initremote parameter names should not have any special
        # characters
        unquote(
            # apply any expansion from the URL components
            v.format(**expansion)
        )
        for v in pu.query.split('&')
        # nothing to pull from an empty string
        if v
    ]
    if all(not p.startswith('type=') for p in params):
        # if there is no type declared, this is a plain type=web
        # export using the full URL
        params = ['type=web', 'exporttree=yes', f'url={url}']

    return params


def make_export_tree(repo):
    """Create an exportable tree

    The function expects a clean (bare) repository. It requires no checkout,
    and does not modify any branches or creates commits.

    The tree is always the same, but still has to be create in the repoannex
    to be accessible for git-annex. It looks like this::

        .datalad
        └── dotgit
            ├── refs
            └── repo.zip

    where the two files under ``dotgit/`` link to the two critical keys. The
    placement of the files under ``.datalad/`` is chosen so that the export can
    blend with an export of the underlying dataset without conflict. The name
    ``dotgit`` rather than ``.git`` is chosen to avoid confusing it with
    an actual nested Git repo.

    Parameters
    ----------
    repo: AnnexRepo
      Repository instance to write to.

    Returns
    -------
    str
        ID of the tree object, suitable for `git-annex export`.
    """
    here = repo.config.get('annex.uuid')
    # re-use existing, or go with fixed random one
    origin = repo.config.get('remote.origin.annex-uuid',
                             '8249ffce-770a-11ec-9578-5f6af5e76eaa')
    assert here, "No 'here'"
    assert origin, "No 'origin'"
    # we need to force Git to use a throwaway index file to maintain
    # the bare nature of the repoannex, git-annex would stop functioning
    # properly otherwise
    env = os.environ.copy()
    index_file = repo.pathobj / 'datalad_tmp_index'
    env['GIT_INDEX_FILE'] = str(index_file)
    try:
        for key, kinfo in RepoAnnexGitRemote.xdlra_key_locations.items():
            # create a blob for the annex link
            out = repo._git_runner.run(
                ['git', 'hash-object', '-w', '--stdin'],
                stdin=bytes(
                    f'../../.git/annex/objects/{kinfo["prefix"]}/{key}/{key}',
                    'utf-8'),
                protocol=StdOutCapture)
            linkhash = out['stdout'].strip()
            # place link into a tree
            out = repo._git_runner.run(
                ['git', 'update-index', '--add', '--cacheinfo', '120000',
                 linkhash, kinfo["loc"]],
                protocol=StdOutCapture,
                env=env)
        # write the complete tree, and return ID
        out = repo._git_runner.run(
            ['git', 'write-tree'],
            protocol=StdOutCapture,
            env=env)
        exporttree = out['stdout'].strip()
        # this should always come out identically
        # unless we made changes in the composition of the export tree
        assert exporttree == '7f0e7953e93b4c9920c2bff9534773394f3a5762'

        # clean slate
        if index_file.exists():
            index_file.unlink()
        # fake export.log record
        # <unixepoch>s <here>:<origin> <exporttree>
        now_ts = datetime.datetime.now().timestamp()
        out = repo._git_runner.run(
            ['git', 'hash-object', '-w', '--stdin'],
            stdin=bytes(
                f'{now_ts}s {here}:{origin} {exporttree}\n', 'utf-8'),
            protocol=StdOutCapture)
        exportlog = out['stdout'].strip()
        repo._git_runner.run(
            ['git', 'read-tree', 'git-annex'],
            env=env)
        out = repo._git_runner.run(
            ['git', 'update-index', '--add', '--cacheinfo', '100644',
             exportlog, 'export.log'],
            protocol=StdOutCapture,
            env=env)
        out = repo._git_runner.run(
            ['git', 'write-tree'],
            protocol=StdOutCapture,
            env=env)
        gaupdate = out['stdout'].strip()
        out = repo._git_runner.run(
            ['git', 'commit-tree', '-m', 'Fake export', '-p', 'git-annex',
             gaupdate],
            protocol=StdOutCapture,
            env=env)
        gacommit = out['stdout'].strip()
        repo.call_git(['update-ref', 'refs/heads/git-annex', gacommit])
    finally:
        index_file.unlink()

    return exporttree


def push_caused_change(operations):
    ok_operations = (
        'new-tag', 'new-branch', 'forced-update', 'fast-forward', 'deleted'
    )
    return any(o in operations for o in ok_operations)


def push_error(operations):
    error_operations = (
        'no-match', 'rejected', 'remote-rejected', 'remote-failure',
        'error',
    )
    return any(o in operations for o in error_operations)


def main():
    """git-remote helper executable entrypoint"""
    try:
        if len(sys.argv) < 3:
            raise ValueError("Usage: git-remote-datalad-annex REMOTE-NAME URL")

        remote, url = sys.argv[1:3]
        # provided by Git
        gitdir = os.environ.pop('GIT_DIR')
        # no fallback, must be present
        if gitdir is None:
            raise RuntimeError('GIT_DIR environment variable not defined')

        # stdin/stdout will be used for interactions with git
        # the 'annex' backend really doesn't do much annex-specific
        # albeit maybe progress reporting (unclear to MIH right now)
        # but it does make credential entry possible here, despite the
        # remote helper process being connected to Git with its stdin/stdout
        ui.set_backend('annex')

        # lock and load
        remote = RepoAnnexGitRemote(gitdir, remote, url)
        remote.communicate()
        # there is no value in keeping around the downloads
        # we either have things in the mirror repo or have to
        # redownload anyways
        # leaving the table clean and always bootstrap from scratch
        # has the advantage that we always automatically react to any
        # git-remote reconfiguration between runs
        rmtree(remote.repoannex.path, ignore_errors=True)
    except Exception as e:
        ce = CapturedException(e)
        # Receiving an exception here is "fatal" by definition.
        # Mimicking git's error reporting style.
        print(f"fatal: {ce}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
