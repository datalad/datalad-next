from __future__ import annotations

# import all the pieces one would need for an implementation
# in a single place
from annexremote import UnsupportedRequest
from typing import Any

from datalad.customremotes import (
    # this is an enhanced RemoteError that self-documents its cause
    RemoteError,
    SpecialRemote as _SpecialRemote,
)
from datalad.customremotes.main import main as super_main

from datalad_next.datasets import LeanAnnexRepo


class SpecialRemote(_SpecialRemote):
    """Base class of all datalad-next git-annex special remotes"""
    def __init__(self, annex):
        super(SpecialRemote, self).__init__(annex=annex)

        self._repo = None
        self._remotename = None

    @property
    def repo(self) -> LeanAnnexRepo:
        """Returns a representation of the underlying git-annex repository

        An instance of :class:`~datalad_next.datasets.LeanAnnexRepo` is
        returned, which intentionally provides a restricted API only. In order
        to limit further proliferation of the ``AnnexRepo`` API.
        """
        if self._repo is None:
            self._repo = LeanAnnexRepo(self.annex.getgitdir())
        return self._repo

    @property
    def remotename(self) -> str:
        """Name of the (git) remote the special remote is operating under"""

        if self._remotename is None:
            self._remotename = self.annex.getgitremotename()
        return self._remotename

    def get_remote_gitcfg(
            self,
            remotetypename: str,
            name: str,
            default: Any | None = None,
            **kwargs
    ):
        """Get a particular Git configuration item for the special remote

        This target configuration here is *not* the git-annex native
        special remote configuration that is provided or altered with
        ``initremote`` and ``enableremote``, and is committed to the
        ``git-annex`` branch. Instead this is a clone and remote
        specific configuration, declared in Git's configuration system.

        The configuration items queried have the naming scheme::

            remote.<remotename>.<remotetypename>-<name>
            datalad.<remotetypename>.<name>

        where ``<remotename>`` is the name of the Git remote, the special
        remote is operating under, ``<remotetypename>`` is the name of the
        special remote implementation (e.g., ``uncurl``), and ``<name>``
        is the name of a particular configuration flavor.

        Parameters
        ----------
        remotetypename: str
          Name of the special remote implementation configuration is
          requested for.
        name: str
          The name of the "naked" configuration item, without any
          sub/sections. Must be a valid git-config variable name, i.e.,
          case-insensitive, only alphanumeric characters and -, and
          must start with an alphabetic character.
        default:
          A default value to be returned if there is no configuration.
        **kwargs:
          Passed on to :func:`datalad_next.config.ConfigManager.get()`

        Returns
        -------
        Any
          If a remote-specific configuration exists, it is reported. Otherwise
          a remote-type specific configuration is reported, or the default
          provided with the method call, if no configuration is found at all.
        """
        cfgget = self.repo.config.get
        return cfgget(
            f'remote.{self.remotename}.{remotetypename}-{name}',
            default=cfgget(
                f'datalad.{remotetypename}.{name}',
                default=default,
            )
        )
