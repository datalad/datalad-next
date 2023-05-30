from __future__ import annotations

# import all the pieces one would need for an implementation
# in a single place
from annexremote import UnsupportedRequest

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
