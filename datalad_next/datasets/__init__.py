"""Representations of DataLad datasets built on git/git-annex repositories

Two sets of repository abstractions are available :class:`LeanGitRepo` and
:class:`LeanAnnexRepo` vs. :class:`LegacyGitRepo` and :class:`LegacyAnnexRepo`.

The latter are the classic classes providing a, now legacy, low-level API to
repository operations. This functionality stems from the earliest days of
DataLad and implements paradigms and behaviors that are no longer common to
the rest of the DataLad API. :class:`LegacyGitRepo` and
:class:`LegacyAnnexRepo` should no longer be used in new developments.

:class:`LeanGitRepo` and :class:`LeanAnnexRepo` on the other hand provide
a more modern, substantially restricted API and represent the present
standard API for low-level repository operations. They are geared towards
interacting with Git and git-annex more directly, and are more suitable
for generator-like implementations, promoting low response latencies, and
a leaner processing footprint.
"""

from pathlib import Path

from datalad.distribution.dataset import (
    Dataset,
    # this does nothing but provide documentation
    # only kept here until this command is converted to
    # pre-call parameter validation
    EnsureDataset as NoOpEnsureDataset,
    datasetmethod,
    resolve_path,
)
from datalad.dataset.gitrepo import GitRepo as LeanGitRepo

from datalad.support.gitrepo import GitRepo as LegacyGitRepo
from datalad.support.annexrepo import AnnexRepo as LegacyAnnexRepo


class LeanAnnexRepo(LegacyAnnexRepo):
    """git-annex repository representation with a minimized API

    This is a companion of :class:`LeanGitRepo`. In the same spirit, it
    restricts its API to a limited set of method that primarily extend
    :class:`LeanGitRepo` with a set of ``call_annex*()`` methods.
    """
    # list of attributes permitted in the "lean" API. This list extends
    # the API of LeanGitRepo
    # TODO extend whitelist of attributed as necessary
    _lean_attrs = [
        '_check_git_version',
        # used by AnnexRepo.__init__() -- should be using `is_valid()`
        'is_valid_git',
        'is_valid_annex',
        '_is_direct_mode_from_config',
    ]

    # intentionally limiting to just `path` as the only constructor argument
    def __new__(cls, path: Path):
        for attr in dir(cls):
            if not hasattr(LeanGitRepo, attr) \
                    and callable(getattr(cls, attr)) \
                    and attr not in LeanAnnexRepo._lean_attrs:
                setattr(cls, attr, _unsupported_method)

        obj = super(LegacyAnnexRepo, cls).__new__(cls)

        return obj


def _unsupported_method(self):
    raise NotImplementedError('method unsupported by LeanAnnexRepo')
