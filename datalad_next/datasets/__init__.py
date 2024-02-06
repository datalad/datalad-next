"""Representations of DataLad datasets built on git/git-annex repositories

Two sets of repository abstractions are available :class:`LeanGitRepo` and
:class:`LeanAnnexRepo` vs. :class:`LegacyGitRepo` and :class:`LegacyAnnexRepo`.

:class:`LeanGitRepo` and :class:`LeanAnnexRepo` provide a more modern,
small-ish interface and represent the present standard API for low-level
repository operations. They are geared towards interacting with Git and
git-annex more directly, and are more suitable for generator-like
implementations, promoting low response latencies, and a leaner processing
footprint.

The ``Legacy*Repo`` classes provide a, now legacy, low-level API to repository
operations. This functionality stems from the earliest days of DataLad and
implements paradigms and behaviors that are no longer common to the rest of the
DataLad API. :class:`LegacyGitRepo` and :class:`LegacyAnnexRepo` should no
longer be used in new developments, and are not documented here.


.. currentmodule:: datalad_next.datasets
.. autosummary::
   :toctree: generated

   Dataset
   LeanGitRepo
   LeanAnnexRepo
   LegacyGitRepo
   LegacyAnnexRepo
"""

from datalad.distribution.dataset import (
    Dataset,
    # this does nothing but provide documentation
    # only kept here until this command is converted to
    # pre-call parameter validation
    # TODO REMOVE FOR V2.0
    EnsureDataset as NoOpEnsureDataset,
    # TODO REMOVE FOR V2.0
    datasetmethod,
    # TODO REMOVE FOR V2.0
    resolve_path,
)
from datalad.dataset.gitrepo import GitRepo as LeanGitRepo
from datalad.support.gitrepo import GitRepo as LegacyGitRepo

from datalad.support.gitrepo import GitRepo as LegacyGitRepo
from datalad.support.annexrepo import AnnexRepo as LegacyAnnexRepo

from .annexrepo import LeanAnnexRepo
