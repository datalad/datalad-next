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
