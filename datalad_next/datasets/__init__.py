from datalad.distribution.dataset import (
    Dataset,
    # this does nothing but provide documentation
    # only kept here until this command is converted to
    # pre-call parameter validation
    EnsureDataset as NoOpEnsureDataset,
    datasetmethod,
)
from datalad.dataset.gitrepo import GitRepo as LeanGitRepo
