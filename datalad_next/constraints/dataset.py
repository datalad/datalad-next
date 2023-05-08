"""Constraints for DataLad datasets"""

from __future__ import annotations

from pathlib import (
    Path,
    PurePath,
)

from datalad_next.datasets import Dataset

from .base import (
    Constraint,
    DatasetParameter,
)
from .exceptions import NoDatasetFound


class EnsureDataset(Constraint):
    """Ensure an absent/present `Dataset` from any path or Dataset instance

    Regardless of the nature of the input (`Dataset` instance or local path)
    a resulting instance (if it can be created) is optionally tested for
    absence or presence on the local file system.

    Due to the particular nature of the `Dataset` class (the same instance
    is used for a unique path), this constraint returns a `DatasetParameter`
    rather than a `Dataset` directly. Consuming commands can discover
    the original parameter value via its `original` property, and access a
    `Dataset` instance via its `ds` property.

    In addition to any value representing an explicit path, this constraint
    also recognizes the special value `None`. This instructs the implementation
    to find a dataset that contains the process working directory (PWD).
    Such a dataset need not have its root at PWD, but could be located in
    any parent directory too. If no such dataset can be found, PWD is used
    directly. Tests for ``installed`` are performed in the same way as with
    an explicit dataset location argument. If `None` is given and
    ``installed=True``, but no dataset is found, an exception is raised
    (this is the behavior of the ``required_dataset()`` function in
    the DataLad core package). With ``installed=False`` no exception is
    raised and a dataset instances matching PWD is returned.
    """
    def __init__(self,
                 installed: bool | None = None,
                 purpose: str | None = None,
                 require_id: bool | None = None):
        """
        Parameters
        ----------
        installed: bool, optional
          If given, a dataset will be verified to be installed or not.
          Otherwise the installation-state will not be inspected.
        purpose: str, optional
          If given, will be used in generated error messages to communicate
          why a dataset is required (to exist)
        idcheck: bool, option
          If given, performs an additional check whether the dataset has a
          valid dataset ID.
        """
        self._installed = installed
        self._purpose = purpose
        self._require_id = require_id
        super().__init__()

    def __call__(self, value) -> DatasetParameter:
        # good-enough test to recognize a dataset instance cheaply
        if hasattr(value, 'repo') and hasattr(value, 'pathobj'):
            ds = value
        # anticipate what require_dataset() could handle and fail if we got
        # something else
        elif not isinstance(value, (str, PurePath, type(None))):
            self.raise_for(
                value, "cannot create Dataset from {type}", type=type(value)
            )
        else:
            ds = self._require_dataset(value)
        assert ds
        if self._installed is not None:
            is_installed = ds.is_installed()
            if self._installed is False and is_installed:
                self.raise_for(ds, 'already exists locally')
            if self._installed and not is_installed:
                self.raise_for(ds, 'not installed')
        if self._require_id and not ds.id:
            self.raise_for(ds, 'does not have a valid datalad-id')
        return DatasetParameter(value, ds)

    def short_description(self) -> str:
        return "(path to) {}Dataset".format(
            'an existing ' if self._installed is True
            else 'a non-existing ' if self._installed is False else 'a ')


    def _require_dataset(self, value):
        from datalad.distribution.dataset import require_dataset
        try:
            ds = require_dataset(
                value,
                check_installed=self._installed is True,
                purpose=self._purpose,
            )
            return ds
        except NoDatasetFound:
            # mitigation of non-uniform require_dataset() behavior.
            # with value == None it does not honor check_installed
            # https://github.com/datalad/datalad/issues/7281
            if self._installed is True:
                # if we are instructed to ensure an installed dataset
                raise
            else:
                # but otherwise go with CWD. require_dataset() did not
                # find a dataset in any parent dir either, so this is
                # the best we can do. Installation absence verification
                # will happen further down
                return Dataset(Path.cwd())
