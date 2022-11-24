"""Validate DataLad datasets"""

from pathlib import PurePath

from datalad_next.exceptions import NoDatasetFound

from .base import Constraint


class DatasetParameter:
    """Utitlity class to report an original and resolve dataset parameter value

    This is used by `EnsureDataset` to be able to report the original argument
    semantics of a dataset parameter to a receiving command.

    The original argument is provided via the `original` property.
    A corresponding `Dataset` instance is provided via the `ds` property.
    """
    def __init__(self, original, ds):
        self.original = original
        self.ds = ds


class EnsureDataset(Constraint):
    """Ensure a absent/present `Dataset` from any path or Dataset instance

    Regardless of the nature of the input (`Dataset` instance or local path)
    a resulting instance (if it can be created) is optionally tested for
    absence or presence on the local file system.

    Due to the particular nature of the `Dataset` class (the same instance
    is used for a unique path), this constraint returns a `DatasetParameter`
    rather than a `Dataset` directly. Consuming commands can discover
    the original parameter value via its `original` property, and access a
    `Dataset` instance via its `ds` property.
    """
    def __init__(self, installed: bool = None, purpose: str = None):
        """
        Parameters
        ----------
        installed: bool, optional
          If given, a dataset will be verified to be installed or not.
          Otherwise the installation-state will not be inspected.
        purpose: str, optional
          If given, will be used in generated error messages to communicate
          why a dataset is required (to exist)
        """
        self._installed = installed
        self._purpose = purpose
        super().__init__()

    def __call__(self, value) -> DatasetParameter:
        # good-enough test to recognize a dataset instance cheaply
        if hasattr(value, 'repo') and hasattr(value, 'pathobj'):
            if self._installed is not None:
                is_installed = value.is_installed()
                if self._installed and not is_installed:
                    # for uniformity with require_dataset() below, use
                    # this custom exception
                    raise NoDatasetFound(f'{value} is not installed')
                elif not self._installed and is_installed:
                    raise ValueError(f'{value} already exists locally')
            return DatasetParameter(value, value)
        elif not isinstance(value, (str, PurePath)):
            raise TypeError(f"Cannot create Dataset from {type(value)}")

        from datalad.distribution.dataset import require_dataset
        ds = require_dataset(
            value,
            check_installed=self._installed is True,
            purpose=self._purpose,
        )
        if self._installed is False and ds.is_installed():
            raise ValueError(f'{ds} already exists locally')
        return DatasetParameter(value, ds)

    def short_description(self) -> str:
        return "(path to) {}Dataset".format(
            'an existing ' if self._installed is True
            else 'a non-existing ' if self._installed is False else 'a ')
