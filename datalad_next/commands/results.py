from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import logging
from pathlib import (
    Path,
    PurePath,
)

from datalad_next.datasets import Dataset
from datalad_next.exceptions import CapturedException


# TODO Could be `StrEnum`, came with PY3.11
class CommandResultStatus(Enum):
    """Enumeration of possible statuses of command results
    """
    ok = 'ok'
    notneeded = 'notneeded'
    impossible = 'impossible'
    error = 'error'


# which status is a success , which is failure
success_status_map = {
    'ok': 'success',
    'notneeded': 'success',
    'impossible': 'failure',
    'error': 'failure',
}


# We really want this to be `kw_only=True`, but cannot, because it only
# came with PY3.10
# Until this can be enabled, we cannot have additional _required_ properties
# coming from derived classes. Instead, we have to make any and all
# additional properties optional (with default None), because also in this
# base class we do define optional ones (and it makes no sense not to do
# that either).
#@dataclass(kw_only=True)
@dataclass
class CommandResult:
    """Base data class for result records emitted by DataLad commands.

    Historically, such results records have taken the form of a Python
    ``dict``. This class provides some API for its instances to be
    compatible with legacy code that expects a ``dict``.

    .. seealso::

      https://docs.datalad.org/design/result_records.html
    """
    # TODO implement post_init and possibly check for validated of
    # some arguments (e.g. status is a valid value). Maybe do all of that
    # conditional on some config flag that could be set during test
    # execution

    # mandatory as per
    # http://docs.datalad.org/design/result_records.html#mandatory-fields
    action: str
    """A string label identifying which type of operation a result is
    associated with. Labels must not contain white space. They should be
    compact, and lower-cases, and use ``_`` (underscore) to separate words in
    compound labels.
    """
    status: CommandResultStatus
    """This field indicates the nature of a result in terms of four
    categories, identified by a :class:`CommandResultStatus` value.
    The result status is used by user communication, but also for decision
    making on the overall success or failure of a command operation.
    """
    path: str | Path
    """An *absolute* path describing the local entity a result is associated
    with (the subject of the result record). Paths must be platform-specific
    (e.g., Windows paths on Windows, and POSIX paths on other operating
    systems). When a result is about an entity that has no meaningful relation
    to the local file system (e.g., a URL to be downloaded), the ``path`` value
    should be determined with respect to the potential impact of the result
    on any local entity (e.g., a URL downloaded to a local file path, a local
    dataset modified based on remote information).
    """
    # optional
    # TODO complete documentation of all members
    message: str | tuple | None = None
    exception: CapturedException | None = None
    error_message: str | tuple | None = None
    type: str | None = None
    logger: logging.Logger | None = None
    refds: str | Path | Dataset = None

    # any and all of the code below makes it possible to feed such result
    # instances through the datalad-core result processing loop (which
    # expects results to be dicts with string keys and (most) values to
    # be string only also.
    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)

    def __getitem__(self, key: str):
        return self._stringify4legacy(getattr(self, key))

    def get(self, key, default=None):
        return self._stringify4legacy(getattr(self, key, default))

    def pop(self, key, default=None):
        item = getattr(self, key, default)
        if hasattr(self, key):
            setattr(self, key, None)
        return self._stringify4legacy(item)

    def items(self):
        for k, v in self.__dict__.items():
            yield k, self._stringify4legacy(v)

    def _stringify4legacy(self, val):
        if isinstance(val, PurePath):
            return str(val)
        elif isinstance(val, Dataset):
            return val.path
        elif issubclass(getattr(val, '__class__', None), Enum):
            return val.value
        return val
