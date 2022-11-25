"""All custom exceptions used in datalad-next"""

from datalad.runner import CommandError
from datalad.support.exceptions import (
    AccessDeniedError,
    AccessFailedError,
    CapturedException,
    DownloadError,
    IncompleteResultsError,
    NoDatasetFound,
    TargetFileAbsent,
)

# derive from TargetFileAbsent as the closest equivalent in datalad-core
class UrlTargetNotFound(TargetFileAbsent):
    """A connection request succeeded in principle, but target was not found

    Equivalent of an HTTP404 response.
    """
    pass
