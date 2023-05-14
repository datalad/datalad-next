"""All custom exceptions used in datalad-next"""

from datalad_next.runners import CommandError
from datalad.support.exceptions import (
    CapturedException,
    IncompleteResultsError,
    NoDatasetFound,
)

from datalad_next.url_operations import (
    UrlOperationsRemoteError,
    UrlOperationsAuthenticationError,
    UrlOperationsAuthorizationError,
    UrlOperationsInteractionError,
    UrlOperationsResourceUnknown,
)
