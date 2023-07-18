"""All custom exceptions used in datalad-next"""

# TODO rethink the purpose of this module and possibly
# make it about *external* custom exceptions
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
