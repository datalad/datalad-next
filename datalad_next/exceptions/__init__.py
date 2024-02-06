"""Special purpose exceptions

.. currentmodule:: datalad_next.exceptions
.. autosummary::
   :toctree: generated

   CapturedException
   IncompleteResultsError
   NoDatasetFound
"""
# we cannot have CommandError above, sphinx complains

# TODO rethink the purpose of this module and possibly
# make it about *external* custom exceptions
from datalad.runner.exception import CommandError
from datalad.support.exceptions import (
    CapturedException,
    IncompleteResultsError,
    NoDatasetFound,
)

# TODO REMOVE FOR V2.0 (they are specific to that module
from datalad_next.url_operations import (
    UrlOperationsRemoteError,
    UrlOperationsAuthenticationError,
    UrlOperationsAuthorizationError,
    UrlOperationsInteractionError,
    UrlOperationsResourceUnknown,
)
