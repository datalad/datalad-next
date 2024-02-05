"""Handlers for operations on various URL types and protocols

Available handlers:

.. currentmodule:: datalad_next.url_operations
.. autosummary::
   :toctree: generated

   UrlOperations
   AnyUrlOperations
   FileUrlOperations
   HttpUrlOperations
   SshUrlOperations
   UrlOperationsRemoteError
   UrlOperationsResourceUnknown
   UrlOperationsInteractionError
   UrlOperationsAuthenticationError
   UrlOperationsAuthorizationError
"""

from .base import (
    # base class for 3rd-party extensions and implementations
    UrlOperations,
)

# operation support for different protocols
from .any import AnyUrlOperations
from .file import FileUrlOperations
from .http import HttpUrlOperations
from .ssh import SshUrlOperations

# primary exceptions types
from .exceptions import (
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
    UrlOperationsInteractionError,
    UrlOperationsAuthenticationError,
    UrlOperationsAuthorizationError,
)

# TODO REMOVE EVERYTHING BELOW FOR V2.0
import logging
from functools import partial
from more_itertools import side_effect
from pathlib import Path
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
)

import datalad
from datalad_next.config import ConfigManager
from datalad_next.utils import log_progress
from datalad_next.utils.multihash import (
    MultiHash,
    NoOpHash,
)

lgr = logging.getLogger('datalad.ext.next.url_operations')
