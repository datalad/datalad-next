"""Credential management

.. currentmodule:: datalad_next.credman
.. autosummary::
   :toctree: generated

   exceptions
   manager
"""
from .exceptions import (
    InvalidCredential,
    NoSuitableCredentialAvailable,
)
from .manager import CredentialManager
