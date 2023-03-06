"""Data validation, coercion, and parameter documentation

.. currentmodule:: datalad_next.constraints
.. autosummary::
   :toctree: generated

   base
   basic
   compound
   formats
   parameter
   git
   dataset
   exceptions
"""
from .base import (
    AllOf,
    AnyOf,
    Constraint,
    DatasetParameter,
)
# expose constraints with direct applicability, but not
# base and helper classes
from .basic import (
    EnsureBool,
    EnsureCallable,
    EnsureChoice,
    EnsureFloat,
    EnsureInt,
    EnsureKeyChoice,
    EnsureNone,
    EnsurePath,
    EnsureStr,
    EnsureRange,
    EnsureValue,
    NoConstraint,
)
from .compound import (
    EnsureIterableOf,
    EnsureListOf,
    EnsureTupleOf,
    EnsureMapping,
    EnsureGeneratorFromFileLike,
)
# this is the key type, almost all consuming code will want to
# have this for `except` clauses
from .exceptions import ConstraintError
from .formats import (
    EnsureJSON,
    EnsureURL,
    EnsureParsedURL,
)
