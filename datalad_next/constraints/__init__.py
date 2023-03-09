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
    IsCallable,
    IsChoice,
    EnsureFloat,
    EnsureInt,
    IsKeyChoice,
    IsNone,
    EnsurePath,
    IsStr,
    IsRange,
    IsValue,
    NoConstraint,
)
from .compound import (
    ToIterableOf,
    ToListOf,
    ToTupleOf,
    EnsureMapping,
    EnsureGeneratorFromFileLike,
    WithDescription,
)
# this is the key type, almost all consuming code will want to
# have this for `except` clauses
from .exceptions import ConstraintError
from .formats import (
    EnsureJSON,
    IsURL,
    EnsureParsedURL,
)
