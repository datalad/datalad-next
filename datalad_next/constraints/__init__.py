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
"""

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
from .formats import (
    EnsureJSON,
    EnsureURL,
    EnsureParsedURL,
)
