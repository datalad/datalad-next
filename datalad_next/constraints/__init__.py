"""Data validation, coercion, and parameter documentation

This module provides a set of uniform classes to validate and document
particular aspects of inputs. In a nutshell, each of these
:class:`~datalad_next.constraints.base.Constraint` class:

- focuses on a specific aspect, such as data type coercion,
  or checking particular input properties
- is instantiated with a set of parameters to customize
  such an instance for a particular task
- performs its task by receiving an input via its ``__call__()``
  method
- provides default auto-documentation that can be customized
  by wrapping an instance in
  :class:`~datalad_next.constraints.compound.WithDescription`

Individual ``Constraint`` instances can be combined with logical AND
(:class:`~datalad_next.constraints.base.AllOf`) and OR
(:class:`~datalad_next.constraints.base.AnyOf`) operations to form arbitrarily
complex constructs.

On (validation/coercion) error, instances raise
:class:`~datalad_next.constraints.exceptions.ConstraintError`) via their
``raise_for()`` method. This approach to error reporting helps to communicate
standard (yet customizable) error messages, aids structured error reporting,
and is capable of communication the underlying causes of an error in full
detail without the need to generate long textual descriptions.

:class:`~datalad_next.constraints.parameter.EnsureCommandParameterization` is a
particular variant of a ``Constraint`` that is capable of validating a complete
parameterization of a command (or function), for each parameter individually,
and for arbitrary combinations of parameters. It puts a particular emphasis on
structured error reporting.


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
    WithDescription,
)
# this is the key type, almost all consuming code will want to
# have this for `except` clauses
from .exceptions import ConstraintError
from .formats import (
    EnsureJSON,
    EnsureURL,
    EnsureParsedURL,
)
