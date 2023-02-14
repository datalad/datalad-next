"""Components for basic functions of commands and their results"""
from __future__ import annotations

from typing import Dict

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import generic_result_renderer
try:
    # datalad 0.17.10+
    from datalad.interface.base import eval_results
except ImportError:
    # older datalad
    from datalad.interface.utils import eval_results
from datalad.support.param import Parameter

from datalad_next.constraints.parameter import (
    EnsureCommandParameterization,
    ParameterConstraintContext,
)
from datalad_next.datasets import datasetmethod


class ValidatedInterface(Interface):
    """Alternative base class for commands with uniform parameter validation

    .. note::
       This interface is a draft. Usage is encouraged, but future changes
       are to be expected.

    Commands derived from the traditional ``Interface`` class have no built-in
    input parameter validation beyond CLI input validation of individual
    parameters. Consequently, each command must perform custom parameter
    validation, which often leads to complex boilerplate code that is largely
    unrelated to the purpose of a particular command.

    This class is part of a framework for uniform parameter validation,
    regardless of the target API (Python, CLI, GUI). The implementation of
    a command's ``__call__`` method can focus on the core purpose of the
    command, while validation and error handling can be delegated elsewhere.

    A validator for all individual parameters and the joint-set of all
    parameters is declared in a ``_validator_`` class member.
    This is an instance of ``EnsureCommandParameterization``.
    Only the output of this validator (`Constraint` implementation) will be
    passed to the underlying command's ``__call__`` method. Consequently,
    a command only needs to support the output values of the validators
    declared by itself.

    To transition a command from ``Interface`` to ``ValidatedInterface``,
    replace the base class declaration and declare a ``_validator_`` class
    member. Any ``constraints=`` declaration for ``Parameter`` instances
    should either be removed, or moved to the corresponding entry in
    ``_validator_``.
    """
    _validator_ = None

    @classmethod
    def get_parameter_validator(cls) -> EnsureCommandParameterization | None:
        return cls._validator_
