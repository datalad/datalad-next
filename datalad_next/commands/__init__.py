"""Components for basic functions of commands and their results"""
from __future__ import annotations

from typing import Dict

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import generic_result_renderer
from datalad.interface.base import eval_results
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
    parameters can be provided through the :meth:`get_parameter_validator`
    method.

    To transition a command from ``Interface`` to ``ValidatedInterface``,
    replace the base class declaration and declare a ``_validator_`` class
    member. Any ``constraints=`` declaration for ``Parameter`` instances
    should either be removed, or moved to the corresponding entry in
    ``_validator_``.
    """
    _validator_: EnsureCommandParameterization | None = None

    @classmethod
    def get_parameter_validator(cls) -> EnsureCommandParameterization | None:
        """Returns a validator for the entire parameter set of a command

        If parameter validation shall be performed, this method must return an
        instance of
        :class:`~datalad_next.constraints.parameter.EnsureCommandParameterization`.
        All parameters will be passed through this validator, and only the its
        output will be passed to the underlying command's ``__call__`` method.

        Consequently, the core implementation of a command only needs to
        support the output values of the validators declared by itself.

        Factoring out input validation, normalization, type coercion etc. into
        a dedicated component also makes it accessible for upfront validation
        and improved error reporting via the different DataLad APIs.

        If a command does not implement parameter validation in this fashion,
        this method must return ``None``.

        The default implementation returns the ``_validator_`` class member.
        """
        return cls._validator_
