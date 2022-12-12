"""Components for basic functions of commands and their results"""
from typing import Dict

from datalad.interface.base import (
    Interface,
    build_doc,
)
from datalad.interface.results import get_status_dict
from datalad.interface.utils import (
    eval_results,
    generic_result_renderer,
)
from datalad.support.param import Parameter


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

    This class provides the framework for uniform parameter validation,
    regardless of the target API (Python, CLI, GUI). The implementation of
    a command's ``__call__`` method can focus on the core purpose of the
    command, while validation and error handling can be delegated elsewhere.

    Validators for individual parameters are declared in a ``_validators_``
    class member. This is a dict mapping from parameter name to a
    ``Constraint`` instance. The latter can be the same as those used for the
    ``Parameter`` specifications.  ``ValidatedInterface.validate_args()`` will
    inspect this dict for the presence of a validator for particular
    parameters, and run them. The output of the respective validator will be
    collected and passed to the underlying command's ``__call__`` method.
    Consequently, a command only needs to support the output values of the
    validators declared by itself.

    In two cases no validation is performed: 1) when no validator for a
    particular parameter is declared, any input value is passed on as-is.
    2) when a parameter value is identical to its default value, the default
    is passed as-is.

    An important consequence of the second condition is that validators need
    not cover a default value. For example, a parameter ``path=None``, where
    ``None`` is a special value used to indiciate an optional and unset value,
    but actually only paths are acceptable input values, can be described as::

        _validators_ = {'path': EnsurePath()}

    and it is not necessary to do something like
    ``EnsurePath() | EnsureNone()``.

    To transition a command from ``Interface`` to ``ValidatedInterface``,
    replace the base class declaration and declare a ``_validators_`` class
    member. Any ``constraints=`` declaration for ``Parameter`` instances
    should either be removed, or moved to the corresponding entry in
    ``_validators_``.
    """
    @classmethod
    def validate_args(cls: Interface, kwargs: Dict, at_default: set) -> Dict:
        validated = {}
        for argname, arg in kwargs.items():
            if argname in at_default:
                # do not validate any parameter where the value matches the
                # default declared in the signature. Often these are just
                # 'do-nothing' settings or have special meaning that need
                # not be communicated to a user. Not validating them has
                # two consequences:
                # - the condition can simply be referred to as "default
                #   behavior" regardless of complexity
                # - a command implementation must always be able to handle
                #   its own defaults directly, and cannot delegate a
                #   default value handling to a constraint
                #
                # we must nevertheless pass any such default value through
                # to make/keep them accessible to the general result handling
                # code
                validated[argname] = arg
                continue
            validator = cls._validators_.get(argname, lambda x: x)
            # TODO option to validate all args despite failure
            try:
                validated[argname] = validator(arg)
            except Exception as e:
                raise ValueError(
                    f'Validation of parameter {argname!r} failed') from e
        return validated
