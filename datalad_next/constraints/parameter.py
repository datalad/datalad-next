from __future__ import annotations

from collections.abc import Container
from typing import (
    Any,
    Dict,
    TYPE_CHECKING,
    TypeVar,
)

from .base import (
    Constraint,
    ConstraintDerived,
)
from .basic import (
    EnsureBool,
    EnsureChoice,
    EnsureFloat,
    EnsureInt,
    EnsureStr,
    NoConstraint,
)
from .compound import (
    EnsureIterableOf,
    EnsureMapping,
)

from .exceptions import (
    ConstraintError,
    CommandParametrizationError,
)

if TYPE_CHECKING:  # pragma: no cover
    from datalad_next.commands import Parameter

aEnsureParameterConstraint = TypeVar(
    'aEnsureParameterConstraint',
    bound='aEnsureParameterConstraint',
)
aParameter = TypeVar('aParameter', bound='Parameter')


class NoValue:
    """Type to annotate the absence of a value

    For example in a list of parameter defaults. In general `None` cannot
    be used, as it may be an actual value, hence we use a local, private
    type.
    """
    pass


class EnsureParameterConstraint(EnsureMapping):
    """Ensures a mapping from a Python parameter name to a value constraint

    An optional "pass-though" value can be declare that is then exempt from
    validation and is returned as-is. This can be used to support, for example,
    special default values that only indicate the optional nature of a
    parameter. Declaring them as "pass-through" avoids a needless
    complexity-increase of a value constraint that would translate onto
    user-targeted error reporting.
    """
    # valid parameter name for Python and CLI
    # - must start with a lower-case letter
    # - must not contain symbols other than lower-case letters,
    #   digits, and underscore
    valid_param_name_regex = r'[a-z]{1}[a-z0-9_]*'

    def __init__(self,
                 constraint: ConstraintDerived,
                 passthrough: Any = NoValue):
        """
        Parameters
        ----------
        constraint:
          Any ``Constraint`` subclass instance that will be used to validate
          parameter values.
        passthrough:
          A value that will not be subjected to validation by the value
          constraint, but is returned as-is. This can be used to exempt
          default values from validation, e.g. when defaults are only
          placeholder values to indicate the optional nature of a parameter.
        """
        super().__init__(
            key=EnsureStr(
                match=EnsureParameterConstraint.valid_param_name_regex),
            value=constraint,
            # make it look like dict(...)
            delimiter='=',
        )
        self._passthrough = passthrough

    @property
    def parameter_constraint(self):
        return self._value_constraint

    @property
    def passthrough_value(self):
        return self._passthrough

    def __call__(self, value) -> Dict:
        key, val = self._get_key_value(value)
        key = self._key_constraint(key)
        val = self._value_constraint(val) \
            if val != self.passthrough_value else val
        return {key: val}

    @classmethod
    def from_parameter(
            cls,
            spec: aParameter,
            default: Any,
            item_constraint: ConstraintDerived = None,
            nargs: str or int = None) -> aEnsureParameterConstraint:
        """
        Parameters
        ----------
        spec: Parameter
          Instance of a datalad-core Parameter. If not overwritten by values
          given to the other arguments of this method, item constraints,
          number of arguments and other argparse-specific information
          is taken from this object and processed to built a comprehensive
          constraint that handles all aspects of the specification in a
          homogenous fashion via the Constraint interface.
        default: Any
          A parameter's default value. It is configured as a "pass-through"
          value that will not be subjected to validation.
        item_constraint:
          If given, it override any constraint declared in the Parameter
          instance given to `spec`
        nargs:
          If given, it override any nargs setting declared in the Parameter
          instance given to `spec`.
        """
        value_constraint = _get_comprehensive_constraint(
            spec,
            item_constraint,
            nargs,
        )
        return cls(value_constraint, passthrough=default)


# that mapping is NOT to be expanded!
# it is a legacy leftover. It's usage triggers a DeprecationWarning
_constraint_spec_map = {
    'float': EnsureFloat(),
    'int': EnsureInt(),
    'bool': EnsureBool(),
    'str': EnsureStr(),
}


def _get_comprehensive_constraint(
        param_spec: aParameter,
        item_constraint_override: ConstraintDerived = None,
        nargs_override: str or int = None):
    action = param_spec.cmd_kwargs.get('action')
    # definitive per-item constraint, consider override
    # otherwise fall back on Parameter.constraints
    constraint = item_constraint_override or param_spec.constraints

    if not (constraint is None or hasattr(constraint, '__call__')):
        import warnings
        warnings.warn("Literal constraint labels are no longer supported.",
                      DeprecationWarning)
        try:
            return _constraint_spec_map[constraint]
        except KeyError:
            raise ValueError(
                f"unsupported constraint specification '{constraint}'")

    if not constraint:
        if action in ('store_true', 'store_false'):
            constraint = EnsureBool()
        elif param_spec.cmd_kwargs.get('choices'):
            constraint = EnsureChoice(*param_spec.cmd_kwargs.get('choices'))
        else:
            # always have one for simplicity
            constraint = NoConstraint()

    # we must addtionally consider the following nargs spec for
    # a complete constraint specification
    # (int, '*', '+'), plus action=
    # - 'store_const' TODO
    # - 'store_true' and 'store_false' TODO
    # - 'append'
    # - 'append_const' TODO
    # - 'count' TODO
    # - 'extend' TODO

    # get the definitive argparse "nargs" value
    nargs = nargs_override or param_spec.cmd_kwargs.get('nargs', None)
    # try making a specific number explicit via dtype change
    try:
        nargs = int(nargs)
    except (ValueError, TypeError):
        pass

    # TODO reconsider using `list`, with no length-check it could
    # be a generator
    if isinstance(nargs, int):
        # we currently consider nargs=1 to be a request of a
        # single item, not a forced single-item list
        if nargs > 1:
            # sequence of a particular length
            constraint = EnsureIterableOf(
                list, constraint, min_len=nargs, max_len=nargs)
    elif nargs == '*':
        # datalad expects things often/always to also work for a single item
        constraint = EnsureIterableOf(list, constraint) | constraint
    elif nargs == '+':
        # sequence of at least 1 item, always a sequence,
        # but again datalad expects things often/always to also work for
        # a single item
        constraint = EnsureIterableOf(
            list, constraint, min_len=1) | constraint
    # handling of `default` and `const` would be here
    #elif nargs == '?'

    if action == 'append':
        # wrap into a(nother) sequence
        # (think: list of 2-tuples, etc.
        constraint = EnsureIterableOf(list, constraint)

    return constraint


class EnsureCommandParameterization(Constraint):
    """
    """
    def __init__(self,
                 param_constraints: dict[ConstraintDerived],
                 *,
                 validate_defaults: Container[str] | None = None,
    ):
        """
        Parameters
        ----------
        param_constraints: dict
          Mapping of parameter names to parameter constraints. On validation
          an ``EnsureParameterConstraint`` instance will be created for
          each item in this dict.
        validate_defaults: container(str)
          If given, this is a set of parameter names for which the default
          rule, to not validate default values, does not apply and
          default values shall be passed through a given validator.
        """
        super().__init__()
        self._param_constraints = param_constraints
        self._validate_defaults = validate_defaults or set()

    def joint_validation(self, params: Dict) -> Dict:
        """Implement for joint validation of the full parameterization

        This method is called with all, individually validated, command
        parameters in keyword-argument form in the ``params`` dict argument.

        Arbritrary additional validation steps can be performed on the full
        set of parameters that may involve raising exceptions on validation
        errors, but also value transformation or replacements of individual
        parameters based on the setting of others.

        The parameter values return by the method are passed on to the
        respective command implementation.

        Returns
        -------
        dict
          The returned dict must have a value for each item pass in via
          ``params``.

        Raises
        ------
        ConstraintError
          The ``value`` contained in an exception is the entire ``params``
          dict. A typical ``raise`` statement would look like::

            raise ConstraintError(self, params, "description of the violation")

          Alternatively, the ``raise_for(value, message)`` method of a
          ``Constraint`` can be used as a convenience.
        """
        return params

    def __call__(
        self,
        kwargs,
        at_default=None,
        on_error='raise-immediately',
    ) -> Dict:
        """
        Parameters
        ----------
        kwargs: dict
          Parameter name (``str``)) to value (any) mapping of the parameter
          set.
        at_default: set or None
          Set of parameter names where the respective values in ``kwargs``
          match their respective defaults. This is used for deciding whether
          or not to process them with an associated value constraint (see the
          ``validate_defaults`` constructor argument).
        on_error: {'raise-immediately', 'raise-at-end'}
          Flag how to handle constraint violation. By default, validation is
          stopped at the first error and an exception is raised. When an
          exhaustive validation is performed, an eventual exception contains
          information on all constraint violations.

        Raises
        ------
        CommandParametrizationError
          Raised whenever one (or more) ``ConstraintError`` exceptions are
          caught during validation. Other exception types are not caught and
          pass through.
        """
        assert on_error in ('raise-immediately', 'raise-at-end')
        exceptions = {}
        validated = {}
        for argname, arg in kwargs.items():
            if at_default \
                    and argname not in self._validate_defaults \
                    and argname in at_default:
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
            validator = self._param_constraints.get(argname, lambda x: x)
            try:
                validated[argname] = validator(arg)
            # we catch only ConstraintError -- only these exceptions have what
            # we need for reporting. If any validator chooses to raise
            # something else, we do not handle it here, but let it bubble up.
            # it may be an indication of something being wrong with validation
            # itself
            except ConstraintError as e:
                exceptions[argname] = e
                if on_error == 'raise-immediately':
                    raise CommandParametrizationError(exceptions)

        if exceptions:
            raise CommandParametrizationError(exceptions)

        try:
            # call (subclass) method to perform holistic, cross-parameter
            # validation of the full parameterization
            validated = self.joint_validation(validated)
        except ConstraintError as e:
            # here 'raise-immediately' is no longer different from
            # 'raise-at-end'
            exceptions['__all__'] = e

        if exceptions:
            raise CommandParametrizationError(exceptions)

        return validated

    def short_description(self):
        # TODO Constraint.__repr__ is shit!
        return "I have to be here, but I do not want to"
