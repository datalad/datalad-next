"""Constraints for command/function parameters"""

from __future__ import annotations

from collections.abc import Container
from typing import (
    Any,
    Callable,
    Dict,
    TYPE_CHECKING,
    Type,
    TypeVar,
)

from .base import Constraint
from .basic import (
    EnsureBool,
    EnsureChoice,
    EnsureFloat,
    EnsureInt,
    EnsureStr,
    NoConstraint,
)
from .compound import (
    ConstraintWithPassthrough,
    EnsureIterableOf,
    EnsureMapping,
)

from .exceptions import (
    ConstraintError,
    ParametrizationErrors,
    CommandParametrizationError,
    ParameterConstraintContext,
)

if TYPE_CHECKING:  # pragma: no cover
    from datalad_next.commands import Parameter

EnsureParameterConstraint_T = TypeVar(
    'EnsureParameterConstraint_T',
    bound='EnsureParameterConstraint',
)


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
                 constraint: Constraint,
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
            value=ConstraintWithPassthrough(
                constraint,
                passthrough,
            ),
            # make it look like dict(...)
            delimiter='=',
        )

    @property
    def parameter_constraint(self):
        return self._value_constraint

    @property
    def passthrough_value(self):
        return self._value_constraint.passthrough

    def __call__(self, value) -> Dict:
        key, val = self._get_key_value(value)
        key = self._key_constraint(key)
        val = self._value_constraint(val) \
            if val != self.passthrough_value else val
        return {key: val}

    @classmethod
    def from_parameter(
            cls: Type[EnsureParameterConstraint_T],
            spec: Parameter,
            default: Any,
            item_constraint: Constraint | None = None,
            nargs: str | int | None = None) -> EnsureParameterConstraint_T:
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
        param_spec: Parameter,
        # TODO remove `str` when literal constraint support is removed
        item_constraint_override: Constraint | str | None = None,
        nargs_override: str | int | None = None):
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
    """Base class for `ValidatedInterface` parameter validators

    This class can be used as-is, by declaring individual constraints
    in the constructor, or it can be subclassed to consolidate all
    custom validation-related code for a command in a single place.

    Commonly this constraint is used by declaring particular value constraints
    for individual parameters as a mapping. Declaring that the ``path``
    parameter should receive something that is or can be coerced to
    a valid ``Path`` object looks like this::

      EnsureCommandParameterization({'path': EnsurePath()})

    This class differs from a standard ``Constraint`` implementation,
    because its ``__call__()`` method support additional arguments
    that are used by the internal ``Interface`` handling code to
    control how parameters are validated.

    During validation, when no validator for a particular parameter is
    declared, any input value is passed on as-is, and otherwise an input is
    passed through the validator.

    There is one exception to this rule: When a parameter value is identical to
    its default value (as declared in the command signature, and communicated
    via the ``at_default`` argument of ``__call__()``), this default
    value is also passed as-is, unless the respective parameter name is
    included in the ``validate_defaults`` constructor argument.

    An important consequence of this behavior is that validators need
    not cover a default value. For example, a parameter constraint for
    ``path=None``, where ``None`` is a special value used to indicate an
    optional and unset value, but actually only paths are acceptable input
    values. can simply use ``EnsurePath()`` and it is not necessary to do
    something like ``EnsurePath() | EnsureNone()``.

    However, `EnsureCommandParameterization` can also be specifically
    instructed to perform validation of defaults for individual parameters, as
    described above.  A common use case is the auto-discovery of datasets,
    where often `None` is the default value of a `dataset` parameter (to make
    it optional), and an `EnsureDataset` constraint is used. This constraint
    can perform the auto-discovery (with the `None` value indicating that), but
    validation of defaults must be turned on for the `dataset` parameter in
    order to do that.

    A second difference to a common ``Constraint`` implementation is the
    ability to perform an "exhaustive validation" on request (via
    ``__call__(on_error=...)``). In this case, validation is not stopped at the
    first discovered violation, but all violations are collected and
    communicated by raising a ``CommandParametrizationError`` exception, which
    can be inspected by a caller for details on number and nature of all
    discovered violations.
    """
    def __init__(
        self,
        param_constraints: Dict[str, Constraint],
        *,
        validate_defaults: Container[str] | None = None,
        joint_constraints:
            Dict[ParameterConstraintContext, Callable] | None = None,
    ):
        """
        Parameters
        ----------
        param_constraints: dict
          Mapping of parameter names to parameter constraints. On validation
          an ``EnsureParameterConstraint`` instance will be created for
          each item in this dict.
        validate_defaults: container(str), optional
          If given, this is a set of parameter names for which the default
          rule, to not validate default values, does not apply and
          default values shall be passed through a given validator.
        joint_constraints: dict, optional
          Specification of higher-order constraints considering multiple
          parameters together. See the ``joint_validation()`` method for
          details. Constraints will be processed in the order in which
          they are declared in the mapping. Earlier validators can modify
          the parameter values that are eventually passed to validators
          executed later.
        """
        super().__init__()
        self._param_constraints = param_constraints
        self._joint_constraints = joint_constraints
        self._validate_defaults = validate_defaults or set()

    def joint_validation(self, params: Dict, on_error: str) -> Dict:
        """Higher-order validation considering multiple parameters at a time

        This method is called with all, individually validated, command
        parameters in keyword-argument form in the ``params`` dict argument.

        Arbitrary additional validation steps can be performed on the full
        set of parameters that may involve raising exceptions on validation
        errors, but also value transformation or replacements of individual
        parameters based on the setting of others.

        The parameter values returned by the method are passed on to the
        respective command implementation.

        The default implementation iterates over the ``joint_validators``
        specification given to the constructor, in order to perform
        any number of validations. This is a mapping of a
        ``ParameterConstraintContext`` instance to a callable implementing a
        validation for a particular parameter set.

        Example::

          _joint_validators_ = {
              ParameterConstraintContext(('p1', 'p2'), 'sum'):
                  MyValidator._check_sum,
          }

          def _checksum(self, p1, p2):
              EnsureRange(min=3)(p1 + p2)

        The callable will be passed the arguments named in the
        ``ParameterConstraintContext`` as keyword arguments, using the same
        names as originally given to ``EnsureCommandParameterization``. Any
        raised ``ConstraintError`` is caught and reported together with the
        respective ``ParameterConstraintContext``. If the callable anyhow
        modifies the passed arguments, it must return them as a kwargs-like
        mapping.  If nothing is modified, it is OK to return ``None``.

        Returns
        -------
        dict
          The returned dict must have a value for each item passed in via
          ``params``.
        on_error: {'raise-early', 'raise-at-end'}
          Flag how to handle constraint violation. By default, validation is
          stopped at the first error and an exception is raised. When an
          exhaustive validation is performed, an eventual exception contains
          information on all constraint violations.

        Raises
        ------
        ConstraintErrors
          With `on_error='raise-at-end'` an implementation can choose to
          collect more than one higher-order violation and raise them
          as a `ConstraintErrors` exception.
        """
        # if we have nothing, do nothing
        if not self._joint_constraints:
            return params

        exceptions = {}
        validated = params.copy()

        for ctx, validator in self._joint_constraints.items():
            # what the validator will produce
            res = None
            try:
                # call the validator with the parameters given in the context
                # and only with those, to make sure the context is valid
                # and not an underspecification.
                # pull the values form `validated` to be able to benefit
                # from incremental coercing done in individual checks
                res = validator(**{p: validated[p] for p in ctx.parameters})
            except ConstraintError as e:
                exceptions[ctx] = e
                if on_error == 'raise-early':
                    raise CommandParametrizationError(exceptions)
            if res is not None:
                validated.update(**res)

        if exceptions:
            raise CommandParametrizationError(exceptions)

        return validated

    def __call__(
        self,
        kwargs,
        at_default=None,
        on_error='raise-early',
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
        on_error: {'raise-early', 'raise-at-end'}
          Flag how to handle constraint violation. By default, validation is
          stopped at the first error and an exception is raised. When an
          exhaustive validation is performed, an eventual exception contains
          information on all constraint violations. Regardless of this mode
          more than one error can be reported (in case (future) implementation
          perform independent validations in parallel).

        Raises
        ------
        CommandParametrizationError
          Raised whenever one (or more) ``ConstraintError`` exceptions are
          caught during validation. Other exception types are not caught and
          pass through.
        """
        assert on_error in ('raise-early', 'raise-at-end')
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
                exceptions[ParameterConstraintContext((argname,))] = e
                if on_error == 'raise-early':
                    raise CommandParametrizationError(exceptions)

        try:
            # call (subclass) method to perform holistic, cross-parameter
            # validation of the full parameterization
            final = self.joint_validation(validated, on_error)
            # check requirements of .joint_validation(), a particular
            # implementation could be faulty, and we want to report this
            # problem in the right context
            try:
                assert final.keys() == validated.keys()
            except Exception as e:
                raise RuntimeError(
                    f"{self.__class__.__name__}.joint_validation() "
                    "did not return items for all passed parameters. "
                    "Invalid implementation.") from e
        # we catch the good stuff first. the underlying implementation is
        # providing an exception with detailed context info on possibly
        # multiple errors
        except ParametrizationErrors as e:
            # we can simply suck in the reports, the context keys do not
            # overlap, unless the provided validators want that for some
            # reason
            exceptions.update(e.errors)

        if exceptions:
            raise CommandParametrizationError(exceptions)

        return final
