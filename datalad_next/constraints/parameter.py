"""Constraints for command/function parameters"""

from __future__ import annotations

from collections.abc import Container
from itertools import chain
from typing import (
    Callable,
    Dict,
)

from .base import Constraint
from .basic import (
    NoConstraint,
)
from .dataset import DatasetParameter
from .exceptions import (
    ConstraintError,
    ParametrizationErrors,
    CommandParametrizationError,
    ParameterConstraintContext,
)


class NoValue:
    """Type to annotate the absence of a value

    For example in a list of parameter defaults. In general `None` cannot
    be used, as it may be an actual value, hence we use a local, private
    type.
    """
    pass


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

    Exhaustive validation and joint reporting are only supported for individual
    constraint implementations that raise `ConstraintError` exceptions. For
    legacy constraints, any raised exception of another type are not caught
    and reraised immediately.
    """
    def __init__(
        self,
        param_constraints: Dict[str, Constraint],
        *,
        validate_defaults: Container[str] | None = None,
        joint_constraints:
            Dict[ParameterConstraintContext, Callable] | None = None,
        tailor_for_dataset: Dict[str, str] | None = None,
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
        tailor_for_dataset: dict, optional
          If given, this is a mapping of a name of a parameter whose
          constraint should be tailored to a particular dataset, to a name
          of a parameter providing this dataset. The dataset-providing
          parameter constraints will be evaluated first, and the resulting
          Dataset instances are used to tailor the constraints that
          require a dataset-context. The tailoring is performed if, and
          only if, the dataset-providing parameter actually evaluated
          to a `Dataset` instance. The non-tailored constraint is used
          otherwise.
        """
        super().__init__()
        self._param_constraints = param_constraints
        self._joint_constraints = joint_constraints
        self._validate_defaults = validate_defaults or set()
        self._tailor_for_dataset = tailor_for_dataset or {}

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
              if (p1 + p2) < 3:
                  self.raise_for(
                     dict(p1=p1, p2=p2),
                     'parameter sum is too large',
                  )

        The callable will be passed the arguments named in the
        ``ParameterConstraintContext`` as keyword arguments, using the same
        names as originally given to ``EnsureCommandParameterization``.

        Any raised ``ConstraintError`` is caught and reported together with the
        respective ``ParameterConstraintContext``. The violating value reported
        in such a ``ConstraintError`` must be a mapping of parameter name to
        value, comprising the full parameter set (i.e., keys matching the
        ``ParameterConstraintContext``).  The use of ``self.raise_for()`` is
        encouraged.

        If the callable anyhow modifies the passed arguments, it must return
        them as a kwargs-like mapping.  If nothing is modified, it is OK to
        return ``None``.

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
                if not isinstance(e.value, dict) \
                        or set(ctx.parameters) != e.value.keys():  # pragma: no cover
                    raise RuntimeError(
                        'on raising a ConstraintError the joint validator '
                        f'{validator} did not report '
                        'a mapping of parameter name to (violating) value '
                        'comprising all constraint context parameters. '
                        'This is a software defect of the joint validator. '
                        'Please report!')
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
        required=None,
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
        required: set or None
          Set of parameter names that are known to be required.
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
        missing_args = tuple(a for a in (required or []) if a not in kwargs)
        if missing_args:
            exceptions[ParameterConstraintContext(missing_args)] = \
                ConstraintError(
                    self,
                    dict(zip(missing_args, [NoValue()] * len(missing_args))),
                    'missing required arguments',
                )
            if on_error == 'raise-early':
                raise CommandParametrizationError(exceptions)

        # validators to work with. make a copy of the dict to be able to tailor
        # them for this run only
        # TODO copy likely not needed
        param_constraints = self._param_constraints.copy()

        # names of parameters we need to process
        to_validate = set(kwargs)
        # check for any dataset that are required for tailoring other parameters
        ds_provider_params = set(self._tailor_for_dataset.values())
        # take these out of the set of parameters to validate, because we need
        # to process them first.
        # the approach is to simply sort them first, but otherwise apply standard
        # handling
        to_validate.difference_update(ds_provider_params)
        # strip all args provider args that have not been provided
        ds_provider_params.intersection_update(kwargs)

        validated = {}
        # process all parameters. starts with those that are needed as
        # dependencies for others.
        # this dependency-based sorting is very crude for now. it does not
        # consider possible dependencies within `ds_provider_params` at all
        for argname in chain(ds_provider_params, to_validate):
            arg = kwargs[argname]
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

            # look-up validator for this parameter, if there is none use
            # NoConstraint to avoid complex conditionals in the code below
            validator = param_constraints.get(argname, NoConstraint())

            # do we need to tailor this constraint for a specific dataset?
            # only do if instructed AND the respective other parameter
            # validated to a Dataset instance. Any such parameter was sorted
            # to be validated first in this loop, so the outcome of that is
            # already available
            tailor_for = self._tailor_for_dataset.get(argname)
            if tailor_for and isinstance(validated.get(tailor_for),
                                         DatasetParameter):
                validator = validator.for_dataset(validated[tailor_for])

            try:
                validated[argname] = validator(arg)
            # we catch only ConstraintError -- only these exceptions have what
            # we need for reporting. If any validator chooses to raise
            # something else, we do not handle it here, but let it bubble up.
            # it may be an indication of something being wrong with validation
            # itself
            except ConstraintError as e:
                # standard exception type, record and proceed
                exceptions[ParameterConstraintContext((argname,))] = e
                if on_error == 'raise-early':
                    raise CommandParametrizationError(exceptions)
            except Exception as e:
                # non-standard exception type
                # we need to achieve uniform CommandParametrizationError
                # raising, so let's create a ConstraintError for this
                # exception
                e = ConstraintError(
                    validator, arg, '{__caused_by__}',
                    ctx=dict(__caused_by__=e),
                )
                exceptions[ParameterConstraintContext((argname,))] = e
                if on_error == 'raise-early':
                    raise CommandParametrizationError(exceptions)

        # do not bother with joint validation when the set of expected
        # arguments is not complete
        expected_for_joint_validation = set()
        for jv in self._joint_constraints or []:
            expected_for_joint_validation.update(jv.parameters)

        if not expected_for_joint_validation.issubset(validated):
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
