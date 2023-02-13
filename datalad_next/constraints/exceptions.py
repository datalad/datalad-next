"""Custom exceptions raised by ``Constraint`` implementations"""

from types import MappingProxyType
from typing import (
    Any,
    Dict,
)

# needed for imports in other pieced of the ``constraints`` module
from datalad_next.exceptions import NoDatasetFound


class ConstraintError(ValueError):
    # we derive from ValueError, because it provides the seemingly best fit
    # of any built-in exception. It is defined as:
    #
    #   Raised when an operation or function receives an argument that has
    #   the right type but an inappropriate value, and the situation is not
    #   described by a more precise exception such as IndexError.
    #
    # In general a validation error can also occur because of a TypeError, but
    # ultimately what also matters here is an ability to coerce a given value
    # to a target type/value, but such an exception is not among the built-ins.
    # Moreover, many pieces of existing code do raise ValueError in practice,
    # and we aim to be widely applicable with this specialized class
    """Exception type raised by constraints when their conditions are violated

    A primary purpose of this class is to provide uniform means for
    communicating information on violated constraints.
    """
    def __init__(self,
                 constraint,
                 value: Any,
                 msg: str,
                 ctx: Dict[str, Any] | None = None):
        """
        Parameters
        ----------
        constraint: Constraint
          Instance of the ``Constraint`` class that determined a violation.
        value:
          The value that is in violation of a constraint.
        msg: str
          A message describing the violation. If ``ctx`` is given too, the
          message can contain keyword placeholders in Python's ``format()``
          syntax that will be applied on-access.
        ctx: dict, optional
          Mapping with context information on the violation. This information
          is used to interpolate a message, but may also contain additional
          key-value mappings.
        """
        # the msg/ctx setup is inspired by pydantic
        # we put `msg` in the `.args` container first to match where
        # `ValueError` would have it. Everthing else goes after it.
        super().__init__(msg, constraint, value, ctx)

    @property
    def msg(self):
        """Obtain an (interpolated) message on the contraint violation"""
        if self.args[3]:
            return self.args[0].format(**self.args[3])
        else:
            return self.args[0]

    @property
    def constraint(self):
        """Get the instance of the constraint that was violated"""
        return self.args[1]

    @property
    def value(self):
        """Get the value that violated the constraint"""
        return self.args[2]

    def __repr__(self):
        # rematch constructor arg-order, because we put `msg` first into
        # `.args`
        return '{0}({2!r}, {3!r}, {1!r}, {4!r})'.format(
            self.__class__.__name__,
            *self.args,
        )


class ConstraintErrors(ConstraintError):
    """Exception representing context-specific ConstraintError instances

    This class enables the association of a context in which any particular
    contraint was violated. This is done by passing a mapping, of a context
    identifier (e.g., a label) to the particular ``ConstraintError`` that
    occurred in this context, to the constructor.

    This is a generic implementation with no requirements regarding the
    nature of the context identifiers (expect for being hashable). See
    ``CommandParametrizationError`` for a specialization.
    """
    def __init__(self, exceptions: Dict[Any, ConstraintError]):
        super().__init__(
            # this is the main payload, the base class expects a Constraint
            # but only stores it
            constraint=exceptions,
            # all values are already on record in the respective exceptions
            # no need to pass again
            value=None,
            # no support for a dedicated message here (yet?), pass an empty
            # string to match assumptions
            msg='',
            # and no context
            ctx=None,
        )

    @property
    def errors(self) -> MappingProxyType[Any, ConstraintError]:
        # read-only access
        return MappingProxyType(self.args[1])

    def __repr__(self):
        return '{0}({{{1}}})'.format(
            self.__class__.__name__,
            ', '.join(f'{k!r}={v!r}' for k, v in self.errors.items()),
        )


class ParametrizationError(ConstraintErrors):
    """Exception type raised on violating parameter constraints

    This is a ``ConstraintErrors`` variant that uses parameter names (i.e,
    ``str`` labels) as context identifiers. In addition to individal
    parameter names an additional ``__all__`` identifier is recognized. It
    can be used to record a ``ConstraintError`` arising from high-order
    constraints, such as the violation of "mutually exclusive" requirements
    across more than one parameter.
    """
    def __init__(self, exceptions: Dict[str, ConstraintError]):
        super().__init__(exceptions)

    def __str__(self):
        return self._render_violations_as_indented_text_list(
            'parameter')

    def _render_violations_as_indented_text_list(self, violation_subject):
        violations = len(self.errors)
        return '{ne} {vs}constraint violation{p}\n{el}{joint}'.format(
            ne=violations,
            vs=f'{violation_subject} ' if violation_subject else '',
            p='s' if violations > 1 else '',
            el='\n'.join(
                f'{argname}\n  {c.msg}'
                for argname, c in self.errors.items()
                if argname != '__all__'
            ),
            joint='' if '__all__' not in self.errors
            else '\n\n{}'.format(self.errors['__all__'].msg),
        )


class CommandParametrizationError(ParametrizationError):
    """Exception type raised on violating any command parameter constraints

    .. seealso::

       :mod:`~datalad_next.constraints.parameter.EnsureCommandParameterization`
    """
    def __str__(self):
        return self._render_violations_as_indented_text_list(
            'command parameter')
