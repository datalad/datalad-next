"""Custom exceptions raised by ``Constraint`` implementations"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from textwrap import indent
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    Tuple,
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
          key-value mappings. A recognized key is ``'__caused_by__'``, with
          a value of one exception (or a list of exceptions) that led to a
          ``ConstraintError`` being raised.
        """
        # the msg/ctx setup is inspired by pydantic
        # we put `msg` in the `.args` container first to match where
        # `ValueError` would have it. Everything else goes after it.
        super().__init__(msg, constraint, value, ctx)

    @property
    def msg(self):
        """Obtain an (interpolated) message on the constraint violation

        The error message template can be interpolated with any information
        available in the error context dict (``ctx``). In addition to the
        information provided by the ``Constraint`` that raised the error,
        the following additional placeholders are provided:

        - ``__value__``: the value reported to have caused the error
        - ``__itemized_causes__``: an indented bullet list str with on
          item for each error in the ``caused_by`` report of the error.

        Message template can use any feature of the Python format mini
        language. For example ``{__value__!r}`` to get a ``repr()``-style
        representation of the offending value.
        """
        msg_tmpl = self.args[0]
        # get interpolation values for message formatting
        # we need a copy, because we need to mutate the dict
        ctx = dict(self.context)
        # support a few standard placeholders
        # the verbatim value that caused the error: with !r and !s both
        # types of stringifications are accessible
        ctx['__value__'] = self.value
        if self.caused_by:
            ctx['__itemized_causes__'] = indent(
                '\n'.join(f'- {str(c)}' for c in self.caused_by),
                "  ",
            )
        return msg_tmpl.format(**ctx)

    @property
    def constraint(self):
        """Get the instance of the constraint that was violated"""
        return self.args[1]

    @property
    def caused_by(self) -> Tuple[Exception] | None:
        """Returns a tuple of any underlying exceptions that caused a violation
        """
        cb = self.context.get('__caused_by__', None)
        if cb is None:
            return
        elif isinstance(cb, Exception):
            return (cb,)
        else:
            return tuple(cb)

    @property
    def value(self):
        """Get the value that violated the constraint"""
        return self.args[2]

    @property
    def context(self) -> MappingProxyType:
        """Get a constraint violation's context

        This is a mapping of key/value-pairs matching the ``ctx`` constructor
        argument.
        """
        return MappingProxyType(self.args[3] or {})

    def __str__(self):
        return self.msg

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
    constraint was violated. This is done by passing a mapping, of a context
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
            ', '.join(f'{k!r}: {v!r}' for k, v in self.errors.items()),
        )


class ParameterContextErrors(Mapping):
    """Read-only convenience that wraps a ``ConstraintErrors`` error mapping
    """
    # TODO extend this class with any query functionality that a command
    # API would want to use in order to get streamlined information on what
    # went wrong (in general, for a specific parameter, etc...)
    def __init__(
        self,
        errors: Dict[ParameterConstraintContext, ConstraintError],
    ):
        self._errors = errors

    def __repr__(self):
        return self._errors.__repr__()

    def __len__(self):
        return len(self._errors)

    def __iter__(self):
        return self._errors.__iter__()

    def __getitem__(self, key):
        return self._errors[key]

    def items(self):
        return self._errors.items()

    @property
    def messages(self):
        return [e.msg for e in self._errors.values()]

    @property
    def context_labels(self):
        return [e.label for e in self._errors.keys()]

    # TODO return all errors related to some parameter


@dataclass(frozen=True)
class ParameterConstraintContext:
    """Representation of a parameter constraint context

    This type is used for the keys in the error map of.
    ``ParametrizationErrors``. Its purpose is to clearly identify which
    parameter combination (and its nature) led to a `ConstraintError`.

    An error context comprises to components: 1) the names of the parameters
    that were considered, and 2) a description of how the parameters were
    linked or combined. In the simple case of an error occurring in the context
    of a single parameter, the second component is superfluous. Otherwise,
    it can be thought of as an operation label, describing what aspect of
    the set of parameters is being relevant in a particular context.

    Example:

    A command has two parameters `p1` and `p2`. The may also have respective
    individual constraints, but importantly they 1) must not have identical
    values, and 2) their sum must be larger than 3. If the command is called
    with ``cmd(p1=1, p2=1)``, both conditions are violated. The reporting may
    be implemented using the following ``ParameterConstraintContext`` and
    ``ConstraintError`` instances::

      ParameterConstraintContext(('p1', 'p2'), 'inequality):
        ConstraintError(EnsureValue(True), False, <EnsureValue error>)

      ParameterConstraintContext(('p1', 'p2'), 'sum):
        ConstraintError(EnsureRange(min=3), False, <EnsureRange error>)

    where the ``ConstraintError`` instances are generated by standard
    ``Constraint`` implementation. For the second error, this could look like::

      EnsureRange(min=3)(params['p1'] + params['p2'])
    """
    parameters: Tuple[str]
    description: str | None = None

    def __str__(self):
        return f'Context<{self.label}>'

    @property
    def label(self) -> str:
        """A concise summary of the context

        This label will be a compact as possible.
        """
        # XXX this could be __str__ but its intended usage for rendering
        # a text description of all errors would seemingly forbid adding
        # type information -- which OTOH seems to be desirable for __str__
        return '{param}{descr}'.format(
            param=", ".join(self.parameters),
            descr=f" ({self.description})" if self.description else '',
        )

    def get_label_with_parameter_values(self, values: dict) -> str:
        """Like ``.label`` but each parameter will also state a value"""
        # TODO truncate the values after repr() to ensure a somewhat compact
        # output
        from .parameter import NoValue
        return '{param}{descr}'.format(
            param=", ".join(
                f'{p}=<no value>'
                if isinstance(values[p], NoValue)
                else f'{p}={values[p]!r}'
                for p in self.parameters
            ),
            descr=f" ({self.description})" if self.description else '',
        )


class ParametrizationErrors(ConstraintErrors):
    """Exception type raised on violating parameter constraints

    This is a ``ConstraintErrors`` variant that uses parameter names (i.e,
    ``str`` labels) as context identifiers. In addition to individual
    parameter names an additional ``__all__`` identifier is recognized. It
    can be used to record a ``ConstraintError`` arising from high-order
    constraints, such as the violation of "mutually exclusive" requirements
    across more than one parameter.
    """
    def __init__(
            self,
            exceptions: Dict[str, ConstraintError] |
                        Dict[ParameterConstraintContext, ConstraintError]):
        super().__init__(
            {k if isinstance(k, ParameterConstraintContext)
             else ParameterConstraintContext((k,)):
             v
             for k, v in exceptions.items()}
        )

    @property
    def errors(self) -> ParameterContextErrors:
        # read-only access
        return ParameterContextErrors(self.args[1])

    def __str__(self):
        return self._render_violations_as_indented_text_list(
            'parameter')

    def _render_violations_as_indented_text_list(self, violation_subject):
        violations = len(self.errors)

        return '{ne} {vs}constraint violation{p}\n{el}'.format(
            ne=violations,
            vs=f'{violation_subject} ' if violation_subject else '',
            p='s' if violations > 1 else '',
            el='\n'.join(
                '{ctx}\n{msg}'.format(
                    ctx=ctx.get_label_with_parameter_values(
                        c.value
                        if isinstance(c.value, dict)
                        else {ctx.parameters[0]: c.value}
                    ),
                    msg=indent(str(c), '  '),
                )
                for ctx, c in self.errors.items()
            ),
        )


class CommandParametrizationError(ParametrizationErrors):
    """Exception type raised on violating any command parameter constraints

    .. seealso::

       :mod:`~datalad_next.constraints.parameter.EnsureCommandParameterization`
    """
    def __str__(self):
        return self._render_violations_as_indented_text_list(
            'command parameter')
