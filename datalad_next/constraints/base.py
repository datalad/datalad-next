""""""
__docformat__ = 'restructuredtext'

from typing import (
    TYPE_CHECKING,
    TypeVar,
)

if TYPE_CHECKING:  # pragma: no cover
    from datalad_next.datasets import Dataset

ConstraintDerived = TypeVar('ConstraintDerived', bound='Constraint')
DatasetDerived = TypeVar('DatasetDerived', bound='Dataset')


class Constraint:
    """Base class for input value conversion/validation.

    These classes are also meant to be able to generate appropriate
    documentation on an appropriate parameter value.
    """

    # TODO: __str__ and/or __repr__ for every one of them

    def __repr__(self):
        """Rudimentary repr to avoid default scary to the user Python repr"""
        return "constraint:%s" % self.short_description()

    def __and__(self, other):
        return Constraints(self, other)

    def __or__(self, other):
        return AltConstraints(self, other)

    def __call__(self, value):
        # do any necessary checks or conversions, potentially catch exceptions
        # and generate a meaningful error message
        raise NotImplementedError("abstract class")

    def long_description(self):
        # return meaningful docs or None
        # used as a comprehensive description in the parameter list
        return self.short_description()

    def short_description(self):
        # return meaningful docs or None
        # used as a condensed primer for the parameter lists
        raise NotImplementedError("abstract class")

    def for_dataset(self, dataset: DatasetDerived) -> ConstraintDerived:
        """Return a constraint-variant for a specific dataset context

        The default implementation returns the unmodified, identical
        constraint. However, subclasses can implement different behaviors.
        """
        return self


class _MultiConstraint(Constraint):
    """Helper class to override the description methods to reported
    multiple constraints
    """
    def _get_description(self, attr: str, operation: str) -> str:
        cs = [
            getattr(c, attr)()
            for c in self.constraints
            if hasattr(c, attr)
        ]
        cs = [c for c in cs if c is not None]
        doc = f' {operation} '.join(cs)
        if len(cs) > 1:
            return f'({doc})'
        else:
            return doc


class AltConstraints(_MultiConstraint):
    """Logical OR for constraints.

    An arbitrary number of constraints can be given. They are evaluated in the
    order in which they were specified. The value returned by the first
    constraint that does not raise an exception is the global return value.

    Documentation is aggregated for all alternative constraints.
    """
    def __init__(self, *constraints):
        """
        Parameters
        ----------
        *constraints
           Alternative constraints
        """
        super(AltConstraints, self).__init__()
        # TODO Why is EnsureNone needed? Remove if possible
        from .basic import EnsureNone
        self.constraints = [
            EnsureNone() if c is None else c for c in constraints
        ]

    def __or__(self, other):
        if isinstance(other, AltConstraints):
            self.constraints.extend(other.constraints)
        else:
            self.constraints.append(other)
        return self

    def __call__(self, value):
        e_list = []
        for c in self.constraints:
            try:
                return c(value)
            except Exception as e:
                e_list.append(e)
        raise ValueError(
            f"{value!r} violated all possible constraints {self.constraints}")

    def long_description(self):
        return self._get_description('long_description', 'or')

    def short_description(self):
        return self._get_description('short_description', 'or')


class Constraints(_MultiConstraint):
    """Logical AND for constraints.

    An arbitrary number of constraints can be given. They are evaluated in the
    order in which they were specified. The return value of each constraint is
    passed an input into the next. The return value of the last constraint
    is the global return value. No intermediate exceptions are caught.

    Documentation is aggregated for all constraints.
    """
    def __init__(self, *constraints):
        """
        Parameters
        ----------
        *constraints
           Constraints all of which must be satisfied
        """
        super(Constraints, self).__init__()
        # TODO Why is EnsureNone needed? Remove if possible
        from .basic import EnsureNone
        self.constraints = [
            EnsureNone() if c is None else c for c in constraints
        ]

    def __and__(self, other):
        if isinstance(other, Constraints):
            self.constraints.extend(other.constraints)
        else:
            self.constraints.append(other)
        return self

    def __call__(self, value):
        for c in (self.constraints):
            value = c(value)
        return value

    def long_description(self):
        return self._get_description('long_description', 'and')

    def short_description(self):
        return self._get_description('short_description', 'and')
