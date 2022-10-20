
from typing import (
    Any,
    TYPE_CHECKING,
    TypeVar,
)

from .api import aConstraint
from .basic import (
    EnsureBool,
    EnsureChoice,
    EnsureIterableOf,
    EnsureMapping,
    EnsureNone,
    EnsureStr,
    NoConstraint,
)

if TYPE_CHECKING:  # pragma: no cover
    from datalad.support.param import Parameter

aEnsureParameterConstraint = TypeVar(
    'aEnsureParameterConstraint',
    bound='aEnsureParameterConstraint',
)
aParameter = TypeVar('aParameter', bound='Parameter')


class EnsureParameterConstraint(EnsureMapping):
    """Ensures a mapping from a Python parameter name to a value constraint
    """
    # valid parameter name for Python and CLI
    valid_param_name_regex = r'[^0-9][a-z0-0_]+'

    def __init__(self, constraint: aConstraint):
        super().__init__(
            key=EnsureStr(
                match=EnsureParameterConstraint.valid_param_name_regex),
            value=constraint,
            # make it look like dict(...)
            delimiter='=',
        )

    @classmethod
    def from_parameter(
            cls,
            spec: aParameter,
            default: Any,
            item_constraint: aConstraint = None,
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
          A parameter's default value. Any (intermediate) constraint is tested
          whether it considers the default to be a valid value. If not,
          the constraint is automatically expanded to also cover this
          particular value.
        item_constraint:
          If given, it override any constraint declared in the Parameter
          instance given to `spec`
        nargs:
          If given, it override any nargs setting declared in the Parameter
          instance given to `spec`.
        """
        value_constraint = _get_comprehensive_constraint(
            spec,
            default,
            item_constraint,
            nargs,
        )
        return cls(value_constraint)


def _get_comprehensive_constraint(
        param_spec: aParameter,
        default: Any,
        item_constraint_override: aConstraint = None,
        nargs_override: str or int = None):
    action = param_spec.cmd_kwargs.get('action')
    # definitive per-item constraint, consider override
    # otherwise fall back on Parameter.constraints
    constraint = item_constraint_override or param_spec.constraints

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

    # lastly try to validate the default, if that fails
    # wrap into alternative
    try:
        constraint(default)
    except Exception:
        # should be this TODO
        #constraint = constraint | EnsureValue(default)
        # for now
        if default is None:
            constraint = constraint | EnsureNone()

    return constraint


