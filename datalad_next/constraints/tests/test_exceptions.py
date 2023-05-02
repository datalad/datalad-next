from types import MappingProxyType

from ..basic import EnsureInt
from ..exceptions import (
    CommandParametrizationError,
    ConstraintError,
    ConstraintErrors,
    ParameterConstraintContext,
    ParameterContextErrors,
    ParametrizationErrors,
)


def test_constrainterror_repr():
    c = EnsureInt()
    ce = ConstraintError(c, 'noint', 'yeah, bullshit')
    assert repr(ce) == \
        f"ConstraintError({c!r}, 'noint', 'yeah, bullshit', None)"


def test_constrainterrors():
    c = EnsureInt()
    ce = ConstraintError(c, 'noint', 'yeah, bullshit')
    emap = dict(c1=ce)
    ces = ConstraintErrors(emap)
    assert ces.errors == emap
    assert isinstance(ces.errors, MappingProxyType)
    assert repr(ces) == f"ConstraintErrors({emap!r})"


def test_parametercontext():
    assert str(ParameterConstraintContext(('p1',))) == 'Context<p1>'
    assert str(ParameterConstraintContext(
        ('p1', 'p2'),
        'some details',
    )) == 'Context<p1, p2 (some details)>'


def test_parametercontexterrors():
    c = EnsureInt()
    ce = ConstraintError(c, 'noint', 'yeah, bullshit')
    emap = {
        ParameterConstraintContext(('c1',)): ce,
    }
    pces = ParameterContextErrors(emap)
    assert pces.items() == emap.items()
    assert repr(pces) == repr(emap)


def test_parameterizationerrors():
    c = EnsureInt()
    ce = ConstraintError(c, 'noint', 'yeah, bullshit')
    emap = {
        ParameterConstraintContext(('c1',)): ce,
    }
    pes = ParametrizationErrors(emap)
    assert str(pes) == """\
1 parameter constraint violation
c1='noint'
  yeah, bullshit"""

    # CommandParametrizationError is pretty much the same thing
    cpes = CommandParametrizationError(emap)
    assert str(cpes) == """\
1 command parameter constraint violation
c1='noint'
  yeah, bullshit"""
