import pytest

from ..base import (
    Constraint,
    AllOf,
    AnyOf,
)
from ..basic import (
    EnsureDType,
    EnsureInt,
    EnsureFloat,
    EnsureBool,
    IsNone,
    IsRange,
    IsStr,
)


def test_base():
    # there is no "standard" implementation
    with pytest.raises(NotImplementedError):
        Constraint()('whatever')
    # no standard docs
    with pytest.raises(NotImplementedError):
        Constraint().short_description()
    with pytest.raises(NotImplementedError):
        Constraint().long_description()
    # dataset context switching is by default a no-op
    generic = Constraint()
    assert id(generic) == id(generic.for_dataset('some'))


def test_constraints():
    # this should always work
    c = AllOf(EnsureFloat())
    assert c(7.0) == 7.0
    c = AllOf(EnsureFloat(), IsRange(min=4.0))
    assert c(7.0) == 7.0
    # __and__ form
    c = EnsureFloat() & IsRange(min=4.0)
    assert c.short_description() == '(float and not less than 4.0)'
    assert 'and not less than 4.0' in c.long_description()
    assert c(7.0) == 7.0
    with pytest.raises(ValueError):
        c(3.9)
    c = AllOf(EnsureFloat(), IsRange(min=4), IsRange(max=9))
    assert c(7.0) == 7.0
    with pytest.raises(ValueError):
        c(3.9)
    with pytest.raises(ValueError):
        c(9.01)
    # __and__ form
    c = EnsureFloat() & IsRange(min=4) & IsRange(max=9)
    assert c(7.0) == 7.0
    with pytest.raises(ValueError):
        c(3.99)
    with pytest.raises(ValueError):
        c(9.01)
    # and reordering should not have any effect
    c = AllOf(IsRange(max=4), IsRange(min=9), EnsureFloat())
    with pytest.raises(ValueError):
        c(3.99)
    with pytest.raises(ValueError):
        c(9.01)
    # smoke test concat AND constraints
    c1 = AllOf(IsRange(max=10), IsRange(min=5))
    c2 = AllOf(IsRange(max=6), IsRange(min=2))
    c = c1 & c2
    # make sure that neither c1, nor c2 is modified
    assert len(c1.constraints) == 2
    assert len(c2.constraints) == 2
    assert len(c.constraints) == 4
    assert c(6) == 6
    with pytest.raises(ValueError):
        c(4)


def test_altconstraints():
    # this should always work
    c = AnyOf(EnsureFloat())
    # passes the docs through
    assert c.short_description() == EnsureFloat().short_description()
    assert c(7.0) == 7.0
    c = AnyOf(EnsureFloat(), IsNone())
    # wraps docs in parenthesis to help appreciate the scope of the
    # OR'ing
    assert c.short_description().startswith(
        f'({EnsureFloat().short_description()}')
    assert c.short_description(), '(float or None)'
    assert c(7.0) == 7.0
    assert c(None) is None
    # OR with an alternative just extends
    c = c | EnsureInt()
    assert c.short_description(), '(float or None or int)'
    # OR with an alternative combo also extends
    c = c | AnyOf(EnsureBool(), EnsureInt())
    # yes, no de-duplication
    assert c.short_description(), '(float or None or int or bool or int)'
    # spot check long_description, must have some number
    assert len(c.long_description().split(' or ')) == 5
    # __or__ form
    c = EnsureFloat() | IsNone()
    assert c(7.0) == 7.0
    assert c(None) is None

    # this should always fail
    c = AllOf(IsRange(min=0, max=4), IsRange(min=9, max=11))
    with pytest.raises(ValueError):
        c(7.0)
    c = IsRange(min=0, max=4) | IsRange(min=9, max=11)
    assert c(3.0) == 3.0
    assert c(9.0) == 9.0
    with pytest.raises(ValueError):
        c(7.0)
    with pytest.raises(ValueError):
        c(-1.0)

    # verify no inplace modification
    c1 = EnsureInt() | IsStr()
    c2 = c1 | EnsureDType(c1)
    # OR'ing does not "append" the new alternative to c1.
    assert len(c1.constraints) == 2
    # at the same time, c2 does not contain an AnyOf
    # as an internal constraint, because this would be needless
    # complexity re the semantics of OR
    assert len(c2.constraints) == 3


def test_both():
    # this should always work
    c = AnyOf(
        AllOf(
            EnsureFloat(),
            IsRange(min=7.0, max=44.0)),
        IsNone(),
    )
    assert c(7.0) == 7.0
    assert c(None) is None
    # this should always fail
    with pytest.raises(ValueError):
        c(77.0)
