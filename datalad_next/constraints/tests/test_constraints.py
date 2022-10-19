import pytest

from ..api import (
    Constraints,
    AltConstraints,
)
from ..basic import (
    EnsureInt,
    EnsureFloat,
    EnsureBool,
    EnsureStr,
    EnsureNone,
    EnsureCallable,
    EnsureChoice,
    EnsureKeyChoice,
    EnsureRange,
    EnsureListOf,
    EnsureTupleOf,
)
from ..utils import _type_str


def test_int():
    c = EnsureInt()
    # this should always work
    assert c(7) == 7
    assert c(7.0) == 7
    assert c('7') == 7
    assert c([7, 3]) == [7, 3]
    # this should always fail
    with pytest.raises(ValueError):
        c('fail')
    with pytest.raises(ValueError):
        c([3, 'fail'])
    # this will also fail
    with pytest.raises(ValueError):
        c('17.0')
    assert c.short_description() == 'int'


def test_float():
    c = EnsureFloat()
    # this should always work
    assert c(7.0) == 7.0
    assert c(7) == 7.0
    assert c('7') == 7.0
    assert c([7.0, '3.0']) == [7.0, 3.0]
    # this should always fail
    with pytest.raises(ValueError):
        c('fail')
    with pytest.raises(ValueError):
        c([3.0, 'fail'])


def test_bool():
    c = EnsureBool()
    # this should always work
    assert c(True) is True
    assert c(False) is False
    # all that resuls in True
    assert c('True') is True
    assert c('true') is True
    assert c('1') is True
    assert c('yes') is True
    assert c('on') is True
    assert c('enable') is True
    # all that resuls in False
    assert c('false') is False
    assert c('False') is False
    assert c('0') is False
    assert c('no') is False
    assert c('off') is False
    assert c('disable') is False
    # this should always fail
    with pytest.raises(ValueError):
        c(0)
    with pytest.raises(ValueError):
        c(1)


def test_str():
    c = EnsureStr()
    # this should always work
    assert c('hello') == 'hello'
    assert c('7.0') == '7.0'
    # this should always fail
    with pytest.raises(ValueError):
        c(['ab'])
    with pytest.raises(ValueError):
        c(['a', 'b'])
    with pytest.raises(ValueError):
        c(('a', 'b'))
    # no automatic conversion attempted
    with pytest.raises(ValueError):
        c(7.0)
    assert c.short_description() == 'str'


def test_str_min_len():
    c = EnsureStr(min_len=1)
    assert c('hello') == 'hello'
    assert c('h') == 'h'
    with pytest.raises(ValueError):
        c('')

    c = EnsureStr(min_len=2)
    assert c('hello') == 'hello'
    with pytest.raises(ValueError):
        c('h')


def test_EnsureStr_match():
    # alphanum plus _ and ., non-empty
    pattern = '[a-zA-Z0-9-.]+'
    constraint = EnsureStr(match=pattern)

    # reports the pattern in the description
    for m in (constraint.short_description, constraint.long_description):
        assert pattern in m()

    # must work
    assert constraint('a0F-2.') == 'a0F-2.'

    for v in ('', '123_abc'):
        with pytest.raises(ValueError):
            assert constraint('')


def test_none():
    c = EnsureNone()
    # this should always work
    assert c(None) is None
    # this should always fail
    with pytest.raises(ValueError):
        c('None')
    with pytest.raises(ValueError):
        c([])


def test_callable():
    c = EnsureCallable()
    # this should always work
    assert c(range) == range
    with pytest.raises(ValueError):
        c('range')


def test_choice():
    c = EnsureChoice('choice1', 'choice2', None)
    # this should always work
    assert c('choice1') == 'choice1'
    assert c(None) is None
    # this should always fail
    with pytest.raises(ValueError):
        c('fail')
    with pytest.raises(ValueError):
        c('None')


def test_keychoice():
    c = EnsureKeyChoice(key='some', values=('choice1', 'choice2', None))
    assert c({'some': 'choice1'}) == {'some': 'choice1'}
    assert c({'some': None}) == {'some': None}
    assert c({'some': None, 'ign': 'ore'}) == {'some': None, 'ign': 'ore'}
    with pytest.raises(ValueError):
        c('fail')
    with pytest.raises(ValueError):
        c('None')
    with pytest.raises(ValueError):
        c({'nope': 'None'})
    with pytest.raises(ValueError):
        c({'some': 'None'})
    with pytest.raises(ValueError):
        c({'some': ('a', 'b')})


def test_range():
    c = EnsureRange(min=3, max=7)
    # this should always work
    assert c(3.0) == 3.0

    # this should always fail
    with pytest.raises(ValueError):
        c(2.9999999)
    with pytest.raises(ValueError):
        c(77)
    with pytest.raises(TypeError):
        c('fail')
    with pytest.raises(TypeError):
        c((3, 4))
    # since no type checks are performed
    with pytest.raises(TypeError):
        c('7')

    # Range doesn't have to be numeric
    c = EnsureRange(min="e", max="qqq")
    assert c('e') == 'e'
    assert c('fa') == 'fa'
    assert c('qq') == 'qq'
    with pytest.raises(ValueError):
        c('a')
    with pytest.raises(ValueError):
        c('qqqa')


def test_listof():
    c = EnsureListOf(str)
    assert c(['a', 'b']) == ['a', 'b']
    assert c(['a1', 'b2']) == ['a1', 'b2']


def test_tupleof():
    c = EnsureTupleOf(str)
    assert c(('a', 'b')) == ('a', 'b')
    assert c(('a1', 'b2')) == ('a1', 'b2')


def test_constraints():
    # this should always work
    c = Constraints(EnsureFloat())
    assert c(7.0) == 7.0
    c = Constraints(EnsureFloat(), EnsureRange(min=4.0))
    assert c(7.0) == 7.0
    # __and__ form
    c = EnsureFloat() & EnsureRange(min=4.0)
    assert c(7.0) == 7.0
    with pytest.raises(ValueError):
        c(3.9)
    c = Constraints(EnsureFloat(), EnsureRange(min=4), EnsureRange(max=9))
    assert c(7.0) == 7.0
    with pytest.raises(ValueError):
        c(3.9)
    with pytest.raises(ValueError):
        c(9.01)
    # __and__ form
    c = EnsureFloat() & EnsureRange(min=4) & EnsureRange(max=9)
    assert c(7.0) == 7.0
    with pytest.raises(ValueError):
        c(3.99)
    with pytest.raises(ValueError):
        c(9.01)
    # and reordering should not have any effect
    c = Constraints(EnsureRange(max=4), EnsureRange(min=9), EnsureFloat())
    with pytest.raises(ValueError):
        c(3.99)
    with pytest.raises(ValueError):
        c(9.01)


def test_altconstraints():
    # this should always work
    c = AltConstraints(EnsureFloat())
    assert c(7.0) == 7.0
    c = AltConstraints(EnsureFloat(), EnsureNone())
    assert c.short_description(), '(float or None)'
    assert c(7.0) == 7.0
    assert c(None) is None
    # __or__ form
    c = EnsureFloat() | EnsureNone()
    assert c(7.0) == 7.0
    assert c(None) is None

    # this should always fail
    c = Constraints(EnsureRange(min=0, max=4), EnsureRange(min=9, max=11))
    with pytest.raises(ValueError):
        c(7.0)
    c = EnsureRange(min=0, max=4) | EnsureRange(min=9, max=11)
    assert c(3.0) == 3.0
    assert c(9.0) == 9.0
    with pytest.raises(ValueError):
        c(7.0)
    with pytest.raises(ValueError):
        c(-1.0)


def test_both():
    # this should always work
    c = AltConstraints(
        Constraints(
            EnsureFloat(),
            EnsureRange(min=7.0, max=44.0)),
        EnsureNone())
    assert c(7.0) == 7.0
    assert c(None) is None
    # this should always fail
    with pytest.raises(ValueError):
        c(77.0)


def test_type_str():
    assert _type_str((str,)) == 'str'
    assert _type_str(str) == 'str'
