import pathlib
import pytest

from datalad.support.param import Parameter

from ..api import (
    Constraint,
    Constraints,
    AltConstraints,
)
from ..basic import (
    EnsureInt,
    EnsureFloat,
    EnsureBool,
    EnsureStr,
    EnsureStrPrefix,
    EnsureNone,
    EnsureCallable,
    EnsureChoice,
    EnsureKeyChoice,
    EnsureMapping,
    EnsureRange,
    EnsureIterableOf,
    EnsureListOf,
    EnsureTupleOf,
    EnsurePath,
    NoConstraint,
)
from ..git import (
    EnsureGitRefName,
)
from ..parameter import EnsureParameterConstraint

from ..utils import _type_str


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


def test_noconstraint():
    c = NoConstraint()
    assert c(5) == 5
    assert c.short_description() == ''


def test_int():
    c = EnsureInt()
    # this should always work
    assert c(7) == 7
    assert c(7.0) == 7
    assert c('7') == 7
    # no automatic inspection of iterables, should use EnsureIterableOf
    with pytest.raises(TypeError):
        c([7, 3])
    # this should always fail
    with pytest.raises(ValueError):
        c('fail')
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
    # no automatic inspection of iterables, should use EnsureIterableOf
    with pytest.raises(TypeError):
        c([7.0, '3.0'])
    # this should always fail
    with pytest.raises(ValueError):
        c('fail')


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


def test_EnsureStrPrefix():
    c = EnsureStrPrefix('some-')
    c('some-mess') == 'some-mess'
    with pytest.raises(ValueError):
        c('mess')
    assert c.short_description() == 'some-...'
    assert c.long_description() == "value must start with 'some-'"


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
    assert c.short_description() == 'callable'
    assert c.long_description() == 'value must be a callable'
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
    with pytest.raises(ValueError):
        EnsureRange(min=None, max=None)
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


# imported from ancient test code in datalad-core,
# main test is test_EnsureIterableOf
def test_EnsureTupleOf():
    c = EnsureTupleOf(str)
    assert c(['a', 'b']) == ('a', 'b')
    assert c(['a1', 'b2']) == ('a1', 'b2')
    assert c.short_description() == "tuple(<class 'str'>)"


# imported from ancient test code in datalad-core,
# main test is test_EnsureIterableOf
def test_EnsureListOf():
    c = EnsureListOf(str)
    assert c(['a', 'b']) == ['a', 'b']
    assert c(['a1', 'b2']) == ['a1', 'b2']
    assert c.short_description() == "list(<class 'str'>)"


def test_EnsureIterableOf():
    c = EnsureIterableOf(list, int)
    assert c.short_description() == "<class 'list'>(<class 'int'>)"
    assert c.item_constraint == int
    # testing aspects that are not covered by test_EnsureListOf
    tgt = [True, False, True]
    assert EnsureIterableOf(list, bool)((1, 0, 1)) == tgt
    assert EnsureIterableOf(list, bool, min_len=3, max_len=3)((1, 0, 1)) == tgt
    with pytest.raises(ValueError):
        # too many items
        EnsureIterableOf(list, bool, max_len=2)((1, 0, 1))
    with pytest.raises(ValueError):
        # too few items
        EnsureIterableOf(list, bool, min_len=4)((1, 0, 1))
    with pytest.raises(ValueError):
        # invalid specification min>max
        EnsureIterableOf(list, bool, min_len=1, max_len=0)
    with pytest.raises(TypeError):
        # item_constraint fails
        EnsureIterableOf(list, dict)([5.6, 3.2])
    with pytest.raises(ValueError):
        # item_constraint fails
        EnsureIterableOf(list, EnsureBool())([5.6, 3.2])

    seq = [3.3, 1, 2.6]

    def _mygen():
        for i in seq:
            yield i

    def _myiter(iter):
        for i in iter:
            yield i

    # feeding a generator into EnsureIterableOf and getting one out
    assert list(EnsureIterableOf(_myiter, int)(_mygen())) == [3, 1, 2]


def test_constraints():
    # this should always work
    c = Constraints(EnsureFloat())
    assert c(7.0) == 7.0
    c = Constraints(EnsureFloat(), EnsureRange(min=4.0))
    assert c(7.0) == 7.0
    # __and__ form
    c = EnsureFloat() & EnsureRange(min=4.0)
    assert c.short_description() == '(float and not less than 4.0)'
    assert 'and not less than 4.0' in c.long_description()
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
    # smoke test concat AND constraints
    c = Constraints(EnsureRange(max=10), EnsureRange(min=5)) & \
            Constraints(EnsureRange(max=6), EnsureRange(min=2))
    assert c(6) == 6
    with pytest.raises(ValueError):
        c(4)


def test_altconstraints():
    # this should always work
    c = AltConstraints(EnsureFloat())
    assert c(7.0) == 7.0
    c = AltConstraints(EnsureFloat(), EnsureNone())
    assert c.short_description(), '(float or None)'
    assert c(7.0) == 7.0
    assert c(None) is None
    # OR with an alternative just extends
    c = c | EnsureInt()
    assert c.short_description(), '(float or None or int)'
    # OR with an alternative combo also extends
    c = c | AltConstraints(EnsureBool(), EnsureInt())
    # yes, no de-duplication
    assert c.short_description(), '(float or None or int or bool or int)'
    # spot check long_description, must have some number
    assert len(c.long_description().split(' or ')) == 5
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


def test_EnsurePath(tmp_path):
    target = pathlib.Path(tmp_path)

    assert EnsurePath()(tmp_path) == target
    assert EnsurePath(lexists=True)(tmp_path) == target
    with pytest.raises(ValueError):
        EnsurePath(lexists=False)(tmp_path)
    with pytest.raises(ValueError):
        EnsurePath(lexists=True)(tmp_path / 'nothere')
    assert EnsurePath(is_format='absolute')(tmp_path) == target
    with pytest.raises(ValueError):
        EnsurePath(is_format='relative')(tmp_path)
    with pytest.raises(ValueError):
        EnsurePath(is_format='absolute')(tmp_path.name)
    from stat import S_ISDIR, S_ISREG
    assert EnsurePath(is_mode=S_ISDIR)(tmp_path) == target
    with pytest.raises(ValueError):
        EnsurePath(is_mode=S_ISREG)(tmp_path)
    # give particular path type
    assert EnsurePath(path_type=pathlib.PurePath
        )(tmp_path) == pathlib.PurePath(tmp_path)
    # not everything is possible, this is known and OK
    with pytest.raises(AttributeError):
        EnsurePath(
            path_type=pathlib.PurePath,
            is_mode=S_ISREG,
        )(tmp_path)
    assert EnsurePath().short_description() == 'path'
    assert EnsurePath(is_format='absolute').short_description() == 'absolute path'
    # default comparison mode is parent-or-same-as
    c = EnsurePath(ref=target)
    assert c(target) == target
    assert c(target / 'some') == target / 'some'
    with pytest.raises(ValueError):
        assert c(target.parent)
    c = EnsurePath(ref=target, ref_is='parent-of')
    assert c(target / 'some') == target / 'some'
    with pytest.raises(ValueError):
        assert c(target)
    assert c.short_description() == f'path that is parent-of {target}'


def test_EnsureMapping():
    true_key = 5
    true_value = False

    constraint = EnsureMapping(EnsureInt(), EnsureBool(), delimiter='::')

    assert 'mapping of int -> bool' in constraint.short_description()

    # must all work
    for v in ('5::no',
              [5, 'false'],
              ('5', False),
              {'5': 'False'},
    ):
        d = constraint(v)
        assert isinstance(d, dict)
        assert len(d) == 1
        k, v = d.popitem()
        assert k == true_key
        assert v == true_value

    # must all fail
    for v in ('5',
              [],
              tuple(),
              {},
              # additional value
              [5, False, False],
              {'5': 'False', '6': True}):
        with pytest.raises(ValueError):
            d = constraint(v)

    # TODO test for_dataset() once we have a simple EnsurePathInDataset


def test_EnsureGitRefName():
    assert EnsureGitRefName().short_description() == '(single-level) Git refname'
    # standard branch name must work
    assert EnsureGitRefName()('main') == 'main'
    # normalize is on by default
    assert EnsureGitRefName()('/main') == 'main'
    with pytest.raises(ValueError):
        EnsureGitRefName(normalize=False)('/main')
    assert EnsureGitRefName(normalize=False)('main') == 'main'
    # no empty
    with pytest.raises(ValueError):
        EnsureGitRefName()('')
    with pytest.raises(ValueError):
        EnsureGitRefName()(None)
    # be able to turn off onelevel
    with pytest.raises(ValueError):
        EnsureGitRefName(allow_onelevel=False)('main')
    assert EnsureGitRefName(allow_onelevel=False)(
        'refs/heads/main') == 'refs/heads/main'
    # refspec pattern off by default
    with pytest.raises(ValueError):
        EnsureGitRefName()('refs/heads/*')
    assert EnsureGitRefName(refspec_pattern=True)(
        'refs/heads/*') == 'refs/heads/*'


def test_EnsureParameterConstraint():
    # most basic case, no value constraint
    c = EnsureParameterConstraint(NoConstraint())
    # invalid name
    with pytest.raises(ValueError):
        c({'4way': 123})
    assert c('some=value') == dict(some='value')
    # now some from a standard Parameter declaration
    c = EnsureParameterConstraint.from_parameter(
        Parameter(), 'whateverdefault')
    assert c('some=value') == dict(some='value')


    # want a bool
    c = EnsureParameterConstraint.from_parameter(
        Parameter(action="store_true"),
        False)
    assert c('some=off') == dict(some=False)
    with pytest.raises(ValueError):
        c('some=5')
    c = EnsureParameterConstraint.from_parameter(
        # argparse specific choice declaration without
        # any constraint
        Parameter(choices=['a', 'b']),
        # but specifically use a default that is not a valid choice
        None)
    assert c('choice=a') == dict(choice='a')
    # default is valid too
    assert c({'choice': None}) == dict(choice=None)
    # multi-item values
    c = EnsureParameterConstraint.from_parameter(
        Parameter(nargs=2),
        (None, None))
    assert c({'some': [3, 4]}) == dict(some=[3, 4])
    with pytest.raises(TypeError):
        c({'some': 3})
    with pytest.raises(ValueError):
        c({'some': [3, 4, 5]})
    # one or more items
    c = EnsureParameterConstraint.from_parameter(
        Parameter(nargs='*'),
        None)
    # always prefers a list, no item type conversion by default
    assert c('some=5') == dict(some=['5'])
    assert c({'some': [5, 2]}) == dict(some=[5, 2])
    # empty ok
    assert c({'some': []}) == dict(some=[])
    # at least one item
    c = EnsureParameterConstraint.from_parameter(
        Parameter(nargs='+', constraints=EnsureInt()),
        None)
    assert c('some=5') == dict(some=[5])
    assert c({'some': [5, 2]}) == dict(some=[5, 2])
    # empty not ok
    with pytest.raises(ValueError):
        c({'some': []})
    # complex case of iterables of length 2
    c = EnsureParameterConstraint.from_parameter(
        Parameter(nargs=2, constraints=EnsureInt(), action='append'),
        None)
    # no iterable does not violate
    assert c({'some': []}) == dict(some=[])
    assert c({'some': [[3, 2]]}) == dict(some=[[3, 2]])
    assert c({'some': [[3, 2], [5, 4]]}) == dict(some=[[3, 2], [5, 4]])
    # length mismatch
    with pytest.raises(ValueError):
        c({'some': [[3, 2], [1]]})
    # no iterable
    with pytest.raises(ValueError):
        c({'some': [3, [1, 2]]})
    with pytest.raises(ValueError):
        c({'some': 3})
    # overwrite an item constraint and nargs
    c = EnsureParameterConstraint.from_parameter(
        Parameter(nargs=2, constraints=EnsureInt(), action='append'),
        None,
        item_constraint=EnsureStr(),
        nargs=1)
    assert c({'some': ['5']}) == dict(some=['5'])
    # literal constraint label
    # this is no longer supported, but still works: test until removed
    c = EnsureParameterConstraint.from_parameter(
        Parameter(), 2, item_constraint='float')
    assert c('some=3') == dict(some=3.0)
    with pytest.raises(ValueError):
        EnsureParameterConstraint.from_parameter(
            Parameter(), 2, item_constraint='unknown')
