import pathlib
import pytest

from datalad.support.param import Parameter

from ..basic import (
    EnsureInt,
    EnsureStr,
    NoConstraint,
)
from ..git import (
    EnsureGitRefName,
)
from ..parameter import EnsureParameterConstraint


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
    with pytest.deprecated_call():
        c = EnsureParameterConstraint.from_parameter(
            Parameter(), 2, item_constraint='float')
    assert c('some=3') == dict(some=3.0)
    with pytest.raises(ValueError), \
            pytest.deprecated_call():
        EnsureParameterConstraint.from_parameter(
            Parameter(), 2, item_constraint='unknown')
