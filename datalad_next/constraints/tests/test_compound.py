from inspect import isgenerator
from io import StringIO
import pytest
from tempfile import NamedTemporaryFile
from unittest.mock import patch
from pathlib import Path

from datalad_next.datasets import Dataset
from datalad_next.utils import on_windows

from ..base import DatasetParameter

from ..basic import (
    EnsureInt,
    EnsureBool,
    EnsurePath,
    EnsureStr,
)
from ..compound import (
    ConstraintWithPassthrough,
    EnsureIterableOf,
    EnsureListOf,
    EnsureTupleOf,
    EnsureNTuple,
    EnsureMapping,
    EnsureMappings,
    EnsureGeneratorFromFileLike,
)


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
    # check string splitting
    c = EnsureIterableOf(list, int, delimiter=':')
    assert c('1:2:3:4:5') == [1, 2, 3, 4, 5]


def test_EnsureNTuple(tmp_path):
    true_res = (1, 2, 3, 4, 5)
    c = EnsureNTuple(itemconstraints=[EnsureInt()]*5, delimiter=':')
    for v in [
        [1, 2, 3, 4, 5],   # test string splitting
        '1:2:3:4:5',   # test string splitting
    ]:
        assert c(v) == true_res

    # these should fail
    for v in [
        # too few values given the constraints
        [1, 2, 3],
        '1:2:3',
        1,
        # wrong delimiter
        '1,2,3,4,5'
    ]:
        with pytest.raises(ValueError):
            c(v)

    assert c.short_description() == \
           "mapping to the following constraints: {}".format(
               [const.short_description() for const in [EnsureInt()]*5]
           )
    assert c.__repr__() == 'EnsureNTuple (itemconstraints={})'.format([const for const in [EnsureInt()]*5])


def test_EnsureMapping(tmp_path):
    true_key = 5
    true_value = False

    constraint = EnsureMapping(EnsureInt(), EnsureBool(), delimiter='::')
    # called without a mapping type
    with pytest.raises(ValueError):
        constraint(true_key)

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
    # for now just looking for smoke
    ds = Dataset(tmp_path)
    cds = constraint.for_dataset(ds)
    assert cds._key_constraint == constraint._key_constraint.for_dataset(ds)
    assert cds._value_constraint == \
        constraint._value_constraint.for_dataset(ds)


def test_EnsureMappings(tmp_path):
    # test scenarios that should work
    true_keys = ['one', 'two', 'three']
    true_vals = [1, 2, 3]
    constraint = EnsureMappings(key=EnsureStr(), value=EnsureInt())
    assert 'mapping of str -> int' in constraint.short_description()

    for v in ('one:1,two:2,three:3',  # string input
              {'one': 1, 'two': 2, 'three': 3},   # dict input
              dict(one=1, two=2, three=3),
              ['one', 1, 'two', 2, 'three', 3]  # sequence input
    ):
        d = constraint(v)
        assert isinstance(d, dict)
        assert len(d) == 3
        assert list(d.keys()) == true_keys
        assert list(d.values()) == true_vals

    # test scenarios that should crash
    for v in (true_keys,  # non-module 2 sequence
              '5',
              [],  # too short sequence
              tuple(),
              {},
              [5, False, False],
              set('wtf')  # wrong data type
              ):
        with pytest.raises(ValueError):
            d = constraint(v)

    # test different delimiters
    constraint = EnsureMappings(key=EnsureStr(), value=EnsureInt(),
                                delimiter='=', pair_delimiter='.')
    d = constraint('one=1.two=2.three=3')
    assert isinstance(d, dict)
    assert len(d) == 3
    assert list(d.keys()) == true_keys
    assert list(d.values()) == true_vals
    # test that the paths are resolved for the dataset
    ds = Dataset(tmp_path)
    pathconstraint = \
        EnsureMappings(key=EnsurePath(), value=EnsureInt()).for_dataset(
            DatasetParameter(tmp_path, ds))
    assert pathconstraint('some:5,somemore:6') == \
           {(Path.cwd() / 'some'): 5, Path.cwd() / 'somemore': 6}
    pathconstraint = \
        EnsureMappings(key=EnsurePath(), value=EnsurePath()).for_dataset(
            DatasetParameter(ds, ds))
    assert pathconstraint('some:other,something:more') == \
           {(ds.pathobj / 'some'): (ds.pathobj / 'other'),
            (ds.pathobj / 'something'): (ds.pathobj / 'more')}


def test_EnsureGeneratorFromFileLike():
    item_constraint = EnsureMapping(EnsureInt(), EnsureBool(), delimiter='::')
    constraint = EnsureGeneratorFromFileLike(item_constraint)

    assert 'items of type "mapping of int -> bool" read from a file-like' \
        == constraint.short_description()

    c = constraint(StringIO("5::yes\n1234::no\n"))
    assert isgenerator(c)
    assert list(c) == [{5: True}, {1234: False}]

    # missing final newline is not a problem
    c = constraint(StringIO("5::yes\n1234::no"))
    assert list(c) == [{5: True}, {1234: False}]

    # item constraint violation
    invalid_input = StringIO("1234::BANG\n5::yes")
    # immediate raise is default
    with pytest.raises(ValueError) as e:
        list(constraint(invalid_input))
    assert 'be convertible to boolean' in str(e)
    # but optionally it yields the exception to be able to
    # continue and enable a caller to raise/report/ignore
    # (must redefine `invalid_input` to read from start)
    invalid_input = StringIO("1234::BANG\n5::yes")
    res = list(
        EnsureGeneratorFromFileLike(
            item_constraint,
            exc_mode='yield',
        )(invalid_input)
    )
    # we get the result after the exception occurred
    assert isinstance(res[0], ValueError)
    assert res[1] == {5: True}

    # read from STDIN
    with patch("sys.stdin", StringIO("5::yes\n1234::no")):
        assert list(constraint('-')) == [{5: True}, {1234: False}]

    with patch("sys.stdin", StringIO("5::yes\n1234::no")):
        # will unpack a length-1 sequence for convenience
        assert list(constraint(['-'])) == [{5: True}, {1234: False}]

    # read from file
    if not on_windows:
        # on windows the write-rewind-test logic is not possible
        # (PermissionError) -- too lazy to implement a workaround
        with NamedTemporaryFile('w+') as f:
            f.write("5::yes\n1234::no")
            f.seek(0)
            assert list(constraint(f.name)) == [{5: True}, {1234: False}]

    # invalid file
    with pytest.raises(ValueError) as e:
        list(constraint('pytestNOTHEREdatalad'))

def test_ConstraintWithPassthrough(tmp_path):
    wrapped = EnsureInt()
    cwp = ConstraintWithPassthrough(wrapped, passthrough='mike')
    # main purpose
    assert cwp('mike') == 'mike'
    assert cwp('5') == 5
    # most info is coming straight from `wrapped`, the pass-through is
    # meant to be transparent
    assert str(cwp) == str(wrapped)
    assert cwp.short_description() == wrapped.short_description()
    assert cwp.long_description() == wrapped.long_description()
    # but repr reveals it
    assert repr(cwp).startswith('ConstraintWithPassthrough(')
    # tailoring for a dataset keeps the pass-through
    ds = Dataset(tmp_path)
    cwp_ds = cwp.for_dataset(ds)
    assert cwp_ds.passthrough == cwp.passthrough
    assert cwp.constraint == wrapped.for_dataset(ds)
