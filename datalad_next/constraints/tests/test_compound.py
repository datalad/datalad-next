from inspect import isgenerator
from io import StringIO
import pytest
from tempfile import NamedTemporaryFile
from unittest.mock import patch
from pathlib import Path

from datalad_next.exceptions import CapturedException
from datalad_next.utils import on_windows

from ..base import DatasetParameter

from ..basic import (
    EnsureInt,
    EnsureBool,
    EnsurePath,
)
from ..compound import (
    ConstraintWithPassthrough,
    EnsureIterableOf,
    EnsureListOf,
    EnsureTupleOf,
    EnsureMapping,
    EnsureGeneratorFromFileLike,
    WithDescription,
)
from ..exceptions import ConstraintError


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
    assert repr(c) == \
        "EnsureListOf(item_constraint=<class 'str'>, min_len=None, max_len=None)"


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
    with pytest.raises(ValueError):
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


def test_EnsureMapping(dataset):
    true_key = 5
    true_value = False

    constraint = EnsureMapping(EnsureInt(), EnsureBool(), delimiter='::')
    # called without a mapping type
    with pytest.raises(ValueError):
        constraint(true_key)

    assert 'mapping of int -> bool' in constraint.short_description()
    assert repr(constraint) == \
        "EnsureMapping(key=EnsureInt(), value=EnsureBool(), delimiter='::')"

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

    # test for_dataset()
    # smoketest
    ds = dataset
    cds = constraint.for_dataset(ds)
    assert cds._key_constraint == constraint._key_constraint.for_dataset(ds)
    assert cds._value_constraint == \
        constraint._value_constraint.for_dataset(ds)
    # test that the path is resolved for the dataset
    pathconstraint = \
        EnsureMapping(key=EnsurePath(), value=EnsureInt()).for_dataset(
            DatasetParameter(ds.pathobj, ds))
    assert pathconstraint('some:5') == {(Path.cwd() / 'some'): 5}
    pathconstraint = \
        EnsureMapping(key=EnsurePath(), value=EnsurePath()).for_dataset(
            DatasetParameter(ds, ds))
    assert pathconstraint('some:other') == \
           {(ds.pathobj / 'some'): (ds.pathobj / 'other')}


def test_EnsureGeneratorFromFileLike():
    item_constraint = EnsureMapping(EnsureInt(), EnsureBool(), delimiter='::')
    constraint = EnsureGeneratorFromFileLike(item_constraint)

    assert 'items of type "mapping of int -> bool" read from a file-like' \
        == constraint.short_description()
    assert repr(constraint) == \
        "EnsureGeneratorFromFileLike(" \
        "item_constraint=EnsureMapping(key=EnsureInt(), " \
        "value=EnsureBool(), delimiter='::'))"

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
    assert isinstance(res[0], CapturedException)
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


def test_ConstraintWithPassthrough(dataset):
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
    ds = dataset
    cwp_ds = cwp.for_dataset(ds)
    assert cwp_ds.passthrough == cwp.passthrough
    assert cwp.constraint == wrapped.for_dataset(ds)


def test_WithDescription(dataset):
    wrapped = EnsureInt()
    # confirm starting point
    assert wrapped.input_synopsis == 'int'
    assert wrapped.input_description \
        == "value must be convertible to type 'int'"
    # we are actually not replacing anything
    c = WithDescription(wrapped)
    assert c.input_synopsis == wrapped.input_synopsis
    assert c.input_description == wrapped.input_description
    # with no dataset docs, the wrapping is removed on tailoring
    ds = dataset
    assert isinstance(
        c.for_dataset(DatasetParameter(None, ds)),
        EnsureInt)
    # check all replacements are working
    c = WithDescription(
        wrapped,
        input_synopsis='mysynopsis',
        input_description='mydescription',
        input_synopsis_for_ds='dssynopsis',
        input_description_for_ds='dsdescription',
        error_message='myerror',
        error_message_for_ds='dserror',
    )
    # function is maintained
    assert c('5') is 5
    assert str(c) == '<EnsureInt with custom description>'
    assert repr(c) == \
        "WithDescription(EnsureInt(), " \
        "input_synopsis='mysynopsis', " \
        "input_description='mydescription', " \
        "input_synopsis_for_ds='dssynopsis', " \
        "input_description_for_ds='dsdescription', " \
        "error_message='myerror', " \
        "error_message_for_ds='dserror')"
    assert c.constraint is wrapped
    assert c.input_synopsis == 'mysynopsis'
    assert c.input_description == 'mydescription'
    # description propagates through tailoring
    cds = c.for_dataset(DatasetParameter(None, ds))
    assert isinstance(cds, WithDescription)
    assert cds.input_synopsis == 'dssynopsis'
    assert cds.input_description == 'dsdescription'

    # when the wrapped constraint raises, the wrapper
    # interjects and reports a different error
    with pytest.raises(ConstraintError) as e:
        c(None)
    assert e.value.msg == 'myerror'

    # legacy functionality
    c.short_description() == c.input_synopsis
    c.long_description() == c.input_description
