from inspect import isgenerator
from io import StringIO
import pytest
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from datalad_next.utils import on_windows

from ..basic import (
    EnsureInt,
    EnsureBool,
)
from ..compound import (
    EnsureIterableOf,
    EnsureListOf,
    EnsureTupleOf,
    EnsureMapping,
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


def test_EnsureMapping():
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


def test_EnsureGeneratorFromFileLike():
    item_constraint = EnsureMapping(EnsureInt(), EnsureBool(), delimiter='::')
    constraint = EnsureGeneratorFromFileLike(item_constraint)

    assert 'items of type "mapping of int -> bool" read from a file-like' \
        ==  constraint.short_description()

    c = constraint(StringIO("5::yes\n1234::no\n"))
    assert isgenerator(c)
    assert list(c) == [{5: True}, {1234: False}]

    # missing final newline is not a problem
    c = constraint(StringIO("5::yes\n1234::no"))
    assert list(c) == [{5: True}, {1234: False}]

    # item constraint violation
    c = constraint(StringIO("5::yes\n1234::BANG"))
    with pytest.raises(ValueError) as e:
        list(c)
    assert 'be convertible to boolean' in str(e)

    # read from STDIN
    with patch("sys.stdin", StringIO("5::yes\n1234::no")):
        assert list(constraint('-')) == [{5: True}, {1234: False}]

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
