import pytest

from ..  import ParamDictator


def test_paramdictator():
    d = {'a': 1, 'b': 2}
    pd = ParamDictator(d)
    assert pd.a == 1
    assert pd.b == 2
    with pytest.raises(AssertionError):
        assert pd.__dir__ is None
