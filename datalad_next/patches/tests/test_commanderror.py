import pytest

from datalad_next.exceptions import CommandError


def test_repr_str():
    # standard case of a command that failed with non-zero exit
    # many git/git-annex plumbing commands purposefully signal
    # statuses like this
    e = CommandError('some command', code=1)
    assert 'some command' in str(e)
    assert 'some command' in repr(e)

def test_returncode_code_alias():
    # check that `returncode` is an alias for `code`
    e = CommandError('some command', code=1)
    assert e.returncode == 1
    e.returncode = 2
    assert e.returncode == 2
    assert e.code == 2
    with pytest.raises(AttributeError):
        assert e.xyz == 3
    with pytest.raises(AttributeError):
        e._aliases = 1
