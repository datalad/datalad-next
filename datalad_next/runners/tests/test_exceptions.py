from ..exceptions import CommandError

def test_repr_str():
    # standard case of a command that failed with non-zero exit
    # many git/git-annex plumbing commands purposefully signal
    # statuses like this
    e = CommandError('some command', code=1)
    assert 'some command' in str(e)
    assert 'some command' in repr(e)
