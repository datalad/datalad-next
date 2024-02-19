import pytest

from ..git import (
    CommandError,
    call_git,
    call_git_lines,
    call_git_oneline,
    call_git_success,
    iter_git_subproc,
)


def test_call_git():
    # smoke test
    call_git(['--version'])
    # raises properly
    with pytest.raises(CommandError):
        call_git(['notacommand'])


def test_call_git_success():
    assert call_git_success(['--version'])
    assert not call_git_success(['notacommand'])


def test_call_git_lines():
    lines = call_git_lines(['--version'])
    assert len(lines) == 1
    assert lines[0].startswith('git version')
    # check that we can force Git into LC_ALL mode.
    # this test is only meaningful on systems that
    # run with some other locale
    call_git_lines(['-h'])[0].casefold().startswith('usage')


def test_call_git_oneline():
    line = call_git_oneline(['--version'])
    assert line.startswith('git version')
    with pytest.raises(AssertionError):
        # TODO may not yield multiple lines on all systems
        call_git_oneline(['config', '-l'])


def test_iter_git_subproc():
    # just a smoke test that 'git' gets prepended
    with iter_git_subproc(['--version']) as g:
        assert list(g)
