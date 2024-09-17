import pytest

from datalad_next.runners import (
    call_git_lines,
    call_git_oneline,
)
from ..gitenv import GitEnvironment


def test_gitenv_singlevalue():
    env = GitEnvironment()
    target_key = 'absurd.key'
    target_value = 'absurd_value'
    env[target_key] = env.item_type(target_value)
    assert target_key in env
    assert target_key in env.keys()
    assert env[target_key].value == target_value
    assert env.get(target_key).value == target_value
    assert env.getall(target_key) == (env.item_type(target_value),)
    assert call_git_oneline(['config', target_key]) == target_value
    del env[target_key]
    assert target_key not in env
    with pytest.raises(KeyError):
        env[target_key]


def test_gitenv_multivalue():
    env = GitEnvironment()
    target_key = 'absurd.key'
    target_values = ('absurd_value1', 'absurd_value2', 'absurd_value3')
    assert target_key not in env
    for tv in target_values:
        env.add(target_key, env.item_type(tv))
    assert target_key in env
    assert env[target_key].value == target_values[-1]
    assert env.getall(target_key) == tuple(
        env.item_type(tv) for tv in target_values)
    # git sees all values
    assert call_git_lines(
        ['config', '--get-all', target_key]) == list(target_values)
    assert env.getall('notakey', 'mike') == (env.item_type('mike'),)
    del env[target_key]
