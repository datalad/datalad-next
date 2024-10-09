import pytest

from datalad_next.config import manager


def test_manager_setup():
    """Test the actual global configuration manager"""
    target_sources = [
        'git-command', 'legacy-environment', 'git-global', 'git-system',
        'defaults',
    ]
    target_key = 'user.name'
    absurd_must_be_absent_key = 'nobody.would.use.such.a.key'
    # the order of sources is the precedence rule
    assert list(manager.sources.keys()) == target_sources
    # any real manager will have some keys
    assert len(manager)
    assert target_key in manager
    assert absurd_must_be_absent_key not in manager
    # test query
    item = manager[target_key]
    with pytest.raises(KeyError):
        manager[absurd_must_be_absent_key]
    # we cannot be really specific and also robust
    assert item.value
    assert manager[target_key]
    assert manager.get(absurd_must_be_absent_key).value is None
