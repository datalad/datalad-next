from datalad_next.runners import call_git_oneline

from ..git import (
    GlobalGitConfig,
)
from ..item import ConfigurationItem


def test_global_git_config(datalad_cfg):
    target_key = 'my.config.key'
    target_value = 'my/config.value'

    gc = GlobalGitConfig()
    gc[target_key] = ConfigurationItem(value=target_value)
    # immediate availability
    assert target_key in gc
    assert gc[target_key].value == target_value

    # if we create another instance, it also has the key, because
    # we wrote to a file, not just the instance
    gc2 = GlobalGitConfig()
    assert target_key in gc2
    assert gc2[target_key].value == target_value

    assert 'user.email' in gc
    assert gc['user.email']


def test_global_git_config_pure(datalad_cfg, monkeypatch):
    orig_keys = GlobalGitConfig().keys()
    with monkeypatch.context() as m:
        m.setenv('GIT_CONFIG_COUNT', '1')
        m.setenv('GIT_CONFIG_KEY_0', 'datalad.absurdkey')
        m.setenv('GIT_CONFIG_VALUE_0', 'absurddummy')
        # check that the comamnd-scope configuration does not bleed
        # into the global scope (global here being an example for any
        # other scope)
        assert GlobalGitConfig().keys() == orig_keys
        # but Git does see the manipulation
        assert call_git_oneline(
            ['config', 'datalad.absurdkey']) == 'absurddummy'
