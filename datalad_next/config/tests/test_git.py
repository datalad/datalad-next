from ..git import (
    GlobalGitConfig,
    SystemGitConfig,
)
from ..item import ConfigurationItem


def test_global_git_config(datalad_cfg):
    target_key = 'my.config.key'
    target_value = 'my/config.value'

    gc = GlobalGitConfig()
    gc[target_key] = ConfigurationItem(value=target_value)
    # immediate availability
    assert target_key in gc
    assert gc[target_key] == ConfigurationItem(value=target_value)

    # if we create another instance, it also has the key, because
    # we wrote to a file, not just the instance
    gc2 = GlobalGitConfig()
    assert target_key in gc2
    assert gc2[target_key].value == target_value

    assert 'user.email' in gc
    assert gc['user.email']
