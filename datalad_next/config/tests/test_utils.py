
import pytest

from .. import utils  # for patching environ

from ..utils import get_gitconfig_items_from_env


def test_get_gitconfig_items_from_env(monkeypatch):
    with monkeypatch.context() as m:
        # without the COUNT the rest does not matter and we always
        # get an empty dict
        m.delenv('GIT_CONFIG_COUNT', raising=False)
        assert get_gitconfig_items_from_env() == {}

    with monkeypatch.context() as m:
        # setting zero items, also makes everything else irrelevant
        m.setenv('GIT_CONFIG_COUNT', '0')
        assert get_gitconfig_items_from_env() == {}

    with monkeypatch.context() as m:
        # predictable error for botched count
        m.setenv('GIT_CONFIG_COUNT', 'rubbish')
        with pytest.raises(ValueError) as e:
            get_gitconfig_items_from_env()
        assert 'bogus count in GIT_CONFIG_COUNT' in str(e)

    # bunch of std error conditions
    for env, excstr in (
            ({'GIT_CONFIG_COUNT': 1,
              'GIT_CONFIG_KEY_0': 'section.name'},
             'missing config value'),
            ({'GIT_CONFIG_COUNT': 1,
              'GIT_CONFIG_VALUE_0': 'value'},
             'missing config key'),
            ({'GIT_CONFIG_COUNT': 1,
              'GIT_CONFIG_KEY_0': '',
              'GIT_CONFIG_VALUE_0': 'value'},
             'empty config key'),
            ({'GIT_CONFIG_COUNT': 1,
              'GIT_CONFIG_KEY_0': 'nosection',
              'GIT_CONFIG_VALUE_0': 'value'},
             'does not contain a section'),
    ):
        with monkeypatch.context() as m:
            m.setattr(utils, 'environ', env)
            with pytest.raises(ValueError) as e:
                get_gitconfig_items_from_env()
            assert excstr in str(e)

    # proper functioning
    for env, target in (
            ({'GIT_CONFIG_COUNT': 1,
              'GIT_CONFIG_KEY_0': 'section.name',
              'GIT_CONFIG_VALUE_0': 'value'},
             {'section.name': 'value'}),
            ({'GIT_CONFIG_COUNT': 2,
              'GIT_CONFIG_KEY_0': 'section.name1',
              'GIT_CONFIG_VALUE_0': 'value1',
              'GIT_CONFIG_KEY_1': 'section.name2',
              'GIT_CONFIG_VALUE_1': 'value2'},
             {'section.name1': 'value1', 'section.name2': 'value2'}),
            # double-specification appends
            # ❯ GIT_CONFIG_COUNT=2 \
            #   GIT_CONFIG_KEY_0=section.name \
            #   GIT_CONFIG_VALUE_0=val1 \
            #   GIT_CONFIG_KEY_1=section.name \
            #   GIT_CONFIG_VALUE_1=val2 \
            #   git config --list --show-origin | grep 'command line:'
            # command line:   section.name=val1
            # command line:   section.name=val2
            ({'GIT_CONFIG_COUNT': 3,
              'GIT_CONFIG_KEY_0': 'section.name',
              'GIT_CONFIG_VALUE_0': 'value0',
              'GIT_CONFIG_KEY_1': 'section.name',
              'GIT_CONFIG_VALUE_1': 'value1',
              'GIT_CONFIG_KEY_2': 'section.name',
              'GIT_CONFIG_VALUE_2': 'value2'},
             {'section.name': ('value0', 'value1', 'value2')}),
    ):
        with monkeypatch.context() as m:
            m.setattr(utils, 'environ', env)
            assert get_gitconfig_items_from_env() == target
