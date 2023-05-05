
import pytest

from .. import utils  # for patching environ

from ..utils import (
    get_gitconfig_items_from_env,
    set_gitconfig_items_in_env,
)


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
            # GIT_CONFIG_KEY_1=section.name \
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


def test_set_gitconfig_items_in_env(monkeypatch):
    for start, items, target in (
            # giving nothing preserves statusquo
            ({}, {}, {}),
            ({'DUMMY': 'value'}, {}, {'DUMMY': 'value'}),
            # fixable specification is cleaned up
            ({'GIT_CONFIG_COUNT': '526'}, {}, {}),
            # but it has limits
            ({'GIT_CONFIG_COUNT': 'nochance'}, {},
             {'GIT_CONFIG_COUNT': 'nochance'}),
            # and there is no exhaustive search
            ({'GIT_CONFIG_KEY_3': 'dummy'}, {}, {'GIT_CONFIG_KEY_3': 'dummy'}),
            # virgin territory
            ({}, {'section.name': 'value'},
             {'GIT_CONFIG_COUNT': '1',
              'GIT_CONFIG_KEY_0': 'section.name',
              'GIT_CONFIG_VALUE_0': 'value'}),
            # "set" means "replace, not amend
            ({'GIT_CONFIG_COUNT': '1',
              'GIT_CONFIG_KEY_0': 'section.name',
              'GIT_CONFIG_VALUE_0': 'value'},
             {'altsection.name2': 'value2'},
             {'GIT_CONFIG_COUNT': '1',
              'GIT_CONFIG_KEY_0': 'altsection.name2',
              'GIT_CONFIG_VALUE_0': 'value2'}),
            # full cleanupage
            ({'GIT_CONFIG_COUNT': '2',
              'GIT_CONFIG_KEY_0': 'section.name',
              'GIT_CONFIG_VALUE_0': 'value',
              'GIT_CONFIG_KEY_1': 'altsection.name2',
              'GIT_CONFIG_VALUE_1': 'value2'},
             {}, {}),
            # multi-value support, order preserved
            ({}, {'section.name': ('c', 'a', 'b')},
             {'GIT_CONFIG_COUNT': '3',
              'GIT_CONFIG_KEY_0': 'section.name',
              'GIT_CONFIG_VALUE_0': 'c',
              'GIT_CONFIG_KEY_1': 'section.name',
              'GIT_CONFIG_VALUE_1': 'a',
              'GIT_CONFIG_KEY_2': 'section.name',
              'GIT_CONFIG_VALUE_2': 'b'}),
    ):
        with monkeypatch.context() as m:
            env = dict(start)
            m.setattr(utils, 'environ', env)
            set_gitconfig_items_in_env(items)
            assert env == target


def test_get_set_gitconfig_env_roundtrip(monkeypatch):
    items = {'section.name': ('c', 'a', 'b'),
             'space section.na me.so me': 'v al'}
    with monkeypatch.context() as m:
        env = {}
        m.setattr(utils, 'environ', env)
        # feed in copy to ensure validity of the test
        set_gitconfig_items_in_env(dict(items))
        assert get_gitconfig_items_from_env() == items
