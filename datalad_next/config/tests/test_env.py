from ..env import LegacyEnvironment
from ..item import ConfigurationItem


def test_environment():
    env = LegacyEnvironment()
    assert str(env) == 'LegacyEnvironment'
    assert repr(env) == 'LegacyEnvironment()'


def test_load_datalad_env(monkeypatch):
    target_key = 'datalad.chunky-monkey.feedback'
    target_value = 'ohmnomnom'
    absurd_must_be_absent_key = 'nobody.would.use.such.a.key'
    with monkeypatch.context() as m:
        m.setenv('DATALAD_CHUNKY__MONKEY_FEEDBACK', 'ohmnomnom')
        env = LegacyEnvironment()
        assert target_key in env.keys()  # noqa: SIM118
        assert target_key in env
        assert env.get(target_key).value == target_value
        # default is wrapped into ConfigurationItem if needed
        assert env.get(
            absurd_must_be_absent_key,
            target_value
        ).value is target_value
        assert env.get(
            absurd_must_be_absent_key,
            ConfigurationItem(value=target_value)
        ).value is target_value
        assert env[target_key].value == target_value
        assert env.get(absurd_must_be_absent_key).value is None
        assert len(env)


def test_load_legacy_overrides(monkeypatch, caplog):
    with monkeypatch.context() as m:
        m.setenv(
            'DATALAD_CONFIG_OVERRIDES_JSON',
            '{"datalad.key1":"override", "datalad.key2":"override"}',
        )
        m.setenv('DATALAD_KEY1', 'evenmoreoverride')
        env = LegacyEnvironment()
        assert env['datalad.key1'].value == 'evenmoreoverride'
        assert env.get('datalad.key2').value == 'override'

    assert 'Failed to load' not in caplog.text
    with monkeypatch.context() as m:
        m.setenv(
            'DATALAD_CONFIG_OVERRIDES_JSON',
            '{"datalad.key1":NOJSON, "datalad.key2":"override"}',
        )
        env = LegacyEnvironment()
        assert 'datalad.key1' not in env
        assert 'datalad.key2' not in env
        assert 'Failed to load' in caplog.text
