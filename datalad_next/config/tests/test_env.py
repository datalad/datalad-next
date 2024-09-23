from ..env import Environment
from ..item import ConfigurationItem


def test_environment():
    env = Environment()
    assert str(env) == 'Environment'
    assert repr(env) == 'Environment()'


def test_load_datalad_env(monkeypatch):
    target_key = 'datalad.chunky-monkey.feedback'
    target_value = 'ohmnomnom'
    absurd_must_be_absent_key = 'nobody.would.use.such.a.key'
    with monkeypatch.context() as m:
        m.setenv('DATALAD_CHUNKY__MONKEY_FEEDBACK', 'ohmnomnom')
        env = Environment()
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
        assert env.getvalue(target_key) == target_value
        assert env.getvalue(absurd_must_be_absent_key) is None
        assert len(env)


def test_load_legacy_overrides(monkeypatch):
    with monkeypatch.context() as m:
        m.setenv(
            'DATALAD_CONFIG_OVERRIDES_JSON',
            '{"datalad.key1":"override", "annex.key2":"override"}',
        )
        m.setenv('DATALAD_KEY1', 'evenmoreoverride')
        env = Environment()
        assert env.getvalue('datalad.key1') == 'evenmoreoverride'
        assert env.getvalue('annex.key2') == 'override'

    with monkeypatch.context() as m:
        m.setenv(
            'DATALAD_CONFIG_OVERRIDES_JSON',
            '{"datalad.key1":NOJSON, "annex.key2":"override"}',
        )
        env = Environment()
        assert 'datalad.key1' not in env
        assert 'annex.key2' not in env
