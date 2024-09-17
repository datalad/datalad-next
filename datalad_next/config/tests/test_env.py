from ..env import Environment


def test_load_datalad_env(monkeypatch):
    target_key = 'datalad.chunky-monkey.feedback'
    target_value = 'ohmnomnom'
    with monkeypatch.context() as m:
        m.setenv('DATALAD_CHUNKY__MONKEY_FEEDBACK', 'ohmnomnom')
        env = Environment()
        assert target_key in env.keys()  # noqa: SIM118
        assert target_key in env
        assert env.getvalue(target_key) == target_value


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

