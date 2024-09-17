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
