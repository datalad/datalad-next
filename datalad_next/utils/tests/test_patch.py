from ..patch import patched_env
from os import environ


def test_patched_env():
    if 'HOME' in environ:
        home = environ['HOME']
        with patched_env(HOME=None):
            assert 'HOME' not in environ
        assert environ['HOME'] == home
    unusual_name = 'DATALADPATCHENVTESTVAR'
    if unusual_name not in environ:
        with patched_env(**{unusual_name: 'dummy'}):
            assert environ[unusual_name] == 'dummy'
        assert unusual_name not in environ
