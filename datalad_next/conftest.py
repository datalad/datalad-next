import logging
from os import environ
from pathlib import Path
import pytest

from datalad.conftest import setup_package

from datalad_next.tests.utils import md5sum

lgr = logging.getLogger('datalad.next')


@pytest.fixture(autouse=False, scope="function")
def memory_keyring():
    """Patch keyring to temporarily use a backend that only stores in memory

    No credential read or write actions will impact any existing credential
    store of any configured backend.

    The patched-in backend is yielded by the fixture. It offers a ``store``
    attribute, which is a ``dict`` that uses keys of the pattern::

        (datalad-<credential name>, <field name>)

    and the associated secrets as values. For non-legacy credentials the
    ``<field name>`` is uniformly ``'secret'``. For legacy credentials
    other values are also used, including fields that are not actually
    secrets.
    """
    import keyring
    import keyring.backend

    class MemoryKeyring(keyring.backend.KeyringBackend):
        # high priority
        priority = 1000

        def __init__(self):
            self.store = {}

        def set_password(self, servicename, username, password):
            self.store[(servicename, username)] = password

        def get_password(self, servicename, username):
            return self.store.get((servicename, username))

        def delete_password(self, servicename, username):
            del self.store[(servicename, username)]

    old_backend = keyring.get_keyring()
    new_backend = MemoryKeyring()
    keyring.set_keyring(new_backend)

    yield new_backend

    keyring.set_keyring(old_backend)


@pytest.fixture(autouse=True, scope="function")
def check_gitconfig_global():
    """No test must modify a user's global Git config.

    If such modifications are needed, a custom configuration setup
    limited to the scope of the test requiring it must be arranged.
    """
    globalcfg_fname = environ.get('GIT_CONFIG_GLOBAL')
    if globalcfg_fname is None:
        # this can happen with the datalad-core setup for Git < 2.32.
        # we provide a fallback, but we do not aim to support all
        # possible variants
        globalcfg_fname = Path(environ['HOME']) / '.gitconfig'

    globalcfg_fname = Path(globalcfg_fname)
    if not globalcfg_fname.exists():
        lgr.warning(
            'No global/user Git config file exists. This is an unexpected '
            'test environment, no config modifications checks can be '
            'performed. Proceeding nevertheless.')
        # let the test run
        yield
        # and exit quietly
        return

    # we have a config file. hash it pre and post test. Fail is changed.
    pre = md5sum(globalcfg_fname)
    yield
    post = md5sum(globalcfg_fname)
    assert pre == post, \
        "Global Git config modification detected. Test must be modified to use " \
        "a temporary configuration target."
