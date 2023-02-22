import pytest

from datalad.conftest import setup_package


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
