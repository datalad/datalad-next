import logging
import os
from pathlib import Path
import pytest
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from datalad.conftest import setup_package

from datalad_next.tests.utils import (
    SkipTest,
    external_versions,
    get_git_config_global_fpath,
    md5sum,
)

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


# the following is taken from datalad/conftest.py
# sadly, this is defined inline and cannot be reused directly
standard_gitconfig = """\
[user]
        name = DataLad Tester
        email = test@example.com
[core]
	askPass =
[datalad "log"]
        exc = 1
[annex "security"]
	# from annex 6.20180626 file:/// and http://localhost access isn't
	# allowed by default
	allowed-url-schemes = http https file
	allowed-http-addresses = all
[protocol "file"]
    # since git 2.38.1 cannot by default use local clones for submodules
    # https://github.blog/2022-10-18-git-security-vulnerabilities-announced/#cve-2022-39253
    allow = always
""" + os.environ.get('DATALAD_TESTS_GITCONFIG', '').replace('\\n', os.linesep)


@pytest.fixture(autouse=False, scope="function")
def datalad_cfg():
    """Temporarily alter configuration to use a plain "global" configuration

    The global configuration manager at `datalad.cfg` is reloaded after
    adjusting `GIT_CONFIG_GLOBAL` to point to a new temporary `.gitconfig`
    file.

    After test execution the file is removed, and the global `ConfigManager`
    is reloaded once more.

    Any test using this fixture will be skipped for Git versions earlier
    than 2.32, because the `GIT_CONFIG_GLOBAL` environment variable used
    here was only introduced with that version.
    """
    if external_versions['cmd:git'] < "2.32":
        raise SkipTest(
            "Git configuration redirect via GIT_CONFIG_GLOBAL "
            "only supported since Git v2.32"
        )
    from datalad import cfg
    with NamedTemporaryFile(
            'w',
            prefix='datalad_gitcfg_global_',
            delete=False) as tf:
        tf.write(standard_gitconfig)
        # we must close, because windows does not like the file being open
        # already when ConfigManager would open it for reading
        tf.close()
        with patch.dict(os.environ, {'GIT_CONFIG_GLOBAL': tf.name}):
            cfg.reload(force=True)
            yield cfg
    # reload to put the previous config in effect again
    cfg.reload(force=True)


@pytest.fixture(autouse=True, scope="function")
def check_gitconfig_global():
    """No test must modify a user's global Git config.

    If such modifications are needed, a custom configuration setup
    limited to the scope of the test requiring it must be arranged.
    """
    globalcfg_fname = get_git_config_global_fpath()
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
        "a temporary configuration target. Hint: use the `datalad_cfg` fixture."


@pytest.fixture(autouse=True, scope="function")
def check_plaintext_keyring():
    """No test must modify a user's keyring.

    If such modifications are needed, a custom keyring setup
    limited to the scope of the test requiring it must be arranged.
    The ``memory_keyring`` fixture can be employed in such cases.
    """
    # datalad-core configures keyring to use a plaintext backend
    # we will look for the underlying file and verify that it is either
    # no there, or remains unmodified
    import keyring
    kr = keyring.get_keyring()
    if not hasattr(kr, 'file_path'):
        # this is not the plain text keyring, nothing we can do here
        # run as-is, but leave a message
        lgr.warning('Running without the expected plain-text keyring')
        yield
        return

    kr_fpath = Path(kr.file_path)
    pre = md5sum(kr_fpath) if kr_fpath.exists() else ''
    yield
    post = md5sum(kr_fpath) if kr_fpath.exists() else ''
    assert pre == post, \
        "Keyring modification detected. Test must be modified to use " \
        "a temporary keyring. Hint: use the `memory_keyring` fixture."
