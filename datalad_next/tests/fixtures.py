import logging
import os
from pathlib import Path
import pytest
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from datalad_next.datasets import Dataset
from datalad_next.tests.utils import (
    HTTPPath,
    SkipTest,
    WebDAVPath,
    external_versions,
    get_git_config_global_fpath,
    md5sum,
)

lgr = logging.getLogger('datalad.next.tests.fixtures')


@pytest.fixture(autouse=False, scope="function")
def tmp_keyring():
    """Patch plaintext keyring to temporarily use a different storage

    No credential read or write actions will impact any existing credential
    store of any configured backend.

    The patched backend is yielded by the fixture.
    """
    import keyring

    # the testsetup assumes this to be a plaintext backend.
    # this backend is based on a filename and maintains no state.
    # each operation opens, reads/writes, and then closes the file.
    # hence we can simply point to a different file
    backend = keyring.get_keyring()
    prev_fpath = backend.file_path

    # no tmp keyring yet, make one
    with NamedTemporaryFile(
            'r',
            prefix='datalad_tmp_keyring_',
            delete=True) as tf:
        # we must close, because windows does not like the file being open
        # already when ConfigManager would open it for reading
        tf.close()
        backend.file_path = tf.name
        with patch.dict(os.environ,
                        {'DATALAD_TESTS_TMP_KEYRING_PATH': tf.name}):
            yield backend

    backend.file_path = prev_fpath


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
    The ``tmp_keyring`` fixture can be employed in such cases.
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
        "a temporary keyring. Hint: use the `tmp_keyring` fixture."


@pytest.fixture(autouse=False, scope="function")
def credman(datalad_cfg, tmp_keyring):
    """Provides a temporary credential manager

    It comes with a temporary global datalad config and a temporary
    keyring as well.

    This manager can be used to deploy or manipulate credentials within the
    scope of a single test.
    """
    from datalad import cfg
    from datalad_next.credman import CredentialManager
    cm = CredentialManager(cfg)
    yield cm


@pytest.fixture(autouse=False, scope="function")
def dataset(datalad_cfg, tmp_path_factory):
    """Provides a ``Dataset`` instance for a not-yet-existing repository

    The instance points to an existing temporary path, but ``create()``
    has not been called on it yet.
    """
    # must use the factory to get a unique path even when a concrete
    # test also uses `tmp_path`
    ds = Dataset(tmp_path_factory.mktemp("dataset"))
    yield ds


@pytest.fixture(autouse=False, scope="function")
def existing_dataset(dataset):
    """Provides a ``Dataset`` instance pointing to an existing dataset/repo

    This fixture uses an instance provided by the ``dataset`` fixture and
    calls ``create()`` on it, before it yields the ``Dataset`` instance.
    """
    dataset.create(result_renderer='disabled')
    yield dataset


@pytest.fixture(autouse=False, scope="function")
def existing_noannex_dataset(dataset):
    """just like ``existing_dataset``, but created with ``annex=False``
    """
    dataset.create(annex=False, result_renderer='disabled')
    yield dataset


@pytest.fixture(autouse=False, scope="session")
def webdav_credential():
    yield dict(
        name='dltest-my&=webdav',
        user='datalad',
        secret='secure',
        type='user_password',
    )


@pytest.fixture(autouse=False, scope="function")
def webdav_server(tmp_path_factory, webdav_credential):
    """Provides a WebDAV server, serving a temporary directory

    The fixtures yields an instance of ``WebDAVPath``, providing the
    following essential attributes:

    - ``path``: ``Path`` instance of the served temporary directory
    - ``url``: HTTP URL to access the WebDAV server

    Server access requires HTTP Basic authentication with the credential
    provided by the ``webdav_credential`` fixture.
    """
    auth = (webdav_credential['user'], webdav_credential['secret'])
    # must use the factory to get a unique path even when a concrete
    # test also uses `tmp_path`
    path = tmp_path_factory.mktemp("webdav")
    # this looks a little awkward, but is done to avoid a change in
    # WebDAVPath. It would be better to have WebDAVPath directly
    # set `.url` internally, but that would require adjusting
    # the old `serve_path_via_webdav`
    server = WebDAVPath(path, auth=auth)
    with server as server_url:
        server.url = server_url
        yield server


@pytest.fixture(autouse=False, scope="session")
def http_credential():
    yield dict(
        name='dltest-my&=http',
        user='datalad',
        secret='secure',
        type='user_password',
    )


@pytest.fixture(autouse=False, scope="function")
def http_server(tmp_path_factory):
    """Provides an HTTP server, serving a temporary directory

    The fixtures yields an instance of ``HTTPPath``, providing the
    following essential attributes:

    - ``path``: ``Path`` instance of the served temporary directory
    - ``url``: HTTP URL to access the HTTP server

    Server access requires HTTP Basic authentication with the credential
    provided by the ``webdav_credential`` fixture.
    """
    # must use the factory to get a unique path even when a concrete
    # test also uses `tmp_path`
    path = tmp_path_factory.mktemp("webdav")
    server = HTTPPath(path, use_ssl=False, auth=None)
    with server:
        # overwrite path with Path object for convenience
        server.path = path
        yield server


@pytest.fixture(autouse=False, scope="function")
def http_server_with_basicauth(tmp_path_factory, http_credential):
    """Like ``http_server`` but requiering authenticat with ``http_credential``
    """
    path = tmp_path_factory.mktemp("webdav")
    server = HTTPPath(
        path, use_ssl=False,
        auth=(http_credential['user'], http_credential['secret']),
    )
    with server:
        # overwrite path with Path object for convenience
        server.path = path
        yield server
