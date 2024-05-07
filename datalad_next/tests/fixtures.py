"""Collection of fixtures for facilitation test implementations
"""
import getpass
import logging
import os
from pathlib import Path
import subprocess
import pytest
from tempfile import NamedTemporaryFile
from time import sleep
from urllib.request import urlopen

from datalad_next.datasets import Dataset
from datalad_next.runners import (
    call_git_lines,
    call_git_success,
)
from datalad_next.utils import patched_env
from .utils import (
    HTTPPath,
    WebDAVPath,
    assert_ssh_access,
    external_versions,
    get_git_config_global_fpath,
    md5sum,
    rmtree,
)

lgr = logging.getLogger('datalad.next.tests.fixtures')


@pytest.fixture(autouse=True, scope="session")
def reduce_logging():
    """Reduce the logging output during test runs

    DataLad emits a large amount of repetitive INFO log messages that only
    clutter the test output, and hardly ever help to identify an issue.
    This fixture modifies the standard logger to throw away all INFO level
    log messages.

    With this approach, such messages are still fed to and processes by the
    logger (in contrast to an apriori level setting).
    """
    dllgr = logging.getLogger('datalad')
    # leave a trace that this is happening
    dllgr.info("Test fixture starts suppressing INFO level messages")

    class NoInfo(logging.Filter):
        def filter(self, record):
            # it seems unnecessary to special case progress logs, moreover
            # not filtering them out will make clone/fetch/push very visible
            # in the logs with trivial messages
            #if hasattr(record, 'dlm_progress'):
            #    # this is a progress log message that may trigger something
            #    # a test is looking for
            #    return True
            if record.levelno == 20:
                # this is a plain INFO message, ignore
                return False
            else:
                return True

    noinfo = NoInfo()
    # we need to attach the filter to any handler to make it effective.
    # adding to the logger only will not effect any log messages produced
    # via descendant loggers
    for hdlr in dllgr.handlers:
        hdlr.addFilter(noinfo)


@pytest.fixture(autouse=False, scope="function")
def no_result_rendering(monkeypatch):
    """Disable datalad command result rendering for all command calls

    This is achieved by forcefully supplying `result_renderer='disabled'`
    to any command call via a patch to internal argument normalizer
    ``get_allargs_as_kwargs()``.
    """
    # we need to patch our patch function, because datalad-core's is no
    # longer used
    import datalad_next.patches.interface_utils as dnpiu

    old_get_allargs_as_kwargs = dnpiu.get_allargs_as_kwargs

    def no_render_get_allargs_as_kwargs(call, args, kwargs):
        kwargs, one, two = old_get_allargs_as_kwargs(call, args, kwargs)
        kwargs['result_renderer'] = 'disabled'
        return kwargs, one, two

    with monkeypatch.context() as m:
        m.setattr(dnpiu,
                  'get_allargs_as_kwargs',
                  no_render_get_allargs_as_kwargs)
        yield


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
        with patched_env(DATALAD_TESTS_TMP_KEYRING_PATH=tf.name):
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
[datalad "extensions"]
    # load the next extension to be able to test patches of annex remotes
    # that run in subprocesses
    load = next
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
        pytest.skip(
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
        with patched_env(GIT_CONFIG_GLOBAL=tf.name):
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


@pytest.fixture(scope="session")
def modified_dataset(tmp_path_factory):
    """Produces a dataset with various modifications

    The fixture is module-scope, aiming to be reused by many tests focused
    on reporting. It does not support any further modification. The fixture
    will fail, if any such modification is detected.

    ``git status`` will report::

        ❯ git status -uall
        On branch dl-test-branch
        Changes to be committed:
          (use "git restore --staged <file>..." to unstage)
                new file:   dir_m/file_a
                new file:   file_a
                new file:   file_am

        Changes not staged for commit:
          (use "git add/rm <file>..." to update what will be committed)
          (use "git restore <file>..." to discard changes in working directory)
          (commit or discard the untracked or modified content in submodules)
                deleted:    dir_d/file_d
                deleted:    dir_m/file_d
                modified:   dir_m/file_m
                deleted:    dir_sm/sm_d
                modified:   dir_sm/sm_m (modified content)
                modified:   dir_sm/sm_mu (modified content, untracked content)
                modified:   dir_sm/sm_n (new commits)
                modified:   dir_sm/sm_nm (new commits, modified content)
                modified:   dir_sm/sm_nmu (new commits, modified content, untracked content)
                modified:   dir_sm/sm_u (untracked content)
                modified:   file_am
                deleted:    file_d
                modified:   file_m

        Untracked files:
          (use "git add <file>..." to include in what will be committed)
                dir_m/dir_u/file_u
                dir_m/file_u
                dir_u/file_u
                file_u


    Suffix indicates the ought-to state (multiple possible):

    a - added
    c - clean
    d - deleted
    n - new commits
    m - modified
    u - untracked content

    Prefix indicated the item type:

    file - file
    sm - submodule
    dir - directory
    """
    ds = Dataset(tmp_path_factory.mktemp("modified_dataset"))
    ds.create(result_renderer='disabled')
    ds_dir = ds.pathobj / 'dir_m'
    ds_dir.mkdir()
    ds_dir_d = ds.pathobj / 'dir_d'
    ds_dir_d.mkdir()
    (ds_dir / 'file_m').touch()
    (ds.pathobj / 'file_m').touch()
    dirsm = ds.pathobj / 'dir_sm'
    dss = {}
    for smname in (
        'sm_d', 'sm_c', 'sm_n', 'sm_m', 'sm_nm', 'sm_u', 'sm_mu', 'sm_nmu',
        'droppedsm_c',
    ):
        sds = Dataset(dirsm / smname).create(result_renderer='disabled')
        # for the plain modification, commit the reference right here
        if smname in ('sm_m', 'sm_nm', 'sm_mu', 'sm_nmu'):
            (sds.pathobj / 'file_m').touch()
        sds.save(to_git=True, result_renderer='disabled')
        dss[smname] = sds
    # files in superdataset to be deleted
    for d in (ds_dir_d, ds_dir, ds.pathobj):
        (d / 'file_d').touch()
    dss['.'] = ds
    dss['dir'] = ds_dir
    ds.save(to_git=True, result_renderer='disabled')
    ds.drop(dirsm / 'droppedsm_c', what='datasets', reckless='availability',
            result_renderer='disabled')
    # a new commit
    for smname in ('.', 'sm_n', 'sm_nm', 'sm_nmu'):
        sds = dss[smname]
        (sds.pathobj / 'file_c').touch()
        sds.save(to_git=True, result_renderer='disabled')
    # modified file
    for smname in ('.', 'dir', 'sm_m', 'sm_nm', 'sm_mu', 'sm_nmu'):
        obj = dss[smname]
        pobj = obj.pathobj if isinstance(obj, Dataset) else obj
        (pobj / 'file_m').write_text('modify!')
    # untracked
    for smname in ('.', 'dir', 'sm_u', 'sm_mu', 'sm_nmu'):
        obj = dss[smname]
        pobj = obj.pathobj if isinstance(obj, Dataset) else obj
        (pobj / 'file_u').touch()
        (pobj / 'dirempty_u').mkdir()
        (pobj / 'dir_u').mkdir()
        (pobj / 'dir_u' / 'file_u').touch()
    # delete items
    rmtree(dss['sm_d'].pathobj)
    rmtree(ds_dir_d)
    (ds_dir / 'file_d').unlink()
    (ds.pathobj / 'file_d').unlink()
    # added items
    for smname in ('.', 'dir', 'sm_m', 'sm_nm', 'sm_mu', 'sm_nmu'):
        obj = dss[smname]
        pobj = obj.pathobj if isinstance(obj, Dataset) else obj
        (pobj / 'file_a').write_text('added')
        assert call_git_success(['add', 'file_a'], cwd=pobj)
    # added and then modified file
    file_am_obj = ds.pathobj / 'file_am'
    file_am_obj.write_text('added')
    assert call_git_success(['add', 'file_am'], cwd=ds.pathobj)
    file_am_obj.write_text('modified')

    # record git-status output as a reference
    status_start = call_git_lines(['status'], cwd=ds.pathobj)
    yield ds
    # compare with initial git-status output, if there are any
    # differences the assumptions of any consuming test could be
    # invalidated. The modifying code must be found and fixed
    assert status_start == call_git_lines(['status'], cwd=ds.pathobj), \
        "Unexpected modification of the testbed"


@pytest.fixture(autouse=False, scope="session")
def webdav_credential():
    """Provides HTTP Basic authentication credential necessary to access the
    server provided by the ``webdav_server`` fixture."""
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
    # WebDAVPath.
    server = WebDAVPath(path, auth=auth)
    with server as server_url:
        server.url = server_url
        yield server


@pytest.fixture(autouse=False, scope="session")
def http_credential():
    """Provides the HTTP Basic authentication credential necessary to access the
    HTTP server provided by the ``http_server_with_basicauth`` fixture."""
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
    """Like ``http_server`` but requiring authentication via ``http_credential``
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


@pytest.fixture(scope="session")
def httpbin_service():
    """Return canonical access URLs for the HTTPBIN service

    This fixture tries to spin up a httpbin Docker container at localhost:8765;
    if successful, it returns this URL as the 'standard' URL.  If the attempt
    fails, a URL pointing to the canonical instance is returned.

    For tests that need to have the service served via a specific
    protocol (https vs http), the corresponding URLs are returned
    too. They always point to the canonical deployment, as some
    tests require both protocols simultaneously and a local deployment
    generally won't have https.
    """
    hburl = 'http://httpbin.org'
    hbsurl = 'https://httpbin.org'
    ciurl = 'http://localhost:8765'
    if os.name == "posix":
        try:
            r = subprocess.run(
                ["docker", "run", "-d", "-p", "127.0.0.1:8765:80", "kennethreitz/httpbin"],
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError):
            lgr.warning("Failed to spin up httpbin Docker container:", exc_info=True)
            container_id = None
        else:
            container_id = r.stdout.strip()
    else:
        container_id = None
    try:
        if container_id is not None:
            # Wait for container to fully start:
            for _ in range(25):
                try:
                    urlopen(ciurl)
                except Exception:
                    sleep(1)
                else:
                    break
            else:
                raise RuntimeError("httpbin container did not start up in time")
        yield {
            "standard": ciurl if container_id is not None else hbsurl,
            "http": hburl,
            "https": hbsurl,
        }
    finally:
        if container_id is not None:
            subprocess.run(["docker", "rm", "-f", container_id], check=True)


@pytest.fixture(scope="function")
def httpbin(httpbin_service):
    """Does the same thing as ``httpbin_service``, but skips on function-scope

    ``httpbin_service`` always returns access URLs for HTTPBIN. However,
    in some cases it is simply not desirable to run a test. For example,
    the appveyor workers are more or less constantly unable to access the
    public service. This fixture is evaluated at function-scope and
    skips the test whenever any of these undesired conditions is
    detected. Otherwise it just relays ``httpbin_service``.
    """
    if os.environ.get('DATALAD_TESTS_NONETWORK'):
        pytest.skip(
            'Not running httpbin-based test: NONETWORK flag set'
        )
    if 'APPVEYOR' in os.environ and 'DEPLOY_HTTPBIN_IMAGE' not in os.environ:
        pytest.skip(
            "Not running httpbin-based test on appveyor without "
            "docker-deployed instance -- too unreliable"
        )
    yield httpbin_service


@pytest.fixture(autouse=False, scope="function")
def datalad_interactive_ui(monkeypatch):
    """Yields a UI replacement to query for operations and stage responses

    No output will be written to STDOUT/ERR by this UI.

    A standard usage pattern is to stage one or more responses, run the
    to-be-tested code, and verify that the desired user interaction
    took place::

       > datalad_interactive_ui.staged_responses.append('skip')
       > ...
       > assert ... datalad_interactive_ui.log
    """
    from datalad_next.uis import ui_switcher
    from .utils import InteractiveTestUI

    with monkeypatch.context() as m:
        m.setattr(ui_switcher, '_ui', InteractiveTestUI())
        yield ui_switcher.ui


@pytest.fixture(autouse=False, scope="function")
def datalad_noninteractive_ui(monkeypatch):
    """Yields a UI replacement to query for operations

    No output will be written to STDOUT/ERR by this UI.

    A standard usage pattern is to run the to-be-tested code, and verify that
    the desired user messaging took place::

       > ...
       > assert ... datalad_interactive_ui.log
    """
    from datalad_next.uis import ui_switcher
    from .utils import TestUI

    with monkeypatch.context() as m:
        m.setattr(ui_switcher, '_ui', TestUI())
        yield ui_switcher.ui


@pytest.fixture(autouse=False, scope="session")
def sshserver_setup(tmp_path_factory):
    if not os.environ.get('DATALAD_TESTS_SSH'):
        pytest.skip(
            "set DATALAD_TESTS_SSH=1 to enable")

    # query a bunch of recognized configuration environment variables,
    # fill in the blanks, then check if the given configuration is working,
    # and post the full configuration again as ENV vars, to be picked up by
    # the function-scope `datalad_cfg`
    tmp_root = str(tmp_path_factory.mktemp("sshroot"))
    host = os.environ.get('DATALAD_TESTS_SERVER_SSH_HOST', 'localhost')
    port = os.environ.get('DATALAD_TESTS_SERVER_SSH_PORT', '22')
    login = os.environ.get(
        'DATALAD_TESTS_SERVER_SSH_LOGIN',
        getpass.getuser())
    seckey = os.environ.get(
        'DATALAD_TESTS_SERVER_SSH_SECKEY',
        str(Path.home() / '.ssh' / 'id_rsa'))
    path = os.environ.get('DATALAD_TESTS_SERVER_SSH_PATH', tmp_root)
    # TODO this should not use `tmp_root` unconditionally, but only if
    # the SSH_PATH is known to be the same. This might not be if SSH_PATH
    # is explicitly configured and LOCALPATH is not -- which could be
    # an indication that there is none
    localpath = os.environ.get('DATALAD_TESTS_SERVER_LOCALPATH', tmp_root)

    assert_ssh_access(host, port, login, seckey, path, localpath)

    info = {}
    # as far as we can tell, this is good, post effective config in ENV
    for v, e in (
            (host, 'HOST'),
            # this is SSH_*, because elsewhere we also have other properties
            # for other services
            (port, 'SSH_PORT'),
            (login, 'SSH_LOGIN'),
            (seckey, 'SSH_SECKEY'),
            (path, 'SSH_PATH'),
            (localpath, 'LOCALPATH'),
    ):
        os.environ[f"DATALAD_TESTS_SERVER_{e}"] = v
        info[e] = v

    yield info


@pytest.fixture(autouse=False, scope="function")
def sshserver(sshserver_setup, datalad_cfg, monkeypatch):
    # strip any leading / from the path, we add one, and
    # only one below
    sshserver_path = sshserver_setup['SSH_PATH'].lstrip('/')
    baseurl = f"ssh://{sshserver_setup['SSH_LOGIN']}" \
        f"@{sshserver_setup['HOST']}" \
        f":{sshserver_setup['SSH_PORT']}" \
        f"/{sshserver_path}"
    with monkeypatch.context() as m:
        m.setenv("DATALAD_SSH_IDENTITYFILE", sshserver_setup['SSH_SECKEY'])
        # force reload the config manager, to ensure the private key setting
        # makes it into the active config
        datalad_cfg.reload(force=True)
        yield baseurl, Path(sshserver_setup['LOCALPATH'])
