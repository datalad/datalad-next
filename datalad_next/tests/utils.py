import logging
from functools import wraps
from pathlib import Path

# all datalad-core test utils needed for datalad-next
from datalad.tests.utils_pytest import (
    DEFAULT_BRANCH,
    DEFAULT_REMOTE,
    SkipTest,
    assert_equal,
    assert_false,
    assert_in,
    assert_in_results,
    assert_not_in,
    assert_raises,
    assert_result_count,
    assert_status,
    assert_str_equal,
    assert_true,
    attr,
    chpwd,
    eq_,
    get_deeply_nested_structure,
    neq_,
    ok_,
    ok_broken_symlink,
    ok_exists,
    ok_good_symlink,
    rmtree,
    serve_path_via_http,
    skip_if_on_windows,
    skip_ssh,
    skip_wo_symlink_capability,
    swallow_logs,
    with_tempfile,
    with_testsui,
    with_tree,
)
from datalad.tests.test_utils_testrepos import BasicGitTestRepo
from datalad.cli.tests.test_main import run_main
from datalad.support.keyring_ import MemoryKeyring
from datalad_next.utils import (
    CredentialManager,
    optional_args,
)

lgr = logging.getLogger("datalad.tests.utils")


class WebDAVPath(object):
    """Serve the content of a path via an HTTP WebDAV URL.

    This class is a context manager.

    Parameters
    ----------
    path : str
        Directory with content to serve.
    auth : tuple
        Username, password

    Returns
    -------
    str
      WebDAV server URL
    """
    def __init__(self, path, auth=None):
        self.path = Path(path)
        self.auth = auth
        self.server = None
        self.server_thread = None

    def __enter__(self):
        try:
            from cheroot import wsgi
            from wsgidav.wsgidav_app import WsgiDAVApp
        except ImportError as e:
            raise SkipTest('No WSGI capabilities') from e

        if self.auth:
            auth = {self.auth[0]: {'password': self.auth[1]}}
        else:
            auth = True

        self.path.mkdir(exist_ok=True, parents=True)

        config = {
            "host": "127.0.0.1",
            # random fixed number, maybe make truly random and deal with taken ports
            "port": 43612,
            "provider_mapping": {"/": str(self.path)},
            "simple_dc": {"user_mapping": {'*': auth}},
        }
        app = WsgiDAVApp(config)
        self.server = wsgi.Server(
            bind_addr=(config["host"], config["port"]),
            wsgi_app=app,
        )
        lgr.debug('Starting WebDAV server')
        from threading import Thread
        self.server.prepare()
        self.server_thread = Thread(target=self.server.serve)
        self.server_thread.start()
        lgr.debug('WebDAV started')
        return f'http://{config["host"]}:{config["port"]}'

    def __exit__(self, *args):
        lgr.debug('Stopping WebDAV server')
        # graceful exit
        self.server.stop()
        lgr.debug('WebDAV server stopped, waiting for server thread to exit')
        # wait for shutdown
        self.server_thread.join()
        lgr.debug('WebDAV server thread exited')


@optional_args
def serve_path_via_webdav(tfunc, *targs, auth=None):
    """Decorator which serves content of a directory via a WebDAV server

    Parameters
    ----------
    path : str
        Directory with content to serve.
    auth : tuple or None
        If a (username, password) tuple is given, the server access will
        be protected via HTTP basic auth.
    """
    @wraps(tfunc)
    @attr('serve_path_via_webdav')
    def  _wrap_serve_path_via_http(*args, **kwargs):

        if len(args) > 1:
            args, path = args[:-1], args[-1]
        else:
            args, path = (), args[0]

        with WebDAVPath(path, auth=auth) as url:
            return tfunc(*(args + (path, url)), **kwargs)
    return  _wrap_serve_path_via_http


def with_credential(name, **kwargs):
    """A decorator to temporarily deploy a credential.

    If a credential of the given name already exists, it will
    be temporarily replaced by the given one.

    In pretty much all cases, the keyword arguments need to include
    `secret`. Otherwise any properties are supported.
    """
    def with_credential_decorator(fx):
        @wraps(fx)
        def _with_credential(*dargs, **dkw):
            credman = CredentialManager()
            # retrieve anything that might be conflicting with the
            # to-be-deployed credential
            prev_cred = credman.get(name)
            try:
                credman.set(name, **kwargs)
                fx(*dargs, **dkw)
            finally:
                if prev_cred:
                    credman.set(name, **prev_cred)
                else:
                    credman.remove(name)

        return _with_credential
    return with_credential_decorator
