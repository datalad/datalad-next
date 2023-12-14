from __future__ import annotations


from collections import deque
import logging
from functools import wraps
import os
from os import environ
from pathlib import Path
import subprocess
from typing import Any

from datalad.support.external_versions import external_versions
# all datalad-core test utils needed for datalad-next
from datalad.tests.utils_pytest import (
    DEFAULT_BRANCH,
    DEFAULT_REMOTE,
    HTTPPath,
    SkipTest,
    assert_in,
    assert_in_results,
    assert_raises,
    assert_result_count,
    assert_status,
    attr,
    chpwd,
    eq_,
    get_deeply_nested_structure,
    ok_,
    ok_broken_symlink,
    ok_exists,
    ok_good_symlink,
    rmtree,
    skip_if_on_windows,
    skip_wo_symlink_capability,
    swallow_logs,
)
from datalad.tests.test_utils_testrepos import BasicGitTestRepo
from datalad.cli.tests.test_main import run_main
from datalad.ui.progressbars import SilentProgressBar
from datalad.utils import (
    create_tree,
    md5sum,
)
from datalad_next.utils import (
    CredentialManager,
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
            raise SkipTest('No WSGI capabilities. Install cheroot and/or wsgidav') from e

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
            # disable DAV server logging to avoid clustering the test output
            # unless logger runs at least on debug log level
            "logging": {"enable": lgr.isEnabledFor(10)},
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


def with_credential(name, **kwargs):
    """A decorator to temporarily deploy a credential.

    If a credential of the given name already exists, it will
    be temporarily replaced by the given one.

    In pretty much all cases, the keyword arguments need to include
    `secret`. Otherwise any properties are supported.
    """
    import warnings
    warnings.warn(
        "datalad_next.tests.utils.with_credential was replaced by a `credman` "
        "fixture in datalad_next 1.0, and will be removed in "
        "datalad_next 2.0.",
        DeprecationWarning,
    )

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


def get_git_config_global_fpath() -> Path:
    """Returns the file path for the "global" (aka user) Git config scope"""
    fpath_str = environ.get('GIT_CONFIG_GLOBAL')
    if fpath_str is None:
        # this can happen with the datalad-core setup for Git < 2.32.
        # we provide a fallback, but we do not aim to support all
        # possible variants
        fpath = Path(environ['HOME']) / '.gitconfig'
    else:
        fpath = Path(fpath_str)
    return fpath


class TestUI:
    """Drop-in replacement for the DataLad UI to protocol any calls"""

    is_interactive = False
    """Flag is inspected in generic UI code to check for the possibility
    of interactivity"""

    def __init__(self):
        # this member will hold a log of all calls made to the UI wrapper
        self._log = []

    def __str__(self) -> str:
        return "{cls}(\n{log}\n)".format(
            cls=self.__class__.__name__,
            log='\n'.join(f'  {i[0]}: {i[1]}' for i in self.log),
        )

    @property
    def log(self) -> list:
        """Call log

        Returns
        -------
        list
          Each item is a two-tuple with the label of the UI operation
          as first element, and the verbatim parameters/values of the
          respective operation.
        """
        return self._log

    @property
    def operation_sequence(self) -> list:
        """Same as ``.log()``, but limited to just the operation labels"""
        return [i[0] for i in self.log]

    def question(self, *args, **kwargs) -> Any:
        """Raise ``RuntimeError`` when a question needs to be asked"""
        self._log.append(('question', (args, kwargs)))
        raise RuntimeError(
            'non-interactive test UI was asked for a response to a question')

    def message(self, msg, cr='\n'):
        """Post a message"""
        self._log.append(('message', (msg, cr)))

    def get_progressbar(self, *args, **kwargs):
        """Return a progress handler"""
        self._log.append(('get_progressbar', (args, kwargs)))
        return SilentProgressBar(*args, **kwargs)


class InteractiveTestUI(TestUI):
    """DataLad UI that can also provide staged user responses"""

    is_interactive = True

    def __init__(self):
        super().__init__()
        # queue to provision responses
        self._responses = deque()

    def __str__(self) -> str:
        return "{cls}(\n{log}\n  (unused responses: {res})\n)".format(
            cls=self.__class__.__name__,
            log='\n'.join(f'  {i[0]}: {i[1]}' for i in self.log),
            res=list(self.staged_responses),
        )

    @property
    def staged_responses(self) -> deque:
        """``deque`` for staging user responses and retrieving them"""
        return self._responses

    def question(self, *args, **kwargs) -> Any:
        """Report a provisioned response when a question is asked"""
        self._log.append(('question', (args, kwargs)))
        if not self.staged_responses:
            raise AssertionError(
                "UI response requested, but no further are provisioned")
        response = self.staged_responses.popleft()
        self._log.append(('response', response))
        return response


def assert_ssh_access(
    host: str,
    port: str,
    login: str,
    seckey: str,
    path: str,
    localpath: str | None = None,
):
    """Test for a working SSH connection and sufficient permissions to write

    This helper establishes a connection to an SSH server identified by
    ``host`` and ``port``, using a given SSH private key file (``seckey``) for
    authentication.  Once logged in successfully, it tries to create a
    directory and a file at POSIX ``path`` on the server. If ``localpath`` is
    given, it must be a representation of that server-side path on the local
    file system (e.g., a bindmount), and the helper tests whether the created
    content is also reflected in this directory.
    """
    # we can only handle openssh
    ssh_bin = os.environ.get('DATALAD_SSH_EXECUTABLE', 'ssh')

    ssh_call = [
        ssh_bin,
        '-i', seckey,
        '-p', port,
        f'{login}@{host}',
    ]
    # now try if this is a viable configuration
    # verify execute and write permissions (implicitly also POSIX path handling
    subprocess.run(
        ssh_call + [
            f"bash -c 'mkdir -p {path} && touch {path}/datalad-tests-probe'"],
        stdin=subprocess.PIPE,
        check=True,
    )
    if localpath:
        # check if a given
        assert (Path(localpath) / 'datalad-tests-probe').exists()
    subprocess.run(
        ssh_call + [f"bash -c 'rm {path}/datalad-tests-probe'"],
        stdin=subprocess.PIPE,
        check=True,
    )
    if localpath:
        assert not (Path(localpath) / 'datalad-tests-probe').exists()
