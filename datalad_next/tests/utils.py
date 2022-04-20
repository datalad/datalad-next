from functools import wraps
import logging
from pathlib import Path

from datalad.utils import optional_args
from datalad.tests.utils import (
    SkipTest,
    attr,
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
        lgr.debug('Starting WEBDAV server')
        from threading import Thread
        # hmm, it seems as if this would be the proper way, but the docs are
        # super-terse and it does not work like this
        #self.server_thread = Thread(target=self.server._run_in_thread)
        self.server.prepare()
        self.server_thread = Thread(target=self.server.serve)
        self.server_thread.start()
        lgr.debug('WEBDAV started')
        return f'http://{config["host"]}:{config["port"]}'

    def __exit__(self, *args):
        lgr.debug('Stopping WEBDAV server')
        # graceful exit
        self.server.stop()
        # wait for shutdown
        self.server_thread.join()
        lgr.debug('WEBDAV server stopped')


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
