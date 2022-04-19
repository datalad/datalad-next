from pathlib import Path
from webdav3.client import Client as DAVClient

from datalad.tests.utils import (
    ok_,
    with_tempfile,
)
from datalad_next.tests.utils import serve_path_via_webdav

webdav_cred = ('datalad', 'secure')


@with_tempfile
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred)
def test_serve_webdav(localpath, remotepath, url):
    webdav_cfg = dict(
        webdav_hostname=url,
        webdav_login=webdav_cred[0],
        webdav_password=webdav_cred[1],
        webdav_root='/',
    )
    webdav = DAVClient(webdav_cfg)
    # plain use should work without error
    webdav.list()
    (Path(remotepath) / 'probe').touch()
    ok_('probe' in webdav.list())


@with_tempfile
@with_tempfile
@serve_path_via_webdav
def test_serve_webdav_noauth(localpath, remotepath, url):
    webdav_cfg = dict(
        webdav_hostname=url,
        webdav_root='/',
    )
    webdav = DAVClient(webdav_cfg)
    # plain use should work without error
    webdav.list()
    (Path(remotepath) / 'probe').touch()
    ok_('probe' in webdav.list())
