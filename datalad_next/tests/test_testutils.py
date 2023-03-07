from webdav3.client import Client as DAVClient


def test_serve_webdav_fixture(webdav_credential, webdav_server):
    webdav_cfg = dict(
        webdav_hostname=webdav_server.url,
        webdav_login=webdav_credential['user'],
        webdav_password=webdav_credential['secret'],
        webdav_root='/',
    )
    webdav = DAVClient(webdav_cfg)
    # plain use should work without error
    webdav.list()
    (webdav_server.path / 'probe').touch()
    assert 'probe' in webdav.list()


#
# anything below tests deprecated code
#

from pathlib import Path
from datalad_next.tests.utils import serve_path_via_webdav
from datalad.tests.utils_pytest import with_tempfile

webdav_cred = ('datalad', 'secure')


@with_tempfile
@with_tempfile
@serve_path_via_webdav(auth=webdav_cred)
def test_serve_webdav(localpath=None, remotepath=None, url=None):
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
    assert 'probe' in webdav.list()


# while technically possible, there is no practical application of an
# auth-less WebDAV deployment
@with_tempfile
@with_tempfile
@serve_path_via_webdav
def test_serve_webdav_noauth(localpath=None, remotepath=None, url=None):
    webdav_cfg = dict(
        webdav_hostname=url,
        webdav_root='/',
    )
    webdav = DAVClient(webdav_cfg)
    # plain use should work without error
    webdav.list()
    (Path(remotepath) / 'probe').touch()
    assert 'probe' in webdav.list()
