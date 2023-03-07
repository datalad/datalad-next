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
