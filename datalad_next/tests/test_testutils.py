from webdav3.client import Client as DAVClient

from datalad.api import clone

from datalad_next.utils import external_versions

from .utils import verify_special_remote


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


def test_verify_special_remote(existing_dataset, tmp_path, no_result_rendering):
    srdir = tmp_path / 'sr'
    srdir.mkdir()
    existing_dataset.repo.init_remote(
        'sr',
        options=['type=directory', 'encryption=none', f'directory={srdir}'],
    )
    dsclone = clone(existing_dataset.path, tmp_path / 'clone')
    dsclone.repo.enable_remote('sr', options=[f'directory={srdir}'])
    verify_special_remote(dsclone.repo, 'sr')
    if external_versions['cmd:annex'] >= '10.20261026':
        assert dsclone.config.get('remote.sr.annex-verified') == 'verified'
    # nothing is left to verify
    verify_special_remote(dsclone.repo, 'sr')
