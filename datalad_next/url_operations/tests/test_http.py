from pathlib import Path
import pytest

from ..any import AnyUrlOperations
from ..http import (
    HttpUrlOperations,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)


def test_http_url_operations(credman, httpbin, tmp_path):
    hbsurl = httpbin['standard']
    hbscred = (
        'hbscred',
        dict(user='mike', secret='dummy', type='user_password',
             realm=f'{hbsurl}/Fake Realm'),
    )

    credman.set(hbscred[0], **hbscred[1])
    ops = HttpUrlOperations()
    # authentication after redirect
    target_url = f'{hbsurl}/basic-auth/mike/dummy'
    props = ops.stat(f'{hbsurl}/redirect-to?url={target_url}')
    # we get the resolved URL after redirect back
    assert props['url'] == target_url
    # same again, but credentials are wrong
    target_url = f'{hbsurl}/basic-auth/mike/WRONG'
    with pytest.raises(UrlOperationsRemoteError):
        ops.stat(f'{hbsurl}/redirect-to?url={target_url}')
    # make sure we get the size info
    assert ops.stat(f'{hbsurl}/bytes/63')['content-length'] == 63

    # download
    # SFRUUEJJTiBpcyBhd2Vzb21l == 'HTTPBIN is awesome'
    props = ops.download(f'{hbsurl}/base64/SFRUUEJJTiBpcyBhd2Vzb21l',
                         tmp_path / 'mydownload',
                         hash=['md5'])
    assert (tmp_path / 'mydownload').read_text() == 'HTTPBIN is awesome'

    # 404s
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.stat(f'{hbsurl}/status/404')
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.download(f'{hbsurl}/status/404', tmp_path / 'dontmatter')


def test_custom_http_headers_via_config(datalad_cfg):
    for k, v in (
            ('datalad.url-handler.http.*.class',
             'datalad_next.url_operations.http.HttpUrlOperations'),
            ('datalad.url-handler.http.*.kwargs',
             '{"headers": {"X-Funky": "Stuff"}}'),
    ):
        datalad_cfg.set(k, v, scope='global', reload=False)
    datalad_cfg.reload()
    auo = AnyUrlOperations()
    huo = auo._get_handler(f'http://example.com')
    assert huo._headers['X-Funky'] == 'Stuff'


def test_transparent_decompression(tmp_path):
    # this file is offered with transparent compression/decompression
    # by the github webserver
    url = 'https://raw.githubusercontent.com/datalad/datalad-next/' \
          'd0c4746425a48ef20e3b1c218e68954db9412bee/pyproject.toml'
    dpath = tmp_path / 'test.txt'
    ops = HttpUrlOperations()
    ops.download(from_url=url, to_path=dpath)

    # make sure it ends up on disk uncompressed
    assert dpath.read_text() == \
        '[build-system]\nrequires = ["setuptools >= 43.0.0", "wheel"]\n'
