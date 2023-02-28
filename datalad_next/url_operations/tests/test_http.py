from pathlib import Path
import pytest

from datalad_next.tests.utils import get_httpbin_urls
from ..http import (
    HttpUrlOperations,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)


hbsurl = get_httpbin_urls()['standard']
hbscred = (
    'hbscred',
    dict(user='mike', secret='dummy', type='user_password',
         realm=f'{hbsurl}/Fake Realm'),
)


def test_http_url_operations(credman, tmp_path):
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
