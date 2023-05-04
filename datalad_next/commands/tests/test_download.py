from io import StringIO
import json
from pathlib import Path
import pytest

import datalad
from datalad.api import (
    credentials,
    download,
)
from datalad_next.tests.utils import (
    assert_result_count,
    assert_status,
    with_testsui,
)
from datalad_next.utils import chpwd

from datalad_next.utils import CredentialManager

@pytest.fixture
def hbsurl(httpbin):
    # shortcut for the standard URL
    return httpbin["standard"]

test_cred = ('dltest-my&=http', 'datalad', 'secure')

@pytest.fixture
def hbscred(hbsurl):
    return (
        'hbscred',
        dict(user='mike', secret='dummy', type='user_password',
             realm=f'{hbsurl}/Fake Realm'),
    )


def test_download(tmp_path, http_server):
    wdir = tmp_path
    srvurl = http_server.url
    (http_server.path / 'testfile.txt').write_text('test')

    # simple download, taking the target filename from the URL
    # single-pass hashing with two algorithms
    with chpwd(wdir):
        res = download(f'{srvurl}testfile.txt',
                       hash=['md5', 'SHA256'],
                       return_type='item-or-list')

    assert (wdir / 'testfile.txt').read_text() == 'test'
    # keys for hashes keep user-provided captialization
    assert res['md5'] == '098f6bcd4621d373cade4e832627b4f6'
    assert res['SHA256'] == \
        '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08'

    # doing it again fails due to overwrite detection
    with chpwd(wdir):
        assert_result_count(
            download(f'{srvurl}testfile.txt', on_failure='ignore'),
            1, status='error', error_message='target path already exists')

    # works with explicit alternative filename
    with chpwd(wdir):
        download(f'{srvurl}testfile.txt testfile2.txt')

    assert (wdir / 'testfile2.txt').read_text() == 'test'

    # non-existing download source
    assert_result_count(
        download(f'{srvurl}nothere', on_failure='ignore'),
        1, status='error', message='download failure')


def test_download_invalid_calls(monkeypatch):
    # unsupported url scheme, only detected when actually calling
    # a handler inside, hence error result
    assert_result_count(
        download('dummy://mike/file', on_failure='ignore'),
        1,
        status='error',
        message='unsupported URL (custom URL handlers can be declared '
        'via DataLad configuration)',
    )
    # no target path derivable
    # immediate error, when all information is readily available
    with pytest.raises(ValueError):
        download('http://example.com')
    # deferred error result when a generator is gathering batch-mode
    # items at runtime
    monkeypatch.setattr('sys.stdin', StringIO('http://example.com'))
    assert_result_count(
        download(
            '-',
            on_failure='ignore',
        ),
        1, status='impossible')


def test_download_auth(
        tmp_path, credman, http_credential, http_server_with_basicauth):
    credman.set(**http_credential)
    wdir = tmp_path
    srvurl = http_server_with_basicauth.url
    srvpath = http_server_with_basicauth.path
    (srvpath / 'testfile.txt').write_text('test')

    # we have a credential, but there is nothing to discover from
    # that we should use it for this request
    assert_result_count(
        download(f'{srvurl}nothere', on_failure='ignore'),
        1, status='error', message='download failure')

    # amend the test credential with the realm of the test server
    credman = CredentialManager(datalad.cfg)
    credman.set(test_cred[0], realm=f'{srvurl}Protected')

    # now it must be able to auto-detect it
    download({f'{srvurl}testfile.txt': wdir / 'download1.txt'})
    assert (wdir / 'download1.txt').read_text() == 'test'


auth_ok_response = {"authenticated": True, "user": "mike"}


def test_download_basic_auth(credman, capsys, hbscred, hbsurl):
    credman.set(hbscred[0], **hbscred[1])
    # consume stdout to make test self-contained
    capsys.readouterr()
    download(
        {f'{hbsurl}/basic-auth/mike/dummy': '-'})
    assert json.loads(capsys.readouterr().out) == auth_ok_response


def test_download_bearer_token_auth(credman, capsys, hbsurl):
    credman.set('dummy', realm=f'{hbsurl}/', type='token', secret='very')
    # consume stdout to make test self-contained
    capsys.readouterr()
    download(
        {f'{hbsurl}/bearer': '-'})
    assert json.loads(capsys.readouterr().out) == {
        'authenticated': True,
        'token': 'very',
    }


def test_download_digest_auth(credman, capsys, hbscred, hbsurl):
    credman.set(hbscred[0],
                **dict(hbscred[1],
                       realm=f'{hbsurl}/me@kennethreitz.com'))
    # consume stdout to make test self-contained
    capsys.readouterr()
    for url_suffix in (
            '/digest-auth/auth/mike/dummy',
            # non-default algorithm
            '/digest-auth/auth/mike/dummy/SHA-256',
    ):
        download({f'{hbsurl}{url_suffix}': '-'})
        assert json.loads(capsys.readouterr().out) == auth_ok_response
        # repeated reads do not accumulate
        assert capsys.readouterr().out == ''


def test_download_explicit_credential(credman, capsys, hbscred, hbsurl):
    # the provided credential has the wrong 'realm' for auto-detection.
    # but choosing it explicitly must put things to work
    credman.set(hbscred[0], **hbscred[1])
    # consume stdout to make test self-contained
    capsys.readouterr()
    download({f'{hbsurl}/digest-auth/auth/mike/dummy': '-'},
             credential=hbscred[0])
    assert json.loads(capsys.readouterr().out) == auth_ok_response


def test_download_auth_after_redirect(credman, capsys, hbscred, hbsurl):
    credman.set(hbscred[0], **hbscred[1])
    # consume stdout to make test self-contained
    capsys.readouterr()
    download(
        {f'{hbsurl}/redirect-to?url={hbsurl}/basic-auth/mike/dummy': '-'})
    assert json.loads(capsys.readouterr().out) == auth_ok_response


def test_download_no_credential_leak_to_http(credman, capsys, hbscred, httpbin):
    credman.set(hbscred[0], **hbscred[1])
    redirect_url = f'{httpbin["http"]}/basic-auth/mike/dummy'
    res = download(
        # redirect from https to http, must drop provideded credential
        # to avoid leakage
        {f'{httpbin["https"]}/redirect-to?url={redirect_url}': '-'},
        credential=hbscred[0],
        on_failure='ignore')
    assert_status('error', res)
    assert '401' in res[0]['error_message']
    assert f' {redirect_url}' in res[0]['error_message']
    # do the same again, but without the explicit credential,
    # also must not work
    # this is not the right test, though. What would be suitable
    # is an authenticated request that then redirects
    res = download(
        # redirect from https to http, must drop provideded credential
        # to avoid leakage
        {f'{httpbin["https"]}/redirect-to?url={redirect_url}': '-'},
        on_failure='ignore')
    assert_status('error', res)


@with_testsui(responses=[
    'token123',
    # after download, it asks for a name
    'dataladtest_test_download_new_bearer_token',
])
def test_download_new_bearer_token(tmp_keyring, capsys, hbsurl):
    try:
        download({f'{hbsurl}/bearer': '-'})
        # and it was saved under this name
        assert_result_count(
            credentials(
                'get',
                name='dataladtest_test_download_new_bearer_token'),
            1, cred_secret='token123', cred_type='token',
        )
    finally:
        credentials(
            'remove',
            name='dataladtest_test_download_new_bearer_token',
        )


@with_testsui(responses=[
    'datalad_uniquetoken123',
    # after download, it asks for a name, but skip to save
    'skip',
])
def test_download_new_bearer_token_nosave(capsys, hbsurl):
    download({f'{hbsurl}/bearer': '-'})
    # and it was saved under this name
    assert_result_count(
        credentials('query', dict(secret='datalad_uniquetoken123')),
        0,
    )


# make sure a 404 is easily discoverable
# https://github.com/datalad/datalad/issues/6545
def test_download_404(hbsurl):
    assert_result_count(
        download(f'{hbsurl}/status/404', on_failure='ignore'),
        1, status_code=404, status='error')


def test_downloadurl(tmp_path):
    (tmp_path / 'src').mkdir()
    dst_path = tmp_path / 'dst'
    dst_path.mkdir()
    testfile = tmp_path / 'src' / 'myfile.txt'
    testfile.write_text('some content')

    res = download(
        {testfile.as_uri(): dst_path / 'target.txt'},
        hash=['md5'],
        return_type='item-or-list')
    assert_result_count(res, 1, md5='9893532233caff98cd083a116b013c0b')
