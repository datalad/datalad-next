import json
from pathlib import Path

import datalad
from datalad.api import (
    credentials,
    download,
)
from datalad_next.tests.utils import (
    assert_result_count,
    assert_status,
    serve_path_via_http,
    with_credential,
    with_tempfile,
    with_testsui,
)
from datalad_next.utils import chpwd

from datalad_next.utils import CredentialManager


test_cred = ('dltest-my&=http', 'datalad', 'secure')
hburl = 'http://httpbin.org'
hbcred = (
    'hbcred',
    dict(user='mike', secret='dummy', type='user_password',
         realm=f'{hburl}/Fake Realm'),
)
hbsurl = 'https://httpbin.org'
hbscred = (
    'hbscred',
    dict(user='mike', secret='dummy', type='user_password',
         realm=f'{hbsurl}/Fake Realm'),
)


def _get_paths(*args):
    return [Path(p) for p in args]


def _prep_server(path):
    (path / 'testfile.txt').write_text('test')


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@serve_path_via_http
def test_download(wdir=None, srvpath=None, srvurl=None):
    wdir, srvpath = _get_paths(wdir, srvpath)
    _prep_server(srvpath)

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

    # no target path derivable
    assert_result_count(
        download(f'{srvurl}', on_failure='ignore'),
        1, status='impossible')

    # unsupported url scheme
    assert_result_count(
        download('dummy://mike/file', on_failure='ignore'),
        1, status='error', message='unsupported URL scheme')

    # non-existing download source
    assert_result_count(
        download(f'{srvurl}nothere', on_failure='ignore'),
        1, status='error', message='download failure')


@with_credential(
    test_cred[0], user=test_cred[1], secret=test_cred[2],
    type='user_password')
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@serve_path_via_http(use_ssl=False, auth=test_cred[1:])
def test_download_auth(wdir=None, srvpath=None, srvurl=None):
    wdir = Path(wdir)
    srvpath = Path(srvpath)
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


@with_credential(hbscred[0], **hbscred[1])
def test_download_basic_auth(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    download(
        {f'{hbsurl}/basic-auth/mike/dummy': '-'})
    assert json.loads(capsys.readouterr().out) == auth_ok_response


@with_credential('dummy', realm=f'{hbsurl}/', type='token', secret='very')
def test_download_bearer_token_auth(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    download(
        {f'{hbsurl}/bearer': '-'})
    assert json.loads(capsys.readouterr().out) == {
        'authenticated': True,
        'token': 'very',
    }


@with_credential(hbscred[0],
                 **dict(hbscred[1],
                        realm='https://httpbin.org/me@kennethreitz.com'))
def test_download_digest_auth(capsys):
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


# the provided credential has the wrong 'realm' for auto-detection.
# but chosing it explicitly must put things to work
@with_credential(hbscred[0], **hbscred[1])
def test_download_explicit_credential(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    download({f'{hbsurl}/digest-auth/auth/mike/dummy': '-'},
                  credential=hbscred[0])
    assert json.loads(capsys.readouterr().out) == auth_ok_response


@with_credential(hbscred[0], **hbscred[1])
def test_download_auth_after_redirect(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    download(
        {f'{hbsurl}/redirect-to?url={hbsurl}/basic-auth/mike/dummy': '-'})
    assert json.loads(capsys.readouterr().out) == auth_ok_response


@with_credential(hbscred[0], **hbscred[1])
def test_download_no_credential_leak_to_http(capsys):
    redirect_url = f'{hburl}/basic-auth/mike/dummy'
    res = download(
        # redirect from https to http, must drop provideded credential
        # to avoid leakage
        {f'{hbsurl}/redirect-to?url={redirect_url}': '-'},
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
        {f'{hbsurl}/redirect-to?url={redirect_url}': '-'},
        on_failure='ignore')
    assert_status('error', res)


@with_testsui(responses=[
    'token123',
    # after download, it asks for a name
    'dataladtest_test_download_new_bearer_token',
])
def test_download_new_bearer_token(capsys):
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
def test_download_new_bearer_token_nosave(capsys):
    download({f'{hbsurl}/bearer': '-'})
    # and it was saved under this name
    assert_result_count(
        credentials('query', dict(secret='datalad_uniquetoken123')),
        0,
    )


# make sure a 404 is easily discoverable
# https://github.com/datalad/datalad/issues/6545
def test_download_404():
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
