import json
from pathlib import Path

import datalad
from datalad.api import download_file
from datalad.tests.utils_pytest import (
    assert_result_count,
    serve_path_via_http,
    with_tempfile,
)
from datalad.utils import chpwd

from datalad_next.credman import CredentialManager

from .utils import with_credential

test_cred = ('dltest-my&=http', 'datalad', 'secure')
hburl = 'http://httpbin.org'
hbcred = (
    'hbcred',
    dict(user='mike', secret='dummy', realm=f'{hburl}/Fake Realm'),
)


def _get_paths(*args):
    return [Path(p) for p in args]


def _prep_server(path):
    (path / 'testfile.txt').write_text('test')


@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@serve_path_via_http
def test_download_file(wdir=None, srvpath=None, srvurl=None):
    wdir, srvpath = _get_paths(wdir, srvpath)
    _prep_server(srvpath)

    # simple download, taking the target filename from the URL
    with chpwd(wdir):
        download_file(f'{srvurl}testfile.txt')

    assert (wdir / 'testfile.txt').read_text() == 'test'

    # doing it again fails due to overwrite detection
    with chpwd(wdir):
        assert_result_count(
            download_file(f'{srvurl}testfile.txt', on_failure='ignore'),
            1, status='error', error_message='target path already exists')

    # works with explicit alternative filename
    with chpwd(wdir):
        download_file(f'{srvurl}testfile.txt\ttestfile2.txt')

    assert (wdir / 'testfile2.txt').read_text() == 'test'

    # no target path derivable
    assert_result_count(
        download_file(f'{srvurl}', on_failure='ignore'),
        1, status='impossible')

    # unsupported url scheme
    assert_result_count(
        download_file('dummy://mike/file', on_failure='ignore'),
        1, status='error', message='unsupported URL scheme')

    # non-existing download source
    assert_result_count(
        download_file(f'{srvurl}nothere', on_failure='ignore'),
        1, status='error', message='download failure')


@with_credential(
    test_cred[0], user=test_cred[1], secret=test_cred[2],
    type='user_password')
@with_tempfile(mkdir=True)
@with_tempfile(mkdir=True)
@serve_path_via_http(use_ssl=False, auth=test_cred[1:])
def test_download_file_auth(wdir=None, srvpath=None, srvurl=None):
    wdir = Path(wdir)
    srvpath = Path(srvpath)
    (srvpath / 'testfile.txt').write_text('test')

    # we have a credential, but there is nothing to discover from
    # that we should use it for this request
    assert_result_count(
        download_file(f'{srvurl}nothere', on_failure='ignore'),
        1, status='error', message='download failure')

    # amend the test credential with the realm of the test server
    credman = CredentialManager(datalad.cfg)
    credman.set(test_cred[0], realm=f'{srvurl}Protected')

    # now it must be able to auto-detect it
    download_file({f'{srvurl}testfile.txt': wdir / 'download1.txt'})
    assert (wdir / 'download1.txt').read_text() == 'test'


auth_ok_response = {"authenticated": True, "user": "mike"}


@with_credential(hbcred[0], **hbcred[1])
def test_download_file_basic_auth(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    download_file(
        {f'{hburl}/basic-auth/mike/dummy': '-'})
    assert json.loads(capsys.readouterr().out) == auth_ok_response


@with_credential(hbcred[0],
                 **dict(hbcred[1],
                        realm='http://httpbin.org/me@kennethreitz.com'))
def test_download_file_digest_auth(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    for url_suffix in (
            '/digest-auth/auth/mike/dummy',
            # non-default algorithm
            '/digest-auth/auth/mike/dummy/SHA-256',
    ):
        download_file({f'{hburl}{url_suffix}': '-'})
        assert json.loads(capsys.readouterr().out) == auth_ok_response
        # repeated reads do not accumulate
        assert capsys.readouterr().out == ''


# the provided credential has the wrong 'realm' for auto-detection.
# but chosing it explicitly must put things to work
@with_credential(hbcred[0], **hbcred[1])
def test_download_file_explicit_credential(capsys):
    # consume stdout to make test self-contained
    capsys.readouterr()
    download_file({f'{hburl}/digest-auth/auth/mike/dummy': '-'},
                  credential=hbcred[0])
    assert json.loads(capsys.readouterr().out) == auth_ok_response
