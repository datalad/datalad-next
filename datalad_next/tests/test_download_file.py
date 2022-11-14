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
            1, status='error', message='target path already exists')

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
