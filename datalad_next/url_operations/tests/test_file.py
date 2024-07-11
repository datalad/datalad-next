import io
import stat

import pytest
import sys

from datalad_next.tests import skip_if_on_windows

from ..file import (
    FileUrlOperations,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)


def test_file_url_download(tmp_path):
    test_path = tmp_path / 'myfile'
    test_url = test_path.as_uri()
    ops = FileUrlOperations()
    # no target file (yet), precise exception
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.stat(test_url)
    # now put something at the target location
    test_path.write_text('surprise!')
    # and now it works
    props = ops.stat(test_url)
    # we get the correct file size reported
    assert props['content-length'] == test_path.stat().st_size

    # and download
    download_path = tmp_path / 'download'
    props = ops.download(test_url, download_path, hash=['sha256'])
    assert props['sha256'] == '71de4622cf536ed4aa9b65fc3701f4fc5a198ace2fa0bda234fd71924267f696'
    assert props['content-length'] == 9 == test_path.stat().st_size

    # remove source and try again
    test_path.unlink()
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.download(test_url, download_path)


def test_file_url_upload(tmp_path, monkeypatch):
    payload = 'payload'
    payload_file = tmp_path / 'payload'
    test_upload_path = tmp_path / 'myfile'
    test_upload_url = test_upload_path.as_uri()
    ops = FileUrlOperations()
    # missing source file
    # standard exception, makes no sense to go custom thinks mih
    with pytest.raises(FileNotFoundError):
        ops.upload(payload_file, test_upload_url)
    # no empty targets lying around
    assert not test_upload_path.exists()

    # now again
    payload_file.write_text(payload)
    props = ops.upload(payload_file, test_upload_url, hash=['md5'])
    assert test_upload_path.read_text() == 'payload'
    assert props['content-length'] == len(payload)
    assert props['md5'] == '321c3cf486ed509164edec1e1981fec8'

    # upload from STDIN
    from_stdin_url = (tmp_path / 'missingdir' / 'from_stdin').as_uri()
    with monkeypatch.context() as m:
        m.setattr(sys, 'stdin',
                  io.TextIOWrapper(io.BytesIO(
                      bytes(payload, encoding='utf-8'))))
        props = ops.upload(None, from_stdin_url, hash=['md5'])
        assert props['md5'] == '321c3cf486ed509164edec1e1981fec8'
        assert props['content-length'] == len(payload)

    # TODO test missing write permissions


def test_file_url_delete(tmp_path):
    payload = 'payload'
    test_dir_path = tmp_path / 'subdir'
    test_path = test_dir_path / 'myfile'

    test_dir_path.mkdir()
    test_url = test_path.as_uri()
    test_dir_url = test_dir_path.as_uri()
    ops = FileUrlOperations()

    # missing file
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.delete(test_url)

    # place file and write protect
    test_path.write_text(payload)
    assert test_path.read_text() == payload
    test_path.chmod(stat.S_IRUSR)
    # file deletion works
    ops.delete(test_url)
    assert not test_path.exists()

    # place file again protect directory and file
    test_path.write_text(payload)
    assert test_path.read_text() == payload
    test_dir_path.chmod(stat.S_IXUSR | stat.S_IRUSR)
    test_path.chmod(stat.S_IRUSR)
    # non-empty directory deletion works
    ops.delete(test_dir_url)
    assert not test_dir_path.exists()

    # create empty, write-protected dir
    assert not test_dir_path.exists()
    test_dir_path.mkdir()
    test_dir_path.chmod(stat.S_IXUSR | stat.S_IRUSR)
    ops.delete(test_dir_url)
    assert not test_dir_path.exists()


@skip_if_on_windows
def test_file_url_upload_errors(tmp_path):
    source = tmp_path / 'source'
    source.write_text('Some content\n')
    fops = FileUrlOperations()
    with pytest.raises(UrlOperationsRemoteError) as e:
        fops.upload(source, 'file:///tmp//')
