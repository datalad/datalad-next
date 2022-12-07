import io
import locale
import pytest
import sys

from datalad_next.utils import on_linux

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
        ops.sniff(test_url)
    # now put something at the target location
    test_path.write_text('surprise!')
    # and now it works
    props = ops.sniff(test_url)
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

    # TODO test missing write permissons

def test_file_url_delete(tmp_path):
    payload = 'payload'
    test_path = tmp_path / 'subdir' / 'myfile'
    test_path.parent.mkdir()
    test_url = test_path.as_uri()
    ops = FileUrlOperations()
    # missing file
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.delete(test_url)

    # place file
    test_path.write_text(payload)
    assert test_path.read_text() == payload
    # try deleting a non-empty dir
    with pytest.raises(UrlOperationsRemoteError):
        ops.delete(test_path.parent.as_uri())

    # file deletion works
    ops.delete(test_url)
    assert not test_path.exists()

    # both windows and mac give incomprehensible AccessDenied
    # errors on appveyor, although the directory is confirmed
    # to be empty
    if on_linux:
        # empty dir deletion works too
        # confirm it is indeed empty
        assert not list(test_path.parent.iterdir())
        ops.delete(test_path.parent.as_uri())
        assert not test_path.parent.exists()
