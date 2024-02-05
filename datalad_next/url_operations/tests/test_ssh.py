import contextlib
import io

import pytest

from datalad_next.tests import (
    skip_if_on_windows,
)
from ..ssh import (
    SshUrlOperations,
    UrlOperationsRemoteError,
    UrlOperationsResourceUnknown,
)


# path magic inside the test is posix only
@skip_if_on_windows
def test_ssh_url_download(tmp_path, monkeypatch, sshserver):
    ssh_url, ssh_localpath = sshserver
    test_path = ssh_localpath / 'myfile'
    test_url = f'{ssh_url}/myfile'
    ops = SshUrlOperations()
    # no target file (yet), precise exception
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.stat(test_url)
    # this is different for a general connection error
    with pytest.raises(UrlOperationsRemoteError):
        ops.stat(f'ssh://localhostnotaround{test_path}')
    # now put something at the target location
    test_path.write_text('surprise!')
    # and now it works
    props = ops.stat(test_url)
    # we get the correct file size reported
    assert props['content-length'] == test_path.stat().st_size

    # simulate a "protocol error" where the server-side command
    # is not reporting the magic header
    with monkeypatch.context() as m:
        m.setattr(SshUrlOperations, '_stat_cmd', 'echo nothing')
        # we get a distinct exception
        with pytest.raises(RuntimeError):
            ops.stat(test_url)

    # and download
    download_path = tmp_path / 'download'
    props = ops.download(test_url, download_path, hash=['sha256'])
    assert props['sha256'] == '71de4622cf536ed4aa9b65fc3701f4fc5a198ace2fa0bda234fd71924267f696'
    assert props['content-length'] == 9 == test_path.stat().st_size

    # remove source and try again
    test_path.unlink()
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.download(test_url, download_path)
    # this is different for a general connection error
    with pytest.raises(UrlOperationsRemoteError):
        ops.download(f'ssh://localhostnotaround{test_path}', download_path)


# path magic inside the test is posix only
@skip_if_on_windows
def test_ssh_url_upload(tmp_path, monkeypatch, sshserver):
    ssh_url, ssh_localpath = sshserver
    payload = 'surprise!'
    payload_path = tmp_path / 'payload'
    upload_path = ssh_localpath / 'subdir' / 'myfile'
    upload_url = f'{ssh_url}/subdir/myfile'
    ops = SshUrlOperations()

    # standard error if local source is not around
    with pytest.raises(FileNotFoundError):
        ops.upload(payload_path, upload_url)

    payload_path.write_text(payload)
    # upload creates parent dirs, so the next just works.
    # this may seem strange for SSH, but FILE does it too.
    # likewise an HTTP upload is also not required to establish
    # server-side preconditions first.
    # this functionality is not about exposing a full
    # remote FS abstraction -- just upload
    ops.upload(payload_path, upload_url)
    assert upload_path.read_text() == payload


def test_ssh_url_upload_from_stdin(tmp_path, monkeypatch, sshserver):
    ssh_url, ssh_localpath = sshserver
    payload = 'surprise!'
    upload_path = ssh_localpath / 'uploaded.txt'
    upload_url = f'{ssh_url}/uploaded.txt'
    ops = SshUrlOperations()

    class StdinBufferMock:
        def __init__(self, byte_stream: bytes):
            self.buffer = io.BytesIO(byte_stream)

    with monkeypatch.context() as mp_ctx:
        mp_ctx.setattr('sys.stdin', StdinBufferMock(payload.encode()))
        ops.upload(None, upload_url)

    assert upload_path.exists()
    assert upload_path.read_text() == payload


def test_ssh_url_upload_timeout(tmp_path, monkeypatch):
    payload = 'surprise!'
    payload_path = tmp_path / 'payload'
    payload_path.write_text(payload)

    upload_url = f'ssh://localhost/not_used'
    ssh_url_ops = SshUrlOperations()

    @contextlib.contextmanager
    def mocked_iter_subproc(*args, **kwargs):
        yield None

    with monkeypatch.context() as mp_ctx:
        import datalad_next.url_operations.ssh
        mp_ctx.setattr(datalad_next.url_operations.ssh, 'iter_subproc', mocked_iter_subproc)
        mp_ctx.setattr(datalad_next.url_operations.ssh, 'COPY_BUFSIZE', 1)
        with pytest.raises(TimeoutError):
            ssh_url_ops.upload(payload_path, upload_url, timeout=1)


def test_check_return_code():
    SshUrlOperations._check_return_code(0, 'test-0')
    with pytest.raises(UrlOperationsResourceUnknown):
        SshUrlOperations._check_return_code(244, 'test-244')
    with pytest.raises(UrlOperationsRemoteError):
        SshUrlOperations._check_return_code(None, 'test-None')
        SshUrlOperations._check_return_code(1, 'test-1')
