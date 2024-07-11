import io
import stat

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
    upload_url = f'ssh://localhost/not_used.txt'
    ssh_url_ops = SshUrlOperations()

    payload = 'surprise!'
    payload_path = tmp_path / 'payload'
    payload_path.write_text(payload)

    def mocked_ssh_shell_for(*args, **kwargs):

        class XShell:
            def __init__(self, *args, **kwargs):
                self.returncode = 0

            def start(self, *args, **kwargs):
                pass

        return XShell()

    with monkeypatch.context() as mp_ctx:
        import datalad_next.url_operations.ssh
        mp_ctx.setattr(
            datalad_next.url_operations.ssh.SshUrlOperations,
            'ssh_shell_for',
            mocked_ssh_shell_for
        )
        mp_ctx.setattr(datalad_next.url_operations.ssh, 'COPY_BUFSIZE', 1)
        with pytest.raises(TimeoutError):
            ssh_url_ops.upload(payload_path, upload_url, timeout=1)


def test_check_return_code():
    SshUrlOperations._check_return_code(0, 'test-0')
    with pytest.raises(UrlOperationsResourceUnknown):
        SshUrlOperations._check_return_code(244, 'test-244')
    with pytest.raises(UrlOperationsRemoteError):
        SshUrlOperations._check_return_code(None, 'test-None')
    with pytest.raises(UrlOperationsRemoteError):
        SshUrlOperations._check_return_code(1, 'test-1')


def test_ssh_stat(sshserver):
    ssh_url, ssh_local_path = sshserver

    test_path = ssh_local_path / 'file1.txt'
    test_path.write_text('content')
    test_url = f'{ssh_url}/file1.txt'

    ops = SshUrlOperations()
    props = ops.stat(test_url)
    assert props['content-length'] == test_path.stat().st_size

    test_path.unlink()
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.stat(test_url)


@skip_if_on_windows
def test_ssh_delete(sshserver):
    ssh_url, ssh_local_path = sshserver

    test_path = ssh_local_path / 'file1.txt'
    test_path.write_text('content')
    test_path.chmod(stat.S_IRUSR)

    test_dir_path = ssh_local_path / 'dir1'
    test_dir_path.mkdir()
    test_in_dir_file = test_dir_path / 'file2.txt'
    test_in_dir_file.write_text('content 2')
    test_in_dir_file.chmod(stat.S_IRUSR)
    test_dir_path.chmod(stat.S_IRUSR | stat.S_IXUSR)

    test_url = f'{ssh_url}/file1.txt'
    test_dir_url = f'{ssh_url}/dir1'

    ops = SshUrlOperations()
    ops.delete(test_url)
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.delete(test_url)

    ops.delete(test_dir_url)
    with pytest.raises(UrlOperationsResourceUnknown):
        ops.delete(test_dir_url)
