import pytest
from datalad_next.exceptions import (
    AccessFailedError,
    UrlTargetNotFound,
)
from datalad_next.tests.utils import (
    skip_ssh,
    skip_if_on_windows,
)
from ..ssh import SshUrlOperations


# path magic inside the test is posix only
@skip_if_on_windows
# SshUrlOperations does not work against a windows server
# and the test uses 'localhost' as target
@skip_ssh
def test_ssh_url_download(tmp_path, monkeypatch):
    test_path = tmp_path / 'myfile'
    test_url = f'ssh://localhost{test_path}'
    ops = SshUrlOperations()
    # no target file (yet), precise exception
    with pytest.raises(UrlTargetNotFound):
        ops.sniff(test_url)
    # this is different for a general connection error
    with pytest.raises(AccessFailedError):
        ops.sniff(f'ssh://localhostnotaround{test_path}')
    # now put something at the target location
    test_path.write_text('surprise!')
    # and now it works
    props = ops.sniff(test_url)
    # we get the correct file size reported
    assert props['content-length'] == test_path.stat().st_size

    # simulate a "protocol error" where the server-side command
    # is not reporting the magic header
    with monkeypatch.context() as m:
        m.setattr(SshUrlOperations, '_stat_cmd', 'echo nothing')
        # we get a distinct exception
        with pytest.raises(RuntimeError):
            ops.sniff(test_url)

    # and download
    download_path = tmp_path / 'download'
    props = ops.download(test_url, download_path, hash=['sha256'])
    assert props['sha256'] == '71de4622cf536ed4aa9b65fc3701f4fc5a198ace2fa0bda234fd71924267f696'
    assert props['content-length'] == 9 == test_path.stat().st_size

    # remove source and try again
    test_path.unlink()
    with pytest.raises(UrlTargetNotFound):
        ops.download(test_url, download_path)
    # this is different for a general connection error
    with pytest.raises(AccessFailedError):
        ops.download(f'ssh://localhostnotaround{test_path}', download_path)


# path magic inside the test is posix only
@skip_if_on_windows
# SshUrlOperations does not work against a windows server
# and the test uses 'localhost' as target
@skip_ssh
def test_ssh_url_upload(tmp_path, monkeypatch):
    payload = 'surprise!'
    payload_path = tmp_path / 'payload'
    upload_path = tmp_path / 'subdir' / 'myfile'
    upload_url = f'ssh://localhost{upload_path}'
    ops = SshUrlOperations()

    # standard error if local source is not around
    with pytest.raises(FileNotFoundError):
        ops.upload(payload_path, upload_url)

    payload_path.write_text(payload)
    # TODO this should fail (parent dir for the upload missing)
    ops.upload(payload_path, upload_url)
    # TODO this just verifies that the above call should have failed
    # because it did
    assert upload_path.read_text() == payload
