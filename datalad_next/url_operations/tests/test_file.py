import pytest
from datalad_next.exceptions import (
    AccessFailedError,
    UrlTargetNotFound,
)
from ..file import FileUrlOperations


def test_file_url_operations(tmp_path):
    test_path = tmp_path / 'myfile'
    test_url = test_path.as_uri()
    ops = FileUrlOperations()
    # no target file (yet), precise exception
    with pytest.raises(UrlTargetNotFound):
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
    with pytest.raises(UrlTargetNotFound):
        ops.download(test_url, download_path)
