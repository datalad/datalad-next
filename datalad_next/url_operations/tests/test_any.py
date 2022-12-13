import pytest
from .. import (
    UrlOperationsResourceUnknown,
)
from ..any import (
    _url_handlers,
    AnyUrlOperations,
    HttpUrlOperations,
    FileUrlOperations,
)


def test_get_best_url_handler(monkeypatch):
    ops = AnyUrlOperations()
    assert type(ops._get_handler('https://example.com')) == HttpUrlOperations
    # it will report the "longest-matching" Handler
    # we create a non-sensicle FileUrlOperations record to test that
    with monkeypatch.context() as m:
        m.setitem(
            _url_handlers,
            'https://ex.*\.co',
            ('datalad_next.url_operations.file', 'FileUrlOperations'),
        )
        # the handlers are sucked into the class, so we need a new instance
        ops = AnyUrlOperations()
        assert type(ops._get_handler('https://example.com')) == FileUrlOperations


def test_any_url_operations(tmp_path):
    test_path = tmp_path / 'myfile'
    test_url = test_path.as_uri()
    ops = AnyUrlOperations()
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

    # try some obscure URL scheme
    with pytest.raises(ValueError):
        ops.sniff('weird://stuff')

    # and it could have been figured out before
    assert ops.is_supported_url('weird://stuff') == False
