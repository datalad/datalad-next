from pathlib import (
    Path,
    PurePosixPath,
)

from ..add_method_url2transport_path import (
    local_io_url2transport_path,
    http_remote_io_url2transport_path,
)

from datalad.utils import on_windows
from datalad.tests.utils_pytest import skip_if


@skip_if(on_windows)
def test_local_io_url2transport_path_posix():

    for url, transport_path in (
            ('/a/b/c', '/a/b/c'),
            ('/C:/a/b/c', '/C:/a/b/c'),
            ('C:/a/b/c', 'C:/a/b/c'),
    ):
        assert local_io_url2transport_path(
            None,
            PurePosixPath(url)
        ) == Path(transport_path)


@skip_if(not on_windows)
def test_local_io_url2transport_path_windows(monkeypatch):
    monkeypatch.setattr(
        'datalad_next.patches.add_method_url2transport_path.on_windows',
        True,
    )
    warnings = []
    monkeypatch.setattr(
        'datalad_next.patches.add_method_url2transport_path.lgr.warning',
        lambda x: warnings.append(x),
    )

    for url, transport_path in (
            ('/a/b/c', '/a/b/c'),
            ('C:/a/b/c', 'C:/a/b/c'),
            ('C:a/b/c', 'C:a/b/c'),
            ('/C:a/b/c', 'C:a/b/c'),
            ('/C:/a/b/c', 'C:/a/b/c'),
    ):
        assert local_io_url2transport_path(
            None,
            PurePosixPath(url)
        ) == Path(transport_path)
    assert len(warnings) > 0


def test_http_remote_io_url2transport_path():

    for url in ('a/b/c', '/a/b/c', '/C:/a/b/c', '/C:a/b/c', 'C:/a/b/c'):
        assert http_remote_io_url2transport_path(
            None,
            PurePosixPath(url)
        ) == PurePosixPath(url)
