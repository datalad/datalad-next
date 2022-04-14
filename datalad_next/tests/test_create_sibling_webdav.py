from datalad.distribution.dataset import Dataset
from datalad.tests.utils import (
    with_tempfile,
)
from datalad_next.tests.utils import serve_path_via_webdav


@with_tempfile
@with_tempfile
@serve_path_via_webdav
def test_mike(localpath, remotepath, url):
    ca = dict(result_renderer='disabled')
    ds = Dataset(localpath).create(**ca)
    print(ds.create_sibling_webdav(url, **ca))
