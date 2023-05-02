from datalad_next.tests.utils import (
    DEFAULT_REMOTE,
    assert_result_count,
)
from datalad.core.distributed.clone import Clone

# run all -core tests, because with _push() we patched a central piece
from datalad.core.distributed.tests.test_push import *

from datalad_next.datasets import Dataset


# we override this specific test, because the original behavior is no longer
# value, because our implementation behaves "better"
def test_gh1811(tmp_path):
    srcpath = tmp_path / 'src'
    clonepath = tmp_path / 'clone'
    # `annex=false` is the only change from the -core implementation
    # of the test. For normal datasets with an annex, the problem underlying
    # gh1811 is no longer valid, because of more comprehensive analysis of
    # what needs pushing in this case
    orig = Dataset(srcpath).create(annex=False)
    (orig.pathobj / 'some').write_text('some')
    orig.save()
    clone = Clone.__call__(source=orig.path, path=clonepath)
    (clone.pathobj / 'somemore').write_text('somemore')
    clone.save()
    clone.repo.call_git(['checkout', 'HEAD~1'])
    res = clone.push(to=DEFAULT_REMOTE, on_failure='ignore')
    assert_result_count(res, 1)
    assert_result_count(
        res, 1,
        path=clone.path, type='dataset', action='publish',
        status='impossible',
        message='There is no active branch, cannot determine remote '
                'branch',
    )
